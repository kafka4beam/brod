%%%
%%%   Copyright (c) 2014-2017, Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%% @private
-module(brod_sock).

%%%_* Exports ==================================================================

%% API
-export([ get_tcp_sock/1
        , init/5
        , loop/2
        , request_sync/3
        , request_async/2
        , start/4
        , start/5
        , start_link/4
        , start_link/5
        , stop/1
        , debug/2
        ]).

%% system calls support for worker process
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , format_status/2
        ]).

-export_type([ options/0
             ]).

-define(DEFAULT_CONNECT_TIMEOUT, timer:seconds(5)).
-define(DEFAULT_REQUEST_TIMEOUT, timer:minutes(4)).
-define(SIZE_HEAD_BYTES, 4).

%%%_* Includes =================================================================
-include("brod_int.hrl").

-type opt_key() :: connect_timeout
                 | request_timeout
                 | ssl.
-type opt_val() :: term().
-type options() :: [{opt_key(), opt_val()}].
-type requests() :: brod_kafka_requests:requests().
-type byte_count() :: non_neg_integer().

-record(acc, { expected_size = error(bad_init) :: byte_count()
             , acc_size = 0 :: byte_count()
             , acc_buffer = [] :: [binary()] %% received bytes in reversed order
             }).

-type acc() :: binary() | #acc{}.

-record(state, { client_id   :: binary()
               , parent      :: pid()
               , sock        :: ?undef | port()
               , acc = <<>>  :: acc()
               , requests    :: ?undef | requests()
               , mod         :: ?undef | gen_tcp | ssl
               , req_timeout :: ?undef | timeout()
               }).

-type client_id() :: brod:client_id() | binary().

%%%_* API ======================================================================

%% @equiv start_link(Parent, Host, Port, ClientId, [])
start_link(Parent, Host, Port, ClientId) ->
  start_link(Parent, Host, Port, ClientId, []).

-spec start_link(pid(), brod:hostname(), brod:portnum(),
                 client_id() | binary(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Parent, Host, Port, ClientId, Options) when is_atom(ClientId) ->
  BinClientId = atom_to_binary(ClientId, utf8),
  start_link(Parent, Host, Port, BinClientId, Options);
start_link(Parent, Host, Port, ClientId, Options) when is_binary(ClientId) ->
  proc_lib:start_link(?MODULE, init, [Parent, Host, Port, ClientId, Options]).

%% @equiv start(Parent, Host, Port, ClientId, [])
start(Parent, Host, Port, ClientId) ->
  start(Parent, Host, Port, ClientId, []).

-spec start(pid(), brod:hostname(), brod:portnum(),
            client_id() | binary(), term()) ->
               {ok, pid()} | {error, any()}.
start(Parent, Host, Port, ClientId, Options) when is_atom(ClientId) ->
  BinClientId = atom_to_binary(ClientId, utf8),
  start(Parent, Host, Port, BinClientId, Options);
start(Parent, Host, Port, ClientId, Options) when is_binary(ClientId) ->
  proc_lib:start(?MODULE, init, [Parent, Host, Port, ClientId, Options]).

%% @doc Send a request and wait (indefinitely) for response.
-spec request_async(pid(), kpro:req()) ->
        {ok, brod:corr_id()} | ok | {error, any()}.
request_async(Pid, Request) ->
  case call(Pid, {send, Request}) of
    {ok, CorrId} ->
      case Request of
        #kpro_req{no_ack = true} -> ok;
        _  -> {ok, CorrId}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Send a request and wait for response for at most Timeout milliseconds.
-spec request_sync(pid(), kpro:req(), timeout()) ->
        {ok, term()} | ok | {error, any()}.
request_sync(Pid, Request, Timeout) ->
  case request_async(Pid, Request) of
    ok              -> ok;
    {ok, CorrId}    -> wait_for_resp(Pid, Request, CorrId, Timeout);
    {error, Reason} -> {error, Reason}
  end.

%% @doc Stop socket process.
-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) when is_pid(Pid) ->
  call(Pid, stop);
stop(_) ->
  ok.

%% @hidden
-spec get_tcp_sock(pid()) -> {ok, port()}.
get_tcp_sock(Pid) ->
  call(Pid, get_tcp_sock).

%% @doc Enable/disable debugging on the socket process.
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
%%      debug(Pid, none) stops debugging
%% @end
-spec debug(pid(), print | string() | none) -> ok.
debug(Pid, none) ->
  system_call(Pid, {debug, no_debug});
debug(Pid, print) ->
  system_call(Pid, {debug, {trace, true}});
debug(Pid, File) when is_list(File) ->
  system_call(Pid, {debug, {log_to_file, File}}).

%%%_* Internal functions =======================================================

-spec init(pid(), brod:hostname(), brod:portnum(),
           binary(), [any()]) -> no_return().
init(Parent, Host, Port, ClientId, Options) ->
  Debug = sys:debug_options(proplists:get_value(debug, Options, [])),
  Timeout = get_connect_timeout(Options),
  SockOpts = [{active, once}, {packet, raw}, binary, {nodelay, true}],
  case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
    {ok, Sock} ->
      State0 = #state{ client_id = ClientId
                     , parent    = Parent
                     },
      %% adjusting buffer size as per recommendation at
      %% http://erlang.org/doc/man/inet.html#setopts-2
      %% idea is from github.com/epgsql/epgsql
      {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} =
        inet:getopts(Sock, [recbuf, sndbuf]),
      ok = inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]),
      SslOpts = proplists:get_value(ssl, Options, false),
      Mod = get_tcp_mod(SslOpts),
      {ok, NewSock} = maybe_upgrade_to_ssl(Sock, Mod, SslOpts, Timeout),
      ok = maybe_sasl_auth(Host, NewSock, Mod, ClientId, Timeout,
                           brod_utils:get_sasl_opt(Options)),
      State = State0#state{mod = Mod, sock = NewSock},
      proc_lib:init_ack(Parent, {ok, self()}),
      ReqTimeout = get_request_timeout(Options),
      ok = send_assert_max_req_age(self(), ReqTimeout),
      try
        Requests = brod_kafka_requests:new(),
        loop(State#state{requests = Requests, req_timeout = ReqTimeout}, Debug)
      catch error : E ->
        Stack = erlang:get_stacktrace(),
        exit({E, Stack})
      end;
    {error, Reason} ->
      %% exit instead of {error, Reason}
      %% otherwise exit reason will be 'normal'
      exit({connection_failure, Reason})
  end.

get_tcp_mod(_SslOpts = true)  -> ssl;
get_tcp_mod(_SslOpts = [_|_]) -> ssl;
get_tcp_mod(_)                -> gen_tcp.

maybe_upgrade_to_ssl(Sock, _Mod = ssl, _SslOpts = true, Timeout) ->
  ssl:connect(Sock, [], Timeout);
maybe_upgrade_to_ssl(Sock, _Mod = ssl, SslOpts = [_|_], Timeout) ->
  ssl:connect(Sock, SslOpts, Timeout);
maybe_upgrade_to_ssl(Sock, _Mod, _SslOpts, _Timeout) ->
  {ok, Sock}.

%% @private
maybe_sasl_auth(_Host, _Sock, _SockMod, _ClientId, _Timeout, ?undef) ->
  ok;
maybe_sasl_auth(Host, Sock, SockMod, ClientId, Timeout, SaslOpts) ->
  try
    ok = sasl_auth(Host, Sock, SockMod, ClientId, Timeout, SaslOpts),
    %% auth backends may have set other opts, ensure 'once' here
    ok = setopts(Sock, SockMod, [{active, once}])
  catch
    error : Reason ->
      exit({Reason, erlang:get_stacktrace()})
  end.

%% @private
sasl_auth(_Host, Sock, Mod, ClientId, Timeout,
          {_Method = plain, SaslUser, SaslPassword}) ->
  ok = setopts(Sock, Mod, [{active, false}]),
  Req = kpro:req(sasl_handshake_request, _V = 0, [{mechanism, <<"PLAIN">>}]),
  HandshakeRequestBin = kpro:encode_request(ClientId, 0, Req),
  ok = Mod:send(Sock, HandshakeRequestBin),
  {ok, <<Len:32>>} = Mod:recv(Sock, 4, Timeout),
  {ok, HandshakeResponseBin} = Mod:recv(Sock, Len, Timeout),
  {[Rsp], <<>>} = kpro:decode_response(<<Len:32, HandshakeResponseBin/binary>>),
  #kpro_rsp{tag = sasl_handshake_response, vsn = 0, msg = Body} = Rsp,
  ErrorCode = kpro:find(error_code, Body),
  case ?IS_ERROR(ErrorCode) of
    true ->
      exit({sasl_auth_error, ErrorCode});
    false ->
      ok = Mod:send(Sock, sasl_plain_token(SaslUser, SaslPassword)),
      case Mod:recv(Sock, 4, Timeout) of
        {ok, <<0:32>>} ->
          ok;
        {error, closed} ->
          exit({sasl_auth_error, bad_credentials});
        Unexpected ->
          exit({sasl_auth_error, Unexpected})
      end
  end;
sasl_auth(Host, Sock, Mod, ClientId, Timeout,
          {callback, ModuleName, Opts}) ->
  case brod_auth_backend:auth(ModuleName, Host, Sock, Mod,
                              ClientId, Timeout, Opts) of
    ok ->
      ok;
    {error, Reason} ->
      exit({callback_auth_error, Reason})
  end.

%% @private
sasl_plain_token(User, Password) ->
  Message = list_to_binary([0, unicode:characters_to_binary(User),
                            0, unicode:characters_to_binary(Password)]),
  <<(byte_size(Message)):32, Message/binary>>.

setopts(Sock, _Mod = gen_tcp, Opts) -> inet:setopts(Sock, Opts);
setopts(Sock, _Mod = ssl, Opts)     ->  ssl:setopts(Sock, Opts).

%% @private
-spec wait_for_resp(pid(), term(), brod:corr_id(), timeout()) ->
        {ok, term()} | {error, any()}.
wait_for_resp(Pid, _, CorrId, Timeout) ->
  Mref = erlang:monitor(process, Pid),
  receive
    {msg, Pid, #kpro_rsp{corr_id = CorrId} = Rsp} ->
      erlang:demonitor(Mref, [flush]),
      {ok, Rsp};
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  after
    Timeout ->
      erlang:demonitor(Mref, [flush]),
      {error, timeout}
  end.

%% @private
system_call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {system, {self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  end.

%% @private
call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  end.

%% @private
reply({To, Tag}, Reply) ->
  To ! {Tag, Reply}.

%% @private
loop(State, Debug) ->
  Msg = receive Input -> Input end,
  decode_msg(Msg, State, Debug).

%% @private
decode_msg({system, From, Msg}, #state{parent = Parent} = State, Debug) ->
  sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, State);
decode_msg(Msg, State, [] = Debug) ->
  handle_msg(Msg, State, Debug);
decode_msg(Msg, State, Debug0) ->
  Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
  handle_msg(Msg, State, Debug).

%% @private
handle_msg({_, Sock, Bin}, #state{ sock     = Sock
                                 , acc      = Acc0
                                 , requests = Requests
                                 , mod      = Mod
                                 } = State, Debug) when is_binary(Bin) ->
  case Mod of
    gen_tcp -> ok = inet:setopts(Sock, [{active, once}]);
    ssl     -> ok = ssl:setopts(Sock, [{active, once}])
  end,
  Acc1 = acc_recv_bytes(Acc0, Bin),
  {Responses, Acc} = decode_response(Acc1),
  NewRequests =
    lists:foldl(
      fun(#kpro_rsp{corr_id = CorrId} = Rsp, Reqs) ->
        Caller = brod_kafka_requests:get_caller(Reqs, CorrId),
        cast(Caller, {msg, self(), Rsp}),
        brod_kafka_requests:del(Reqs, CorrId)
      end, Requests, Responses),
  ?MODULE:loop(State#state{acc = Acc, requests = NewRequests}, Debug);
handle_msg(assert_max_req_age, #state{ requests = Requests
                                     , req_timeout = ReqTimeout
                                     } = State, Debug) ->
  SockPid = self(),
  erlang:spawn_link(fun() ->
                        ok = assert_max_req_age(Requests, ReqTimeout),
                        ok = send_assert_max_req_age(SockPid, ReqTimeout)
                    end),
  ?MODULE:loop(State, Debug);
handle_msg({tcp_closed, Sock}, #state{sock = Sock}, _) ->
  exit({shutdown, tcp_closed});
handle_msg({ssl_closed, Sock}, #state{sock = Sock}, _) ->
  exit({shutdown, ssl_closed});
handle_msg({tcp_error, Sock, Reason}, #state{sock = Sock}, _) ->
  exit({tcp_error, Reason});
handle_msg({ssl_error, Sock, Reason}, #state{sock = Sock}, _) ->
  exit({ssl_error, Reason});
handle_msg({From, {send, Request}},
           #state{ client_id = ClientId
                 , mod       = Mod
                 , sock      = Sock
                 , requests  = Requests
                 } = State, Debug) ->
  {Caller, _Ref} = From,
  {CorrId, NewRequests} =
    case Request of
      #kpro_req{no_ack = true} ->
        brod_kafka_requests:increment_corr_id(Requests);
      _ ->
        brod_kafka_requests:add(Requests, Caller)
    end,
  RequestBin = kpro:encode_request(ClientId, CorrId, Request),
  Res = case Mod of
          gen_tcp -> gen_tcp:send(Sock, RequestBin);
          ssl     -> ssl:send(Sock, RequestBin)
        end,
  case Res of
    ok ->
      _ = reply(From, {ok, CorrId}),
      ok;
    {error, Reason} ->
      exit({send_error, Reason})
  end,
  ?MODULE:loop(State#state{requests = NewRequests}, Debug);
handle_msg({From, get_tcp_sock}, State, Debug) ->
  _ = reply(From, {ok, State#state.sock}),
  ?MODULE:loop(State, Debug);
handle_msg({From, stop}, #state{mod = Mod, sock = Sock}, _Debug) ->
  Mod:close(Sock),
  _ = reply(From, ok),
  ok;
handle_msg(Msg, #state{} = State, Debug) ->
  error_logger:warning_msg("[~p] ~p got unrecognized message: ~p",
                          [?MODULE, self(), Msg]),
  ?MODULE:loop(State, Debug).

%% @private
cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

%% @private
system_continue(_Parent, Debug, State) ->
  ?MODULE:loop(State, Debug).

%% @private
-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _Parent, Debug, _Misc) ->
  sys:print_log(Debug),
  exit(Reason).

%% @private
system_code_change(State, _Module, _Vsn, _Extra) ->
  {ok, State}.

%% @private
format_status(Opt, Status) ->
  {Opt, Status}.

%% @private
print_msg(Device, {_From, {send, Request}}, State) ->
  do_print_msg(Device, "send: ~p", [Request], State);
print_msg(Device, {tcp, _Sock, Bin}, State) ->
  do_print_msg(Device, "tcp: ~p", [Bin], State);
print_msg(Device, {tcp_closed, _Sock}, State) ->
  do_print_msg(Device, "tcp_closed", [], State);
print_msg(Device, {tcp_error, _Sock, Reason}, State) ->
  do_print_msg(Device, "tcp_error: ~p", [Reason], State);
print_msg(Device, {_From, stop}, State) ->
  do_print_msg(Device, "stop", [], State);
print_msg(Device, Msg, State) ->
  do_print_msg(Device, "unknown msg: ~p", [Msg], State).

%% @private
do_print_msg(Device, Fmt, Args, State) ->
  CorrId = brod_kafka_requests:get_corr_id(State#state.requests),
  io:format(Device, "[~s] ~p [~10..0b] " ++ Fmt ++ "~n",
            [ts(), self(), CorrId] ++ Args).

%% @private
ts() ->
  Now = os:timestamp(),
  {_, _, MicroSec} = Now,
  {{Y,M,D}, {HH,MM,SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

%% @private This is to be backward compatible for
%% 'timeout' as connect timeout option name
%% TODO: change to support 'connect_timeout' only for 2.3
%% @end
-spec get_connect_timeout(options()) -> timeout().
get_connect_timeout(Options) ->
  case {proplists:get_value(connect_timeout, Options),
        proplists:get_value(timeout, Options)} of
    {T, _} when is_integer(T) -> T;
    {_, T} when is_integer(T) -> T;
    _                         -> ?DEFAULT_CONNECT_TIMEOUT
  end.

%% @private Get request timeout from options.
-spec get_request_timeout(options()) -> timeout().
get_request_timeout(Options) ->
  proplists:get_value(request_timeout, Options, ?DEFAULT_REQUEST_TIMEOUT).

%% @private
-spec assert_max_req_age(requests(), timeout()) -> ok | no_return().
assert_max_req_age(Requests, Timeout) ->
  case brod_kafka_requests:scan_for_max_age(Requests) of
    Age when Age > Timeout ->
      erlang:exit(request_timeout);
    _ ->
      ok
  end.

%% @private Send the 'assert_max_req_age' message to brod_sock process.
%% The send interval is set to a half of configured timeout.
%% @end
-spec send_assert_max_req_age(pid(), timeout()) -> ok.
send_assert_max_req_age(Pid, Timeout) when Timeout >= 1000 ->
  %% Check every 1 minute
  %% or every half of the timeout value if it's less than 2 minute
  SendAfter = erlang:min(Timeout div 2, timer:minutes(1)),
  _ = erlang:send_after(SendAfter, Pid, assert_max_req_age),
  ok.

%% @private Accumulate newly received bytes.
-spec acc_recv_bytes(acc(), binary()) -> acc().
acc_recv_bytes(Acc, NewBytes) when is_binary(Acc) ->
  case <<Acc/binary, NewBytes/binary>> of
    <<Size:32/signed-integer, _/binary>> = AccBytes ->
      do_acc(#acc{expected_size = Size + ?SIZE_HEAD_BYTES}, AccBytes);
    AccBytes ->
      AccBytes
  end;
acc_recv_bytes(#acc{} = Acc, NewBytes) ->
  do_acc(Acc, NewBytes).

%% @private Add newly received bytes to buffer.
-spec do_acc(acc(), binary()) -> acc().
do_acc(#acc{acc_size = AccSize, acc_buffer = AccBuffer} = Acc, NewBytes) ->
  Acc#acc{acc_size = AccSize + size(NewBytes),
          acc_buffer = [NewBytes | AccBuffer]
         }.

%% @private Decode response when accumulated enough bytes.
-spec decode_response(acc()) -> {[kpro:rsp()], acc()}.
decode_response(#acc{expected_size = ExpectedSize,
                     acc_size = AccSize,
                     acc_buffer = AccBuffer}) when AccSize >= ExpectedSize ->
  %% iolist_to_binary here to simplify kafka_protocol implementation
  %% maybe make it smarter in the next version
  kpro:decode_response(iolist_to_binary(lists:reverse(AccBuffer)));
decode_response(Acc) ->
  {[], Acc}.

%%%_* Eunit ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

acc_test_() ->
  [{"clean start flow",
    fun() ->
        Acc0 = acc_recv_bytes(<<>>, <<0, 0>>),
        ?assertEqual(Acc0, <<0, 0>>),
        Acc1 = acc_recv_bytes(Acc0, <<0, 1, 0, 0>>),
        ?assertEqual(#acc{expected_size = 5,
                          acc_size = 6,
                          acc_buffer = [<<0, 0, 0, 1, 0, 0>>]
                         }, Acc1)
    end},
   {"old tail leftover",
    fun() ->
        Acc0 = acc_recv_bytes(<<0, 0>>, <<0, 4>>),
        ?assertEqual(#acc{expected_size = 8,
                          acc_size = 4,
                          acc_buffer = [<<0, 0, 0, 4>>]
                         }, Acc0),
        Acc1 = acc_recv_bytes(Acc0, <<0, 0>>),
        ?assertEqual(#acc{expected_size = 8,
                          acc_size = 6,
                          acc_buffer = [<<0, 0>>, <<0, 0, 0, 4>>]
                         }, Acc1),
        Acc2 = acc_recv_bytes(Acc1, <<1, 1>>),
        ?assertEqual(#acc{expected_size = 8,
                          acc_size = 8,
                          acc_buffer = [<<1, 1>>, <<0, 0>>, <<0, 0, 0, 4>>]
                         }, Acc2)
    end
   }
  ].

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
