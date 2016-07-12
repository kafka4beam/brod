%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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

%%%=============================================================================
%%% @doc
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%% ============================================================================

%%%_* Module declaration =======================================================
%% @private
-module(brod_sock).

%%%_* Exports ==================================================================

%% API
-export([ get_tcp_sock/1
        , init/5
        , loop/2
        , request_sync/3
        , request_async/2
        , start/5
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

-define(CONNECT_TIMEOUT, timer:seconds(5)).

%%%_* Includes =================================================================
-include("brod_int.hrl").

%%%_* Records ==================================================================
-record(state, { client_id   :: binary()
               , parent      :: pid()
               , sock        :: port()
               , tail = <<>> :: binary() %% leftover of last data stream
               , requests    :: brod_kafka_requests:requests()
               }).

%%%_* API ======================================================================
-spec start_link(pid(), string(), integer(),
                 brod_client_id() | binary(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Parent, Host, Port, ClientId, Debug) when is_atom(ClientId) ->
  BinClientId = list_to_binary(atom_to_list(ClientId)),
  start_link(Parent, Host, Port, BinClientId, Debug);
start_link(Parent, Host, Port, ClientId, Debug) when is_binary(ClientId) ->
  proc_lib:start_link(?MODULE, init, [Parent, Host, Port, ClientId, Debug]).

-spec start(pid(), string(), integer(), brod_client_id() | binary(), term()) ->
               {ok, pid()} | {error, any()}.
start(Parent, Host, Port, ClientId, Debug) when is_atom(ClientId) ->
  BinClientId = list_to_binary(atom_to_list(ClientId)),
  start(Parent, Host, Port, BinClientId, Debug);
start(Parent, Host, Port, ClientId, Debug) when is_binary(ClientId) ->
  proc_lib:start(?MODULE, init, [Parent, Host, Port, ClientId, Debug]).

-spec request_async(pid(), term()) -> {ok, corr_id()} | ok | {error, any()}.
request_async(Pid, Request) ->
  case call(Pid, {send, Request}) of
    {ok, CorrId} ->
      case Request of
        #kpro_ProduceRequest{requiredAcks = 0} -> ok;
        _                                      -> {ok, CorrId}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec request_sync(pid(), term(), integer()) ->
        {ok, term()} | ok | {error, any()}.
request_sync(Pid, Request, Timeout) ->
  case request_async(Pid, Request) of
    ok              -> ok;
    {ok, CorrId}    -> wait_for_resp(Pid, Request, CorrId, Timeout);
    {error, Reason} -> {error, Reason}
  end.

-spec wait_for_resp(pid(), term(), integer(), timeout()) ->
        {ok, term()} | {error, any()}.
wait_for_resp(Pid, _, CorrId, Timeout) ->
  Mref = erlang:monitor(process, Pid),
  receive
    {msg, Pid, CorrId, Response} ->
      erlang:demonitor(Mref, [flush]),
      {ok, Response};
    {'DOWN', Mref, _, _, Reason} ->
      {error, {sock_down, Reason}}
  after
    Timeout ->
      erlang:demonitor(Mref, [flush]),
      {error, timeout}
  end.

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) when is_pid(Pid) ->
  call(Pid, stop);
stop(_) ->
  ok.

-spec get_tcp_sock(pid()) -> {ok, port()}.
get_tcp_sock(Pid) ->
  call(Pid, get_tcp_sock).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable/disable debugging on the socket process.
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
%%      debug(Pid, none) stops debugging
debug(Pid, none) ->
  system_call(Pid, {debug, no_debug});
debug(Pid, print) ->
  system_call(Pid, {debug, {trace, true}});
debug(Pid, File) when is_list(File) ->
  system_call(Pid, {debug, {log_to_file, File}}).

%%%_* Internal functions =======================================================

-spec init(pid(), hostname(), portnum(), brod_client_id(), [any()]) ->
        no_return().
init(Parent, Host, Port, ClientId, Debug0) ->
  Debug = sys:debug_options(Debug0),
  SockOpts = [{active, true}, {packet, raw}, binary, {nodelay, true}],
  case gen_tcp:connect(Host, Port, SockOpts, ?CONNECT_TIMEOUT) of
    {ok, Sock} ->
      proc_lib:init_ack(Parent, {ok, self()}),
      try
        Requests = brod_kafka_requests:new(),
        State = #state{ client_id = ClientId
                      , parent    = Parent
                      , sock      = Sock
                      , requests  = Requests
                      },
        loop(State, Debug)
      catch error : E ->
        Stack = erlang:get_stacktrace(),
        exit({E, Stack})
      end;
    {error, Reason} ->
      %% exit instead of {error, Reason}
      %% otherwise exit reason will be 'normal'
      exit(Reason)
  end.

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

reply({To, Tag}, Reply) ->
  To ! {Tag, Reply}.

loop(State, Debug) ->
  Msg = receive Input -> Input end,
  decode_msg(Msg, State, Debug).

decode_msg({system, From, Msg}, #state{parent = Parent} = State, Debug) ->
  sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, State);
decode_msg(Msg, State, [] = Debug) ->
  handle_msg(Msg, State, Debug);
decode_msg(Msg, State, Debug0) ->
  Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
  handle_msg(Msg, State, Debug).

handle_msg({tcp, _Sock, Bin}, #state{ tail     = Tail0
                                    , requests = Requests
                                    } = State, Debug) ->
  Stream = <<Tail0/binary, Bin/binary>>,
  {Responses, Tail} = kpro:decode_response(Stream),
  NewRequests =
    lists:foldl(
      fun(#kpro_Response{ correlationId   = CorrId
                        , responseMessage = Response
                        }, Reqs) ->
        Caller = brod_kafka_requests:get_caller(Reqs, CorrId),
        cast(Caller, {msg, self(), CorrId, Response}),
        brod_kafka_requests:del(Reqs, CorrId)
      end, Requests, Responses),
  ?MODULE:loop(State#state{tail = Tail, requests = NewRequests}, Debug);
handle_msg({tcp_closed, _Sock}, _State, _) ->
  exit({shutdown, tcp_closed});
handle_msg({tcp_error, _Sock, Reason}, _State, _) ->
  exit({tcp_error, Reason});
handle_msg({From, {send, Request}},
           #state{ client_id = ClientId
                 , sock      = Sock
                 , requests  = Requests
                 } = State, Debug) ->
  {Caller, _Ref} = From,
  {CorrId, NewRequests} = brod_kafka_requests:add(Requests, Caller),
  RequestBin = kpro:encode_request(ClientId, CorrId, Request),
  ok = gen_tcp:send(Sock, RequestBin),
  reply(From, {ok, CorrId}),
  ?MODULE:loop(State#state{requests = NewRequests}, Debug);
handle_msg({From, get_tcp_sock}, State, Debug) ->
  _ = reply(From, {ok, State#state.sock}),
  ?MODULE:loop(State, Debug);
handle_msg({From, stop}, #state{sock = Sock}, _Debug) ->
  gen_tcp:close(Sock),
  _ = reply(From, ok),
  ok;
handle_msg(Msg, State, Debug) ->
  error_logger:warning_msg("[~p] ~p got unrecognized message: ~p",
                          [?MODULE, self(), Msg]),
  ?MODULE:loop(State, Debug).

cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

system_continue(_Parent, Debug, State) ->
  ?MODULE:loop(State, Debug).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _Parent, Debug, _Misc) ->
  sys:print_log(Debug),
  exit(Reason).

system_code_change(State, _Module, _Vsn, _Extra) ->
  {ok, State}.

format_status(Opt, Status) ->
  {Opt, Status}.

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

do_print_msg(Device, Fmt, Args, State) ->
  CorrId = brod_kafka_requests:get_corr_id(State#state.requests),
  io:format(Device, "[~s] ~p [~10..0b] " ++ Fmt ++ "~n",
            [ts(), self(), CorrId] ++ Args).

ts() ->
  Now = os:timestamp(),
  {_, _, MicroSec} = Now,
  {{Y,M,D}, {HH,MM,SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
