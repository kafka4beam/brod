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
        , send/2
        , send_sync/3
        , start/5
        , start_link/5
        , stop/1
        ]).

%% system calls support for worker process
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , format_status/2
        ]).

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
-spec start_link(pid(), string(), integer(), client_id() | binary(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Parent, Host, Port, ClientId, Debug) when is_atom(ClientId) ->
  BinClientId = list_to_binary(atom_to_list(ClientId)),
  start_link(Parent, Host, Port, BinClientId, Debug);
start_link(Parent, Host, Port, ClientId, Debug) when is_binary(ClientId) ->
  proc_lib:start_link(?MODULE, init, [Parent, Host, Port, ClientId, Debug]).

-spec start(pid(), string(), integer(), client_id() | binary(), term()) ->
               {ok, pid()} | {error, any()}.
start(Parent, Host, Port, ClientId, Debug) when is_atom(ClientId) ->
  BinClientId = list_to_binary(atom_to_list(ClientId)),
  start(Parent, Host, Port, BinClientId, Debug);
start(Parent, Host, Port, ClientId, Debug) when is_binary(ClientId) ->
  proc_lib:start(?MODULE, init, [Parent, Host, Port, ClientId, Debug]).

-spec send(pid(), term()) -> {ok, corr_id()} | ok | {error, any()}.
send(Pid, Request) ->
  call(Pid, {send, Request}).

-spec send_sync(pid(), term(), integer()) -> {ok, term()} | ok | {error, any()}.
send_sync(Pid, #produce_request{acks = 0} = Request, _Timeout) ->
  {ok, _CorrId} = send(Pid, Request),
  ok;
send_sync(Pid, Request, Timeout) ->
  Mref = erlang:monitor(process, Pid),
  {ok, CorrId} = send(Pid, Request),
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

%%%_* Internal functions =======================================================
init(Parent, Host, Port, ClientId, Debug0) ->
  Debug = sys:debug_options(Debug0),
  SockOpts = [{active, true}, {packet, raw}, binary, {nodelay, true}],
  case gen_tcp:connect(Host, Port, SockOpts) of
    {ok, Sock} ->
      proc_lib:init_ack(Parent, {ok, self()}),
      try
        loop(#state{ client_id = ClientId
                   , parent    = Parent
                   , sock      = Sock
                   , requests  = brod_kafka_requests:new()
                   }, Debug)
      catch error : E ->
        Stack = erlang:get_stacktrace(),
        exit({E, Stack})
      end;
    Error ->
      proc_lib:init_ack(Parent, {error, Error})
  end.

call(Pid, Request) ->
  Mref = erlang:monitor(process, Pid),
  Ref = erlang:make_ref(),
  erlang:send(Pid, {{self(), Ref}, Request}),
  receive
    {Ref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {process_down, Reason}}
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
  {Tail, Responses} = brod_kafka:parse_stream(Stream, Requests),
  NewRequests =
    lists:foldl(
      fun({CorrId, Response}, Reqs) ->
        Caller = brod_kafka_requests:get_caller(Reqs, CorrId),
        safe_send(Caller, {msg, self(), CorrId, Response}),
        brod_kafka_requests:del(Reqs, CorrId)
      end, Requests, Responses),
  ?MODULE:loop(State#state{tail = Tail, requests = NewRequests}, Debug);
handle_msg({tcp_closed, _Sock}, _State, _) ->
  exit(tcp_closed);
handle_msg({tcp_error, _Sock, Reason}, _State, _) ->
  exit({tcp_error, Reason});
handle_msg({From, {send, Request}},
           #state{ client_id = ClientId
                 , sock      = Sock
                 , requests  = Requests
                 } = State, Debug) ->
  {Caller, _Ref} = From,
  ApiKey = brod_kafka:api_key(Request),
  {CorrId, NewRequests} =
    brod_kafka_requests:add(Requests, Caller, ApiKey),
  RequestBin = brod_kafka:encode(ClientId, CorrId, Request),
  ok = gen_tcp:send(Sock, RequestBin),
  reply(From, {ok, CorrId}),
  ?MODULE:loop(State#state{requests = NewRequests}, Debug);
handle_msg({From, get_tcp_sock}, State, Debug) ->
  reply(From, {ok, State#state.sock}),
  ?MODULE:loop(State, Debug);
handle_msg({From, stop}, #state{sock = Sock}, _Debug) ->
  gen_tcp:close(Sock),
  reply(From, ok),
  ok;
handle_msg(_Msg, State, Debug) ->
  ?MODULE:loop(State, Debug).

safe_send(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

system_continue(_Parent, Debug, State) ->
  ?MODULE:loop(State, Debug).

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
