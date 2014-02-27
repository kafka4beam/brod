%%% ============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%% ============================================================================

%%%_* Module declaration =======================================================
%% @private
-module(brod_sock).

%%%_* Exports ==================================================================

%% API
-export([ start_link/4
        , start/4
        , loop/2
        , init/4
        , send/2
        , send_sync/3
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

%%%_* Macros -------------------------------------------------------------------
-define(MAX_CORR_ID, 4294967295). % 2^32 - 1
-define(TIMEOUT,     10000).

%%%_* Records ==================================================================
-record(state, { parent                :: pid()
               , sock                  :: port()
               , corr_id = 0           :: integer()
               , tail    = <<>>        :: binary()
               , api_keys = dict:new() :: dict()    % corr_id -> api_key
               }).

%%%_* API ======================================================================
-spec start_link(pid(), string(), integer(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Parent, Host, Port, Debug) ->
  proc_lib:start_link(?MODULE, init, [Parent, Host, Port, Debug]).

-spec start(pid(), string(), integer(), term()) ->
               {ok, pid()} | {error, any()}.
start(Parent, Host, Port, Debug) ->
  proc_lib:start(?MODULE, init, [Parent, Host, Port, Debug]).

-spec send(pid(), term()) -> {ok, integer()} | {error, any()}.
send(Pid, Request) ->
  call(Pid, {send, Request}, ?TIMEOUT).

-spec send_sync(pid(), term(), integer()) -> {ok, term()} | {error, any()}.
send_sync(Pid, Request, Timeout) ->
  {ok, CorrId} = send(Pid, Request),
  receive
    {msg, Pid, CorrId, Response} -> {ok, Response}
  after
    Timeout -> {error, timeout}
  end.

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) when is_pid(Pid) ->
  call(Pid, stop, 5000);
stop(_) ->
  ok.

%%%_* Internal functions =======================================================
init(Parent, Host, Port, Debug0) ->
  Debug = sys:debug_options(Debug0),
  SockOpts = [{active, true}, {packet, raw}, binary, {nodelay, true}],
  case gen_tcp:connect(Host, Port, SockOpts) of
    {ok, Sock} ->
      proc_lib:init_ack(Parent, {ok, self()}),
      loop(#state{parent = Parent, sock = Sock}, Debug);
    Error ->
      proc_lib:init_ack(Parent, {error, Error})
  end.

call(Pid, Request, Timeout) ->
  Mref = erlang:monitor(process, Pid),
  erlang:send(Pid, {{self(), Mref}, Request}),
  receive
    {Mref, Reply} ->
      erlang:demonitor(Mref, [flush]),
      Reply;
    {'DOWN', Mref, _, _, Reason} ->
      {error, {process_down, Reason}}
  after Timeout ->
      erlang:demonitor(Mref),
      receive
        {'DOWN', Mref, _, _, _} -> true
      after 0 -> true
      end,
      {error, timeout}
  end.

reply({To, Tag}, Reply) ->
  To ! {Tag, Reply}.

loop(State, Debug) ->
  Msg = receive Input -> Input end,
  try
    decode_msg(Msg, State, Debug)
  catch
    _:E ->
      exit({E, erlang:get_stacktrace()})
  end.

decode_msg({system, From, Msg}, #state{parent = Parent} = State, Debug) ->
  sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug, State);
decode_msg(Msg, State, [] = Debug) ->
  handle_msg(Msg, State, Debug);
decode_msg(Msg, State, Debug0) ->
  Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
  handle_msg(Msg, State, Debug).

handle_msg({tcp, _Sock, Bin}, #state{tail = Tail0} = State, Debug) ->
  Stream = <<Tail0/binary, Bin/binary>>,
  {Tail, Responses, ApiKeys} = kafka:parse_stream(Stream, State#state.api_keys),
  lists:foreach(fun({CorrId, Response}) ->
                    State#state.parent ! {msg, self(), CorrId, Response}
                end, Responses),
  ?MODULE:loop(State#state{tail = Tail, api_keys = ApiKeys}, Debug);
handle_msg({tcp_closed, _Sock}, _State, _) ->
  exit(tcp_closed);
handle_msg({tcp_error, _Sock, Reason}, _State, _) ->
  exit({tcp_error, Reason});
handle_msg({From, {send, Request}}, #state{sock = Sock} = State, Debug) ->
  CorrId = next_corr_id(State#state.corr_id),
  RequestBin = kafka:encode(CorrId, Request),
  %% reply faster, don't block on gen_tcp:send
  reply(From, {ok, CorrId}),
  ok = gen_tcp:send(Sock, RequestBin),
  ApiKey = kafka:api_key(Request),
  ApiKeys = dict:store(CorrId, ApiKey, State#state.api_keys),
  ?MODULE:loop(State#state{corr_id = CorrId, api_keys = ApiKeys}, Debug);
handle_msg({From, stop}, #state{sock = Sock}, _Debug) ->
  gen_tcp:close(Sock),
  reply(From, ok),
  ok;
handle_msg(_Msg, State, Debug) ->
  ?MODULE:loop(State, Debug).

next_corr_id(?MAX_CORR_ID) -> 0;
next_corr_id(CorrId)       -> CorrId + 1.

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
print_msg(Device, stop, State) ->
  do_print_msg(Device, "stop", [], State);
print_msg(Device, Msg, State) ->
  do_print_msg(Device, "unknown msg: ~p", [Msg], State).

do_print_msg(Device, Fmt, Args, State) ->
  CorrId = State#state.corr_id,
  io:format(Device, "[~s] ~p [~10..0b] " ++ Fmt ++ "~nstate: ~p~n",
            [ts(), self(), CorrId] ++ Args ++ [State]).

ts() ->
  Now = erlang:now(),
  {_, _, MicroSec} = Now,
  {{Y,M,D}, {HH,MM,SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
