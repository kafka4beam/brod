%%% ============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%% ============================================================================

%%%_* Module declaration =======================================================
%% @private
-module(brod_sock).

%%%_* Exports ==================================================================

-export([ start_link/4
        , loop/2
        , init/4
        , send/3
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
-define(TIMEOUT,     1000).

%%%_* Records ==================================================================
-record(state, { parent               :: pid()
               , sock                 :: port()
               , corr_id = 0          :: integer()
               , tail    = <<>>       :: binary()
               , methods = dict:new() :: dict()    % corr_id -> method
               }).

%%%_* API ======================================================================
-spec start_link(pid(), string(), integer(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Parent, Host, Port, Debug) ->
  proc_lib:start_link(?MODULE, init, [Parent, Host, Port, Debug]).

-spec send(pid(), atom(), term()) -> {ok, integer()} | {error, timeout}.
send(Pid, Method, Data) ->
  Ref = erlang:make_ref(),
  Pid ! {send, self(), Ref, Method, Data},
  receive
    {Pid, Ref, Reply} -> Reply
  after
    ?TIMEOUT -> {error, timeout}
  end.

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

loop(#state{parent = Parent, sock = Sock, tail = Tail} = State, Debug0) ->
  receive
    {system, From, Msg} ->
      sys:handle_system_msg(Msg, From, Parent, ?MODULE, Debug0, State);
    {send, Pid, Ref, Method, Data} = Msg ->
      Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
      CorrId = next_corr_id(State#state.corr_id),
      %% reply to parent faster
      Pid ! {self(), Ref, {ok, CorrId}},
      Request = kafka:encode(Method, CorrId, Data),
      ok = gen_tcp:send(Sock, Request),
      Methods = dict:store(CorrId, Method, State#state.methods),
      ?MODULE:loop(State#state{corr_id = CorrId, methods = Methods}, Debug);
    {tcp, Sock, Bin} = Msg ->
      Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
      Stream = <<Tail/binary, Bin/binary>>,
      {Msgs, Tail} = kafka:pre_parse_stream(Stream),
      Methods =
        lists:foldl(fun({CorrId, MsgBin}, NewMethods) ->
                        Method = dict:fetch(CorrId, NewMethods),
                        Response = kafka:decode(Method, MsgBin),
                        Parent ! {msg, self(), CorrId, Method, Response},
                        dict:erase(CorrId, NewMethods)
                      end, State#state.methods, Msgs),
      ?MODULE:loop(State#state{tail = Tail, methods = Methods}, Debug);
    {tcp_closed, _Sock} = Msg ->
      sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
      exit(tcp_closed);
    {tcp_error, _Sock, Reason} = Msg ->
      sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
      exit({tcp_error, Reason});
    stop ->
      gen_tcp:close(Sock),
      ok;
    Msg ->
      Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
      ?MODULE:loop(State, Debug)
  end.

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

print_msg(Device, {send, _Pid, _Ref, Method, Data}, State) ->
  do_print_msg(Device, "send: ~p ~p", [Method, Data], State);
print_msg(Device, {tcp, _Sock, Bin}, State) ->
  do_print_msg(Device, "tcp: ~p", [Bin], State);
print_msg(Device, {tcp_closed, _Sock}, State) ->
  do_print_msg(Device, "tcp_closed", [], State);
print_msg(Device, {tcp_error, _Sock, Reason}, State) ->
  do_print_msg(Device, "tcp_error: ~p", [Reason], State);
print_msg(Device, Msg, State) ->
  do_print_msg(Device, "unknown msg: ~p", [Msg], State).

do_print_msg(Device, Fmt, Args, State) ->
  CorrId = State#state.corr_id,
  io:format(Device, "[~s] ~p [~10..0b] " ++ Fmt ++ "~n",
            [ts(), self(), CorrId] ++ Args).

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
