%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_srv).

-behaviour(gen_server).

%% Server API
-export([ start_link/1
        , stop/1
        ]).

%% Kafka API
-export([ get_metadata/2
        , produce/4
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%%_* Includes -----------------------------------------------------------------
-include_lib("brod/include/brod.hrl").

%%%_* Records ------------------------------------------------------------------
-record(state, { sock                 :: port()
               , tail    = <<>>       :: binary()
               , callers = dict:new() :: dict()
               , corr_id = 0          :: integer()
               }).

%%%_* Macros -------------------------------------------------------------------
-define(MAX_CORR_ID, 4294967295). % 2^32 - 1

-define(metadata, metadata).
-define(produce,  produce).

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}]) -> {ok, pid()} | {error, any()}.
start_link(Hosts) ->
  gen_server:start_link(?MODULE, [Hosts], []).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec get_metadata(pid(), [binary()]) -> #metadata{} | {error, any()}.
get_metadata(Conn, Topics) ->
  gen_server:call(Conn, {get_metadata, Topics}).

-spec produce(pid(), #data{}, integer(), integer()) ->
                 [#topic_offsets{}] | {error, any()}.
produce(Conn, Data, Acks, Timeout) ->
  gen_server:call(Conn, {produce, Data, Acks, Timeout}).

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts]) ->
  SockOpts = [{active, true}, {packet, raw}, binary, {nodelay, true}],
  {ok, Sock} = try_connect(Hosts, SockOpts),
  {ok, #state{sock = Sock}}.

handle_call({get_metadata, Topics}, From, State0) ->
  CorrId = corr_id(State0#state.corr_id),
  Request = kafka:metadata_request(CorrId, Topics),
  State = send_request(CorrId, Request, ?metadata, From, State0),
  {noreply, State};
handle_call({produce, Data, Acks, Timeout}, From, State0) ->
  CorrId = corr_id(State0#state.corr_id),
  Request = kafka:produce_request(CorrId, Data, Acks, Timeout),
  State = send_request(CorrId, Request, ?produce, From, State0),
  {noreply, State};
handle_call(stop, _From, State) ->
  gen_tcp:close(State#state.sock),
  {stop, normal, ok, State};
handle_call(Request, _From, State) ->
  {stop, {unsupported_call, Request}, State}.

handle_cast(Msg, State) ->
  {stop, {unsupported_cast, Msg}, State}.

handle_info({tcp, Sock, Bin}, #state{sock = Sock, tail = T} = State) ->
  Stream = <<T/binary, Bin/binary>>,
  {RawResponses, Tail} = kafka:pre_parse_stream(Stream),
  Responses = parse_responses(RawResponses, State#state.callers),
  Callers = maybe_reply(Responses, State#state.callers),
  {noreply, State#state{tail = Tail, callers = Callers}};
handle_info({tcp_closed, Sock}, #state{sock = Sock} = State) ->
  {stop, sock_closed, State};
handle_info({tcp_error, Sock, Reason}, #state{sock = Sock} = State) ->
  {stop, {sock_error, Reason}, State};
handle_info(Info, State) ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
send_request(CorrId, Request, Type, From, State) ->
  ok = gen_tcp:send(State#state.sock, Request),
  Callers = dict:append(CorrId, {Type, From}, State#state.callers),
  State#state{callers = Callers}.

maybe_reply([], Callers) ->
  Callers;
maybe_reply([{CorrId, Response} | T], Callers) ->
  case dict:find(CorrId, Callers) of
    {ok, [{_, From}]} -> gen_server:reply(From, Response);
    error           -> ok
  end,
  maybe_reply(T, dict:erase(CorrId, Callers)).

parse_responses(RawResponses, Callers) ->
  parse_responses(RawResponses, Callers, []).

parse_responses([], _Callers, Acc) ->
  Acc;
parse_responses([{CorrId, Bin} | T], Callers, Acc) ->
  case dict:find(CorrId, Callers) of
    {ok, [{Type, _}]} ->
      Response = parse_response(Type, Bin),
      parse_responses(T, Callers, [{CorrId, Response} | Acc]);
    error ->
      parse_responses(T, Callers, Acc)
  end.

parse_response(?metadata, Bin) ->
  kafka:metadata_response(Bin);
parse_response(?produce, Bin) ->
  kafka:produce_response(Bin).

try_connect(Hosts, SockOpts) ->
  try_connect(Hosts, SockOpts, undefined).

try_connect([], _, LastError) ->
  LastError;
try_connect([{Host, Port} | Hosts], SockOpts, _) ->
  case gen_tcp:connect(Host, Port, SockOpts) of
    {ok, Sock} -> {ok, Sock};
    Error      -> try_connect(Hosts, SockOpts, Error)
  end.

corr_id(?MAX_CORR_ID) -> 0;
corr_id(CorrId)       -> CorrId + 1.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
