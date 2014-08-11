%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).

-behaviour(gen_server).

%% Server API
-export([ start_link/2
        , start_link/3
        , stop/1
        ]).

%% Kafka API
-export([ produce/5
        ]).

%% Debug API
-export([ debug/2
        , get_sockets/1
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
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------
-record(state, { acks                 :: integer()
               , timeout              :: integer()
               , batch_size           :: integer()
               , batch_timeout        :: integer()
               , sockets              :: [#socket{}]
               , leaders              :: [{{binary(), integer()}, pid()}]
               , buffer = dict:new()  :: dict() % {Topic, Partition} -> [msg]
               , buffer_size = 0      :: integer() % number of messages
               , unacked = []         :: [{integer(), term()}]
               }).

%%%_* Macros -------------------------------------------------------------------
-define(DEFAULT_ACKS,            1). % default required acks
-define(DEFAULT_ACK_TIMEOUT,  1000). % default broker ack timeout
-define(DEFAULT_BATCH_SIZE,     20). % default batch size (N of messages)
-define(DEFAULT_BATCH_TIMEOUT, 200). % default batch timeout (ms)

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options) ->
  start_link(Hosts, Options, []).

-spec start_link([{string(), integer()}], [term()], [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options, Debug) ->
  gen_server:start_link(?MODULE, [Hosts, Options, Debug], [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 ok | {error, any()}.
produce(Pid, Topic, Partition, Key, Value) ->
  gen_server:call(Pid, {produce, Topic, Partition, Key, Value}).

-spec debug(pid(), sys:dbg_opt()) -> ok.
%% @doc Enable/disabling debugging on brod_producer and on all connections
%%      to brokers managed by the given brod_producer instance.
%%      Enable:
%%        brod_producer:debug(Conn, {log, print}).
%%        brod_producer:debug(Conn, {trace, true}).
%%      Disable:
%%        brod_producer:debug(Conn, no_debug).
debug(Pid, Debug) ->
  {ok, Sockets} = get_sockets(Pid),
  lists:foreach(
    fun(#socket{pid = SocketPid}) ->
        {ok, _} = gen:call(SocketPid, system, {debug, Debug})
    end, Sockets),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

-spec get_sockets(pid()) -> {ok, [#socket{}]}.
get_sockets(Pid) ->
  gen_server:call(Pid, get_sockets).

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, Options, Debug]) ->
  erlang:process_flag(trap_exit, true),
  {ok, State} = connect(Hosts, Debug, #state{}),
  Acks = proplists:get_value(required_acks, Options, ?DEFAULT_ACKS),
  Timeout = proplists:get_value(ack_timeout, Options, ?DEFAULT_ACK_TIMEOUT),
  BatchSize = proplists:get_value(batch_size, Options, ?DEFAULT_BATCH_SIZE),
  BatchTimeout = proplists:get_value(batch_timeout, Options,
                                     ?DEFAULT_BATCH_TIMEOUT),
  {ok, State#state{ acks = Acks
                  , timeout = Timeout
                  , batch_size = BatchSize
                  , batch_timeout = BatchTimeout}}.

handle_call({produce, Topic, Partition, Key, Value}, _From, State0) ->
  Buffer0 = State0#state.buffer,
  Buffer = dict:append({Topic, Partition}, {Key, Value}, Buffer0),
  BufferSize = State0#state.buffer_size + 1,
  %% may be flush buffer
  {ok, State} = maybe_send(State0#state{buffer = Buffer,
                                        buffer_size = BufferSize}),
  {reply, ok, State, State#state.batch_timeout};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_sockets, _From, State) ->
  {reply, {ok, State#state.sockets}, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({msg, Pid, CorrId, #produce_response{}}, State) ->
  %% TODO: keep offsets?
  Unacked = lists:keydelete({Pid, CorrId}, 1, State#state.unacked),
  {noreply, State#state{unacked = Unacked}};
handle_info(timeout, #state{buffer_size = Size} = State0) ->
  {ok, State} = case Size > 0 of
                  true  -> do_send(State0);
                  false -> {ok, State0}
                end,
  {noreply, State};
handle_info({'EXIT', Pid, Reason}, State) ->
  Sockets = lists:keydelete(Pid, #socket.pid, State#state.sockets),
  {stop, {socket_down, Reason}, State#state{sockets = Sockets}};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  F = fun(#socket{pid = Pid}) -> brod_sock:stop(Pid) end,
  lists:foreach(F, State#state.sockets).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
connect(Hosts, Debug, State) ->
  {ok, Metadata} = brod_utils:get_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  %% connect to all known nodes which are alive and map node id to connection
  Sockets = lists:foldl(
              fun(#broker_metadata{node_id = Id, host = H, port = P}, Acc) ->
                  %% keep alive nodes only
                  case brod_sock:start_link(self(), H, P, Debug) of
                    {ok, Pid} ->
                      [#socket{ pid = Pid
                              , host = H
                              , port = P
                              , node_id = Id} | Acc];
                    _ ->
                      Acc
                  end
              end, [], Brokers),
  %% map {Topic, Partition} to connection, crash if a node which must be
  %% a leader is not alive
  Leaders =
    lists:foldl(
      fun(#topic_metadata{name = Topic, partitions = Ps}, AccExt) ->
          lists:foldl(
            fun(#partition_metadata{id = Id, leader_id = LeaderId}, AccInt) ->
                #socket{pid = Pid} =
                  lists:keyfind(LeaderId, #socket.node_id, Sockets),
                [{{Topic, Id}, Pid} | AccInt]
            end, AccExt, Ps)
      end, [], Topics),
  {ok, State#state{sockets = Sockets, leaders = Leaders}}.

maybe_send(State) when State#state.buffer_size >= State#state.batch_size ->
  do_send(State);
maybe_send(State) ->
  {ok, State}.

do_send(#state{buffer = Buffer, unacked = Unacked0} = State) ->
  %% distribute buffered data on corresponding leaders
  F1 = fun({_Topic, _Partition} = Key, Msgs, Dict) ->
          {_, Pid} = lists:keyfind(Key, 1, State#state.leaders),
          dict:append(Pid, {Key, Msgs}, Dict)
      end,
  LeadersData = dict:fold(F1, dict:new(), Buffer),
  %% send out buffered data
  F2 = fun(Pid, Data, Acc) ->
           Produce = #produce_request{ acks = State#state.acks
                                     , timeout = State#state.timeout
                                     , data = Data},
           {ok, CorrId} = brod_sock:send(Pid, Produce),
           [{{Pid, CorrId}, Data} | Acc]
       end,
  Unacked = dict:fold(F2, Unacked0, LeadersData),
  {ok, State#state{ buffer = dict:new()
                  , buffer_size = 0
                  , unacked = Unacked}}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
