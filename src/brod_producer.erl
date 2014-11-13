%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).

-behaviour(gen_server).

%% Server API
-export([ start_link/3
        , start_link/4
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
-type topic()     :: binary().
-type partition() :: non_neg_integer().
-type leader()    :: pid(). % brod_sock pid
-type corr_id()   :: non_neg_integer().
-type from()      :: {pid(), reference()}.

-record(state, { acks                   :: integer()
               , ack_timeout            :: integer()
               , sockets = []           :: [#socket{}]
               , leaders = []           :: [{{topic(), partition()}, leader()}]
               , data_buffer = []       :: [{leader(), [{topic(), dict()}]}]
               , from_buffer = dict:new() :: dict() % (leader(), [from()])
               , pending = []           :: [{leader(), {corr_id(), [from()]}}]
               }).

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], integer(), integer()) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, RequiredAcks, AckTimeout) ->
  start_link(Hosts, RequiredAcks, AckTimeout, []).

-spec start_link([{string(), integer()}], integer(), integer(), [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, RequiredAcks, AckTimeout, Debug) ->
  gen_server:start_link(?MODULE, [Hosts, RequiredAcks, AckTimeout, Debug],
                        [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 {ok, reference()}.
produce(Pid, Topic, Partition, Key, Value) ->
  Ref = erlang:make_ref(),
  From = {self(), Ref},
  %% do not use gen_server:cast as it silently "eats" an exception
  %% when a message is sent to a non-existing process
  erlang:send(Pid, {produce, From, Topic, Partition, Key, Value}),
  {ok, Ref}.

-spec debug(pid(), list()) -> ok.
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
init([Hosts, RequiredAcks, AckTimeout, Debug]) ->
  erlang:process_flag(trap_exit, true),
  {ok, State} = connect(Hosts, Debug, #state{}),
  {ok, State#state{acks = RequiredAcks, ack_timeout = AckTimeout}}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_sockets, _From, State) ->
  {reply, {ok, State#state.sockets}, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({produce, From, Topic, Partition, Key, Value}, State0) ->
  {Leader, State1} = handle_produce(From, Topic, Partition, Key, Value, State0),
  State = maybe_send(Leader, State1),
  {noreply, State};
handle_info({msg, Leader, CorrId, #produce_response{}}, State0) ->
  {value, {_, FromList}, Pending} =
    lists:keytake({Leader, CorrId}, 1, State0#state.pending),
  lists:foreach(fun({Pid, Ref}) ->
                    Pid ! {{Ref, self()}, ack}
                end, FromList),
  State = maybe_send(Leader, State0#state{pending = Pending}),
  {noreply, State};
handle_info({'EXIT', Pid, Reason}, State) ->
  Sockets = lists:keydelete(Pid, #socket.pid, State#state.sockets),
  %% Shutdown because cluster has elected other leaders for
  %% every topic/partition most likely.
  %% Users will have to resend outstanding messages
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

handle_produce(From, Topic, Partition, Key, Value, State) ->
  Leader = get_leader(Topic, Partition, State#state.leaders),
  DataBuffer0 = State#state.data_buffer,
  LeaderBuffer0 = get_leader_buffer(Leader, DataBuffer0),
  TopicBuffer0 = get_topic_buffer(Topic, LeaderBuffer0),
  TopicBuffer = dict:append(Partition, {Key, Value}, TopicBuffer0),
  LeaderBuffer = store_topic_buffer(Topic, LeaderBuffer0, TopicBuffer),
  DataBuffer = store_leader_buffer(Leader, DataBuffer0, LeaderBuffer),
  FromBuffer = dict:append(Leader, From, State#state.from_buffer),
  {Leader, State#state{data_buffer = DataBuffer, from_buffer = FromBuffer}}.

maybe_send(Leader, State) ->
  case { get_leader_buffer(Leader, State#state.data_buffer)
       , lists:keyfind(Leader, 1, State#state.pending)} of
    {[], _} -> % nothing to send right now
      {ok, State};
    {LeaderBuffer, false} -> % no outstanding messages
      Produce = #produce_request{ acks    = State#state.acks
                                , timeout = State#state.ack_timeout
                                , data    = LeaderBuffer},
      {ok, CorrId} = brod_sock:send(Leader, Produce),
      DataBuffer = lists:keydelete(Leader, 1, State#state.data_buffer),
      FromList = dict:fetch(Leader, State#state.from_buffer),
      FromBuffer = dict:erase(Leader, State#state.from_buffer),
      Pending = [{Leader, {CorrId, FromList}} | State#state.pending],
      {ok, State#state{ data_buffer = DataBuffer
                      , from_buffer = FromBuffer
                      , pending     = Pending}};
    {_, _} -> % waiting for ack
      {ok, State}
  end.

get_leader(Topic, Partition, Leaders) ->
  {_, Leader} = lists:keyfind({Topic, Partition}, 1, Leaders),
  Leader.

get_leader_buffer(Leader, DataBuffer) ->
  case lists:keyfind(Leader, 1, DataBuffer) of
    {_, LeaderBuffer} -> LeaderBuffer;
    false             -> []
  end.

store_leader_buffer(Leader, DataBuffer, LeaderBuffer) ->
  lists:keystore(Leader, 1, DataBuffer, {Leader, LeaderBuffer}).

get_topic_buffer(Topic, LeaderBuffer) ->
  case lists:keyfind(Topic, 1, LeaderBuffer) of
    {_, TopicBuffer} -> TopicBuffer;
    false            -> dict:new()
  end.

store_topic_buffer(Topic, LeaderBuffer, TopicBuffer) ->
  lists:keystore(Topic, 1, LeaderBuffer, {Topic, TopicBuffer}).

%% Tests -----------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

maybe_send_test() ->
  {ok, brod_sock} = compile:file("test/mock/brod_sock",
                                 [{outdir, "test/mock"}]),
  {module, brod_sock} = code:load_abs("test/mock/brod_sock"),
  State0 = #state{data_buffer = [], pending = []},
  L1 = erlang:list_to_pid("<0.0.1>"),
  ?assertEqual({ok, State0}, maybe_send(L1, State0)),
  State1 = State0#state{pending = [{L1, 2}]},
  ?assertEqual({ok, State1}, maybe_send(L1, State1)),
  L2 = erlang:list_to_pid("<0.0.2>"),
  FromBuffer = dict:store(L2, [d], dict:store(L1, [a, b, c], dict:new())),
  State2 = State0#state{ data_buffer = [{L1, foo}, {L2, bar}]
                       , from_buffer = FromBuffer},
  ExpectedState = #state{ data_buffer = [{L2, bar}]
                        , from_buffer = dict:erase(L1, FromBuffer)
                        , pending = [{L1, {1, [a, b, c]}}]},
  ?assertEqual({ok, ExpectedState}, maybe_send(L1, State2)),
  ok.

handle_produce_test() ->
  F1 = f1,
  L1 = 1,
  T1 = t1,
  P1 = p1,
  K1 = k1,
  V1 = v1,
  Leaders0 = [{{T1, P1}, L1}],
  FromBuffer1 = dict:store(L1, [F1], dict:new()),
  TopicBuffer1 = dict:store(P1, [{K1, V1}], dict:new()),
  DataBuffer1 = [{L1, [{T1, TopicBuffer1}]}],
  State0 = #state{leaders = Leaders0},
  {L1, State1} = handle_produce(F1, T1, P1, K1, V1, State0),
  ?assertEqual(Leaders0, State1#state.leaders),
  ?assertEqual(FromBuffer1, State1#state.from_buffer),
  ?assertEqual(DataBuffer1, State1#state.data_buffer),

  K2 = k2,
  V2 = v2,
  TopicBuffer2 = dict:append(P1, {K2, V2}, TopicBuffer1),
  DataBuffer2 = [{L1, [{T1, TopicBuffer2}]}],
  FromBuffer2 = dict:append(L1, F1, State1#state.from_buffer),
  {L1, State2} = handle_produce(F1, T1, P1, K2, V2, State1),
  ?assertEqual(Leaders0, State1#state.leaders),
  ?assertEqual(FromBuffer2, State2#state.from_buffer),
  ?assertEqual(DataBuffer2, State2#state.data_buffer),

  F2 = f2,
  L2 = 2,
  P2 = p2,
  K3 = k3,
  V3 = v3,
  FromBuffer3 = dict:append(L2, F2, State2#state.from_buffer),
  Leaders1 = [{{T1, P2}, L2} | Leaders0],
  State3 = State2#state{leaders = Leaders1},
  TopicBuffer3 = dict:append(P2, {K3, V3}, dict:new()),
  %% ++ due to lists:keystore/4 behaviour
  DataBuffer3 = State3#state.data_buffer ++ [{L2, [{T1, TopicBuffer3}]}],
  {L2, State4} = handle_produce(F2, T1, P2, K3, V3, State3),
  ?assertEqual(Leaders1, State4#state.leaders),
  ?assertEqual(FromBuffer3, State4#state.from_buffer),
  ?assertEqual(DataBuffer3, State4#state.data_buffer),

  T2 = t2,
  F3 = f3,
  K4 = k4,
  V4 = v4,
  FromBuffer5 = dict:append(L1, F3, State4#state.from_buffer),
  Leaders2 = [{{T2, P1}, L1} | Leaders1],
  State5 = State4#state{leaders = Leaders2},
  Topic2Buffer = dict:append(P1, {K4, V4}, dict:new()),
  {value, {L1, [{T1, Topic1Buffer}]}, DataBuffer4} =
    lists:keytake(L1, 1, DataBuffer3),
  DataBuffer5 = [{L1, [{T1, Topic1Buffer}, {T2, Topic2Buffer}]} |
                DataBuffer4],
  {L1, State6} = handle_produce(F3, T2, P1, K4, V4, State5),
  ?assertEqual(Leaders2, State6#state.leaders),
  ?assertEqual(FromBuffer5, State6#state.from_buffer),
  ?assertEqual(DataBuffer5, State6#state.data_buffer),
  ok.

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
