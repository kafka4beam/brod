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
-export([ produce/4
        ]).

%% Debug API
-export([ debug/2
        , no_debug/1
        , get_sockets/1
        ]).


%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------
-type topic()     :: binary().
-type partition() :: non_neg_integer().
-type leader()    :: pid(). % brod_sock pid
-type corr_id()   :: non_neg_integer().
-type sender()    :: {pid(), reference()}.

-record(state, { hosts            :: [brod:host()]
               , debug            :: [term()]
               , acks             :: integer()
               , ack_timeout      :: integer()
               , sockets = []     :: [#socket{}]
               , leaders = []     :: [{{topic(), partition()}, leader()}]
               , data_buffer = [] :: [{leader(), [{topic(), dict()}]}]
               , senders_buffer = dict:new() :: dict() % (leader(), [sender()])
               , pending = []     :: [{leader(), {corr_id(), [sender()]}}]
               }).

%%%_* API ----------------------------------------------------------------------
-spec start_link([brod:host()], integer(), integer()) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, RequiredAcks, AckTimeout) ->
  start_link(Hosts, RequiredAcks, AckTimeout, []).

-spec start_link([brod:host()], integer(), integer(), [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, RequiredAcks, AckTimeout, Debug) ->
  gen_server:start_link(?MODULE, [Hosts, RequiredAcks, AckTimeout, Debug],
                        [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec produce(pid(), binary(), integer(), [{binary(), binary()}]) ->
                 {ok, reference()}.
produce(Pid, Topic, Partition, KVList) ->
  Sender = self(),
  Ref = erlang:make_ref(),
  gen_server:call(Pid, {produce, Sender, Ref, Topic, Partition, KVList}).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable debugging on producer and its connection to a broker
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
debug(Pid, print) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log, print});
debug(Pid, File) when is_list(File) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log_to_file, File}).

-spec no_debug(pid()) -> ok.
%% @doc Disable debugging
no_debug(Pid) ->
  do_debug(Pid, no_debug).

-spec get_sockets(pid()) -> {ok, [#socket{}]}.
get_sockets(Pid) ->
  gen_server:call(Pid, get_sockets).

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, RequiredAcks, AckTimeout, Debug]) ->
  erlang:process_flag(trap_exit, true),
  {ok, #state{ hosts       = Hosts
             , debug       = Debug
             , acks        = RequiredAcks
             , ack_timeout = AckTimeout
             }}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_sockets, _From, State) ->
  {reply, {ok, State#state.sockets}, State};
handle_call({produce, Sender, Ref, Topic, Partition, KVList}, _From, State0) ->
  case handle_produce(Sender, Ref, Topic, Partition, KVList, State0) of
    {ok, Leader, State1} ->
      {ok, State} = maybe_send(Leader, State1),
      {reply, {ok, Ref}, State};
    {error, Error} ->
      {reply, {error, Error}, State0}
  end;
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({msg, Leader, CorrId, #produce_response{}}, State0) ->
  {value, {_, {CorrId, SendersList}}, Pending} =
    lists:keytake(Leader, 1, State0#state.pending),
  lists:foreach(fun({Sender, Ref}) ->
                    Sender ! {{Ref, self()}, ack}
                end, SendersList),
  {ok, State} = maybe_send(Leader, State0#state{pending = Pending}),
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

format_status(_Opt, [_PDict, State0]) ->
  State = lists:zip(record_info(fields, state), tl(tuple_to_list(State0))),
  [{data, [{"State", State}]}].

%%%_* Internal functions -------------------------------------------------------
handle_produce(Sender, Ref, Topic, Partition, KVList, State0) ->
  case get_leader(Topic, Partition, State0) of
    {ok, Leader, State1} ->
      DataBuffer0 = State1#state.data_buffer,
      LeaderBuffer0 = get_leader_buffer(Leader, DataBuffer0),
      TopicBuffer0 = get_topic_buffer(Topic, LeaderBuffer0),
      TopicBuffer = dict:append_list(Partition, KVList, TopicBuffer0),
      LeaderBuffer = store_topic_buffer(Topic, LeaderBuffer0, TopicBuffer),
      DataBuffer = store_leader_buffer(Leader, DataBuffer0, LeaderBuffer),
      SendersBuffer = dict:append(Leader, {Sender, Ref},
                                  State1#state.senders_buffer),
      State = State1#state{ data_buffer    = DataBuffer
                          , senders_buffer = SendersBuffer},
      {ok, Leader, State};
    {error, Error} ->
      {error, Error}
  end.

get_leader(Topic, Partition, State) ->
  case lists:keyfind({Topic, Partition}, 1, State#state.leaders) of
    {_, Leader} ->
      {ok, Leader, State};
    false ->
      connect(Topic, Partition, State)
  end.

connect(Topic, Partition, State) ->
  {ok, Metadata} = brod_utils:get_metadata(State#state.hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  case lists:keyfind(Topic, #topic_metadata.name, Topics) of
    false ->
      {error, {unknown_topic, Topic}};
    #topic_metadata{partitions = Partitions} ->
      case lists:keyfind(Partition, #partition_metadata.id, Partitions) of
        false ->
          {error, {unknown_partition, Topic, Partition}};
        #partition_metadata{leader_id = LeaderId} ->
          Broker = lists:keyfind(LeaderId, #broker_metadata.node_id, Brokers),
          #broker_metadata{host = H, port = P} = Broker,
          {ok, Pid} = brod_sock:start_link(self(), H, P, State#state.debug),
          Socket = #socket{pid = Pid, host = H, port = P, node_id = LeaderId},
          Sockets = [Socket | State#state.sockets],
          Leaders = [{{Topic, Partition}, Pid} | State#state.leaders],
          {ok, Pid, State#state{sockets = Sockets, leaders = Leaders}}
      end
  end.

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
      SendersList = dict:fetch(Leader, State#state.senders_buffer),
      SendersBuffer = dict:erase(Leader, State#state.senders_buffer),
      Pending = [{Leader, {CorrId, SendersList}} | State#state.pending],
      {ok, State#state{ data_buffer    = DataBuffer
                      , senders_buffer = SendersBuffer
                      , pending        = Pending}};
    {_, _} -> % waiting for ack
      {ok, State}
  end.

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

do_debug(Pid, Debug) ->
  {ok, Sockets} = get_sockets(Pid),
  lists:foreach(
    fun(#socket{pid = SocketPid}) ->
        {ok, _} = gen:call(SocketPid, system, {debug, Debug})
    end, Sockets),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

%% Tests -----------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

mock_brod_sock() ->
  code:delete(brod_sock),
  code:purge(brod_sock),
  BrodDir = filename:dirname(filename:dirname(code:which(brod))),
  OutDir = filename:join(BrodDir, "test/mock"),
  {ok, brod_sock} =
    compile:file(filename:join(BrodDir, "test/mock/brod_sock"),
                 [{outdir, OutDir}, verbose, report_errors]),
  {module, brod_sock} = code:load_abs(filename:join(OutDir, "brod_sock")),
  ok.

connect_test() ->
  ok = mock_brod_sock(),
  Topic = <<"t">>,
  Partition = 0,
  H1 = {"h1", 2181}, H2 = {"h2", 2181}, H3 = {"h3", 2181},
  State0 = #state{hosts = [H1]},
  Error0 = {error, {unknown_topic, Topic}},
  ?assertEqual(Error0, connect(Topic, Partition, State0)),
  State1 = #state{hosts = [H2]},
  Error1 = {error, {unknown_partition, Topic, Partition}},
  ?assertEqual(Error1, connect(Topic, Partition, State1)),
  State2 = #state{hosts = [H3]},
  Socket = #socket{pid = p3, host = "h3", port = 2181, node_id = 1},
  Leader = {{Topic, Partition}, p3},
  State3 = State2#state{ sockets = [Socket]
                       , leaders = [Leader]},
  ?assertEqual({ok, p3, State3}, connect(Topic, Partition, State2)),
  ok.

maybe_send_test() ->
  ok = mock_brod_sock(),
  State0 = #state{data_buffer = [], pending = []},
  L1 = erlang:list_to_pid("<0.0.1>"),
  ?assertEqual({ok, State0}, maybe_send(L1, State0)),
  State1 = State0#state{pending = [{L1, 2}]},
  ?assertEqual({ok, State1}, maybe_send(L1, State1)),
  L2 = erlang:list_to_pid("<0.0.2>"),
  SendersBuffer = dict:store(L2, [d], dict:store(L1, [a, b, c], dict:new())),
  State2 = State0#state{ data_buffer = [{L1, foo}, {L2, bar}]
                       , senders_buffer = SendersBuffer},
  ExpectedState = #state{ data_buffer = [{L2, bar}]
                        , senders_buffer = dict:erase(L1, SendersBuffer)
                        , pending = [{L1, {1, [a, b, c]}}]},
  ?assertEqual({ok, ExpectedState}, maybe_send(L1, State2)),
  ok.

handle_produce_test() ->
  ok = mock_brod_sock(),
  T0 = <<"t">>,
  H1 = {"h1", 2181},
  State0 = #state{hosts = [H1]},
  Error0 = {error, {unknown_topic, T0}},
  ?assertEqual(Error0, handle_produce(pid, ref, T0, p0, [{k0, v0}], State0)),

  S1 = s1,
  R1 = r1,
  L1 = 1,
  T1 = t1,
  P1 = p1,
  K1 = k1,
  V1 = v1,
  Leaders0 = [{{T1, P1}, L1}],
  SendersBuffer1 = dict:store(L1, [{s1, r1}], dict:new()),
  TopicBuffer1 = dict:store(P1, [{K1, V1}], dict:new()),
  DataBuffer1 = [{L1, [{T1, TopicBuffer1}]}],
  State1 = #state{leaders = Leaders0},
  {ok, L1, State2} = handle_produce(S1, R1, T1, P1, [{K1, V1}], State1),
  ?assertEqual(Leaders0, State2#state.leaders),
  ?assertEqual(SendersBuffer1, State2#state.senders_buffer),
  ?assertEqual(DataBuffer1, State2#state.data_buffer),

  K2 = k2,
  V2 = v2,
  TopicBuffer2 = dict:append(P1, {K2, V2}, TopicBuffer1),
  DataBuffer2 = [{L1, [{T1, TopicBuffer2}]}],
  SendersBuffer2 = dict:append(L1, {S1, R1}, State2#state.senders_buffer),
  {ok, L1, State3} = handle_produce(S1, R1, T1, P1, [{K2, V2}], State2),
  ?assertEqual(Leaders0, State2#state.leaders),
  ?assertEqual(SendersBuffer2, State3#state.senders_buffer),
  ?assertEqual(DataBuffer2, State3#state.data_buffer),

  S2 = s2,
  R2 = r2,
  L2 = 2,
  P2 = p2,
  K3 = k3,
  V3 = v3,
  SendersBuffer3 = dict:append(L2, {S2, R2}, State3#state.senders_buffer),
  Leaders1 = [{{T1, P2}, L2} | Leaders0],
  State4 = State3#state{leaders = Leaders1},
  TopicBuffer3 = dict:append(P2, {K3, V3}, dict:new()),
  %% ++ due to lists:keystore/4 behaviour
  DataBuffer3 = State4#state.data_buffer ++ [{L2, [{T1, TopicBuffer3}]}],
  {ok, L2, State5} = handle_produce(S2, R2, T1, P2, [{K3, V3}], State4),
  ?assertEqual(Leaders1, State5#state.leaders),
  ?assertEqual(SendersBuffer3, State5#state.senders_buffer),
  ?assertEqual(DataBuffer3, State5#state.data_buffer),

  T2 = t2,
  S3 = s3,
  R3 = r3,
  K4 = k4,
  V4 = v4,
  SendersBuffer5 = dict:append(L1, {S3, R3}, State5#state.senders_buffer),
  Leaders2 = [{{T2, P1}, L1} | Leaders1],
  State6 = State5#state{leaders = Leaders2},
  Topic2Buffer = dict:append(P1, {K4, V4}, dict:new()),
  {value, {L1, [{T1, Topic1Buffer}]}, DataBuffer4} =
    lists:keytake(L1, 1, DataBuffer3),
  DataBuffer5 = [{L1, [{T1, Topic1Buffer}, {T2, Topic2Buffer}]} |
                DataBuffer4],
  {ok, L1, State7} = handle_produce(S3, R3, T2, P1, [{K4, V4}], State6),
  ?assertEqual(Leaders2, State7#state.leaders),
  ?assertEqual(SendersBuffer5, State7#state.senders_buffer),
  ?assertEqual(DataBuffer5, State7#state.data_buffer),
  ok.

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
