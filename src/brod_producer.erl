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
%%%=============================================================================

-module(brod_producer).

-behaviour(gen_server).

%% Server API
-export([ start_link/2
        , start_link/3
        , stop/1
        , subscribe/3
        , get_request/1
        ]).

%% Kafka API
-export([ produce/4
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
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------

-record(state, { hosts            :: [brod:host()]
               , debug            :: [term()]
               , sup_pid          :: pid()
               , options          :: proplists:proplist()
               , t_leaders        :: ets:tab()
               , blocked_requests :: queue:queue()
               , brokers          :: [#broker_metadata{}]
               , workers          :: [{leader_id(), pid()}]
               %% pending requests
               , leader_queues    :: [{leader_id(), queue:queue()}]
               , max_leader_queue_len :: integer()
               , sup_restarts_cnt :: integer()
               }).

%%%_* Macros -------------------------------------------------------------------
-define(SUP_RESTART_COOLDOWN_BASE_MS, 1000).
-define(MAX_SUP_RESTART_COOLDOWN_MS,  60000).

-define(DEFAULT_MAX_LEADER_QUEUE_LEN, 32).

%% ets table to keep incoming requests so that we
%% can resend data to kafka in case of leader rebalancing
%% and other errors we can handle here
-define(T_REQUESTS, t_requests).

%%%_* API ----------------------------------------------------------------------
-spec start_link([brod:host()], proplists:proplist()) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options) ->
  start_link(Hosts, Options, []).

-spec start_link([brod:host()], proplists:proplist(), [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options, Debug) ->
  case proplists:get_value(producer_id, Options) of
    undefined ->
      gen_server:start_link(?MODULE, [Hosts, Options, Debug], [{debug, Debug}]);
    ProducerId when is_atom(ProducerId) ->
      gen_server:start_link({local, ProducerId}, ?MODULE, [Hosts, Options, Debug], [{debug, Debug}])
  end.

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

-spec produce(pid(), binary(), integer(), [{binary(), binary()}]) ->
                 {ok, reference()} | ok.
produce(Pid, Topic, Partition, KVList) ->
  Ref = erlang:make_ref(),
  SenderPid = self(),
  Msg = {produce, Ref, SenderPid, Topic, Partition, KVList},
  gen_server:call(Pid, Msg, infinity).

subscribe(Pid, WorkerPid, LeaderId) ->
  gen_server:call(Pid, {subscribe, WorkerPid, LeaderId}, infinity).

get_request(RequestRef) ->
  [Request] = ets:lookup(?T_REQUESTS, RequestRef),
  {ok, Request}.

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, Options, Debug]) ->
  erlang:process_flag(trap_exit, true),
  MaxLeaderQueueLen = proplists:get_value(max_leader_queue_len, Options,
                                          ?DEFAULT_MAX_LEADER_QUEUE_LEN),
  TabLeaders = ets:new(t_leaders, [protected]),
  ets:new(?T_REQUESTS, [protected, named_table, ordered_set,
                        {keypos, #request.ref}]),
  {ok, SupPid} = brod_producer_sup:start_link(),
  {ok, Brokers} = refresh_metadata(Hosts, TabLeaders),
  {ok, #state{ hosts            = Hosts
             , debug            = Debug
             , sup_pid          = SupPid
             , options          = Options
             , t_leaders        = TabLeaders
             , blocked_requests = queue:new()
             , brokers          = Brokers
             , workers          = []
             , leader_queues    = []
             , max_leader_queue_len = MaxLeaderQueueLen
             }}.

handle_call({produce, Ref, SenderPid, Topic, Partition, KVList},
            From, State) ->
  R = #request{ ref        = Ref
              , sender_pid = SenderPid
              , from       = From
              , topic      = Topic
              , partition  = Partition
              , data       = KVList},
  ets:insert(?T_REQUESTS, R),
  {ok, LeaderId} = get_leader_id(State#state.t_leaders, Topic, Partition),
  case lists:keyfind(LeaderId, 1, State#state.workers) of
    {_, WorkerPid} ->
      {_, LeaderQueue} = lists:keyfind(LeaderId, 1, State#state.leader_queues),
      case queue:len(LeaderQueue) < State#state.max_leader_queue_len of
        true ->
          ok = brod_producer_worker:produce(WorkerPid, Ref),
          LeaderQueues = lists:keystore(LeaderId, 1, State#state.leader_queues,
                                        {LeaderId, queue:in(Ref, LeaderQueue)}),
          {reply, {ok, Ref}, State#state{leader_queues = LeaderQueues}};
        false ->
          Blocked = queue:in({Ref, From}, State#state.blocked_requests),
          {noreply, State#state{blocked_requests = Blocked}}
      end;
    false ->
      ok = spawn_worker(Topic, Partition, State),
      Blocked = queue:in({Ref, From}, State#state.blocked_requests),
      {noreply, State#state{blocked_requests = Blocked}}
  end;
handle_call({subscribe, WorkerPid, LeaderId}, _From, State0) ->
  %% register worker
  Workers =
    lists:keystore(LeaderId, 1, State0#state.workers, {LeaderId, WorkerPid}),
  State = State0#state{workers = Workers},
  TabLeaders = State#state.t_leaders,
  EmptyQueue = queue:new(),
  case lists:keyfind(LeaderId, 1, State#state.leader_queues) of
    Res when Res =:= false orelse Res =:= EmptyQueue ->
      %% try to unblock producers when there are no pending requests
      Blocked0 = State#state.blocked_requests,
      {Queue, Blocked} =
        maybe_unblock(TabLeaders, LeaderId, WorkerPid, EmptyQueue, Blocked0),
      LeaderQueues = [{LeaderId, Queue} | State#state.leader_queues],
      {reply, ok, State#state{ leader_queues = LeaderQueues
                             , blocked_requests = Blocked
                             , sup_restarts_cnt = 0}}; % subscribe = recovery
    {_, Queue} ->
      %% re-send pending requests
      %% blocked will be picked up later in 'ack' handler
      ok = resend_outstanding(WorkerPid, queue:to_list(Queue)),
      {reply, ok, State#state{sup_restarts_cnt = 0}} % subscribe = recovery
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(Msg, State) ->
  error_logger:warning_msg("Unexpected cast in ~p(~p): ~p",
                           [?MODULE, self(), Msg]),
  {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #state{sup_pid = Pid} = State0) ->
  %% protection agains restarts in a tight loop when cluster is unavailable
  SupRestartsCnt = State0#state.sup_restarts_cnt + 1,
  SleepMs = erlang:min(?SUP_RESTART_COOLDOWN_BASE_MS bsl SupRestartsCnt,
                       ?MAX_SUP_RESTART_COOLDOWN_MS),
  timer:sleep(SleepMs),
  error_logger:info_msg("Workers supervisor ~p crashed, recovering~n", [Pid]),
  TabLeaders = State0#state.t_leaders,
  ets:delete_all_objects(TabLeaders),
  {ok, SupPid} = brod_producer_sup:start_link(),
  {ok, Brokers} = refresh_metadata(State0#state.hosts, TabLeaders),
  State = State0#state{sup_pid = SupPid, brokers = Brokers},
  BlockedList = queue:to_list(State#state.blocked_requests),
  %% loop over ?T_REQUESTS and create new leader queues
  %% skip requests which are in blocked_reqeusts queue
  F = fun(#request{} = R, LeaderQueues) ->
          Ref = R#request.ref,
          case lists:keyfind(Ref, 2, BlockedList) of
            false ->
              Topic = R#request.topic,
              Partition = R#request.partition,
              {ok, LeaderId} = get_leader_id(TabLeaders, Topic, Partition),
              case lists:keyfind(LeaderId, 1, LeaderQueues) of
                {_, Q} ->
                  lists:keystore(LeaderId, 1, LeaderQueues,
                                 {LeaderId, queue:in(Ref, Q)});
                false ->
                  [{LeaderId, queue:in(Ref, queue:new())} | LeaderQueues]
              end;
            {_, _} ->
              LeaderQueues
          end
      end,
  LeaderQueues = ets:foldl(F, [], ?T_REQUESTS),
  %% spawn workers for every leader
  lists:foreach(
    fun({LeaderId, _}) ->
        Broker = lists:keyfind(LeaderId, #broker_metadata.node_id, Brokers),
        ok = spawn_worker(Broker, State)
    end, LeaderQueues),
  {noreply, State#state{ leader_queues = LeaderQueues
                       , sup_restarts_cnt = SupRestartsCnt}};
handle_info({{Ref, WorkerPid}, ack}, State) ->
  [Request] = ets:lookup(?T_REQUESTS, Ref),
  Request#request.sender_pid ! {{Ref, self()}, ack},
  ets:delete(?T_REQUESTS, Ref),
  {LeaderId, WorkerPid} = lists:keyfind(WorkerPid, 2, State#state.workers),
  {_, LeaderQueue0} = lists:keyfind(LeaderId, 1, State#state.leader_queues),
  {{value, Ref}, LeaderQueue1} = queue:out(LeaderQueue0),
  Blocked0 = State#state.blocked_requests,
  TabLeaders = State#state.t_leaders,
  {LeaderQueue, Blocked} =
    maybe_unblock(TabLeaders, LeaderId, WorkerPid, LeaderQueue1, Blocked0),
  LeaderQueues = lists:keystore(LeaderId, 1, State#state.leader_queues,
                                {LeaderId, LeaderQueue}),
  {noreply, State#state{ blocked_requests = Blocked
                       , leader_queues = LeaderQueues}};
handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected info: ~p", [Info]),
  {noreply, State}.

terminate(_Reason, #state{sup_pid = SupPid}) ->
  case brod_utils:is_pid_alive(SupPid) of
    true  -> exit(SupPid, shutdown);
    false -> ok
  end.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
resend_outstanding(_WorkerPid, []) ->
  ok;
resend_outstanding(WorkerPid, [Ref | Tail]) ->
  ok = brod_producer_worker:produce(WorkerPid, Ref),
  resend_outstanding(WorkerPid, Tail).

maybe_unblock(TabLeaders, LeaderId, WorkerPid, LeaderQueue, BlockedQueue) ->
  L = queue:to_list(BlockedQueue),
  maybe_unblock(TabLeaders, LeaderId, WorkerPid, LeaderQueue, L, []).

maybe_unblock(_TabLeaders, _LeaderId, _WorkerPid, LeaderQueue, [], Acc) ->
  {LeaderQueue, queue:from_list(lists:reverse(Acc))};
maybe_unblock(TabLeaders, LeaderId, WorkerPid, LeaderQueue,
              [{Ref, From} | Tail], Acc) ->
  [[Topic, Partition]] = ets:match(?T_REQUESTS, #request{ ref = Ref
                                                        , topic = '$1'
                                                        , partition = '$2'
                                                        , _ = '_'}),
  case get_leader_id(TabLeaders, Topic, Partition) of
    {ok, LeaderId} ->
      ok = brod_producer_worker:produce(WorkerPid, Ref),
      gen_server:reply(From, {ok, Ref}),
      maybe_unblock(TabLeaders, LeaderId, WorkerPid,
                    queue:in(Ref, LeaderQueue), Tail, Acc);
    _ ->
      maybe_unblock(TabLeaders, LeaderId, WorkerPid, Tail, [Ref | Acc])
  end.

get_leader_id(Tab, Topic, Partition) ->
  case ets:lookup(Tab, {Topic, Partition}) of
    [{_, #broker_metadata{node_id = Id}}] ->
      {ok, Id};
    [] ->
      false
  end.

spawn_worker(Topic, Partition, State) ->
  case ets:lookup(State#state.t_leaders, {Topic, Partition}) of
    [{_, #broker_metadata{} = Broker}] ->
      spawn_worker(Broker, State);
    [] ->
      Error = lists:flatten(
                io_lib:format("Unknown Topic/Partittion: ~s.~B",
                              [Topic, Partition])),
      {error, Error}
  end.

spawn_worker(#broker_metadata{} = Broker, State) ->
  Id = Broker#broker_metadata.node_id,
  Host = Broker#broker_metadata.host,
  Port = Broker#broker_metadata.port,
  Options = State#state.options,
  Debug = State#state.debug,
  Args = [self(), Id, Host, Port, Options, Debug],
  case brod_producer_sup:start_worker(State#state.sup_pid, Args) of
    {ok, _Pid} ->
      ok;
    Error ->
      {error, Error}
  end.

-spec refresh_metadata([brod:host()], ets:tab()) -> ok.
refresh_metadata(Hosts, TabLeaders) ->
  {ok, Metadata} = brod_utils:get_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  ets:match_delete(TabLeaders, '_'),
  %% fill in {Topic, Partition} -> Leader mapping table
  [ begin
      Broker = lists:keyfind(Id, #broker_metadata.node_id, Brokers),
      ets:insert(TabLeaders, {{T, P}, Broker})
    end || #topic_metadata{name = T, partitions = PS} <- Topics,
           #partition_metadata{leader_id = Id, id = P} <- PS
  ],
  {ok, Brokers}.

%% Tests -----------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

mock_brod_sock() ->
  meck:new(brod_sock, [passthrough]),
  meck:expect(brod_sock, start_link,
              fun(_Parent, "h1", _Port, _ClientId, _Debug) -> {ok, p1};
                 (_Parent, "h2", _Port, _ClientId, _Debug) -> {ok, p2};
                 (_Parent, "h3", _Port, _ClientId, _Debug) -> {ok, p3}
              end),

  meck:expect(brod_sock, start,
              fun(_Parent, "h1", _Port, _ClientId, _Debug) -> {ok, p1};
                 (_Parent, "h2", _Port, _ClientId, _Debug) -> {ok, p2};
                 (_Parent, "h3", _Port, _ClientId, _Debug) -> {ok, p3}
              end),

  meck:expect(brod_sock, stop, 1, ok),
  meck:expect(brod_sock, send, 2, {ok, 1}),

  meck:expect(brod_sock, send_sync,
              fun(p1, #metadata_request{}, _Timeout) ->
                  {ok, #metadata_response{brokers = [], topics = []}};
                 (p2, #metadata_request{}, _Timeout) ->
                  Topics = [#topic_metadata{name = <<"t">>, partitions = []}],
                  {ok, #metadata_response{brokers = [], topics = Topics}};
                 (p3, #metadata_request{}, _Timeout) ->
                  Brokers = [#broker_metadata{ node_id = 1,
                                               host = "h3",
                                               port = 2181}],
                  Partitions = [#partition_metadata{id = 0, leader_id = 1}],
                  Topics = [#topic_metadata{ name = <<"t">>,
                                             partitions = Partitions}],
                  {ok, #metadata_response{brokers = Brokers, topics = Topics}}
              end),
  ok.

validate_mock() ->
  true = meck:validate(brod_sock),
  ok   = meck:unload(brod_sock).

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
  ok = validate_mock().

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
  ok = validate_mock().

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
  ok = validate_mock().

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
