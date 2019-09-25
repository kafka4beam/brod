%%%
%%%   Copyright (c) 2015-2018 Klarna Bank AB (publ)
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
-module(brod_group_subscriber_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , common_init_per_testcase/2
        , common_end_per_testcase/2
        , init_per_group/2
        , end_per_group/2
        , suite/0
        , groups/0
        ]).

%% brod subscriber callbacks
-export([ init/2
        , get_committed_offsets/3
        , handle_message/4
        , rand_uniform/1
        , assign_partitions/3
        ]).

%% Test cases
-export([ t_async_acks/1
        , t_koc_demo/1
        , t_koc_demo_message_set/1
        , t_loc_demo/1
        , t_loc_demo_message_set/1
        , t_2_members_subscribe_to_different_topics/1
        , t_2_members_one_partition/1
        , t_async_commit/1
        , t_consumer_crash/1
        , t_assign_partitions_handles_updating_state/1
        ]).


-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include("brod.hrl").
-include("brod_group_subscriber_test.hrl").

-define(handled_message(Topic, Partition, Value),
        #{ kind      := group_subscriber_handle_message
         , topic     := Topic
         , partition := Partition
         , value     := Value
         }).

-define(wait_message(Topic, Partition, Value),
        ?block_until( ?handled_message(Topic, Partition, Value)
                    , 5000, infinity
                    )).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  [ {behavior, brod_group_subscriber}
  , {max_seqno, 100}
  | Config].

end_per_suite(_Config) -> ok.

groups() ->
  [ {brod_group_subscriber_v2, [ t_async_acks
                               , t_consumer_crash
                               , t_2_members_subscribe_to_different_topics
                               , t_2_members_one_partition
                               , t_async_commit
                               , t_assign_partitions_handles_updating_state
                               ]}
  ].

init_per_group(brod_group_subscriber_v2, Config) ->
  [{behavior, brod_group_subscriber_v2} |
   lists:keydelete(behavior, 1, Config)];
init_per_group(_Group, Config) ->
  Config.

end_per_group(_Group, _Config) ->
  ok.

common_init_per_testcase(Case, Config) ->
  ClientId       = ?CLIENT_ID,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig   = client_config(),
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, ?TOPIC1, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC2, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC3, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC4, _ProducerConfig = []),
  Config.

common_end_per_testcase(Case, Config) when is_list(Config) ->
  catch meck:unload(),
  %% Clean up the processes:
  case get(subscribers) of
    undefined ->
      ct:pal("Nothing to clean up.", []),
      ok;
    Subscribers ->
      ct:pal("Cleaning up subscribers: ~p", [Subscribers]),
      [catch stop_subscriber(Config, Pid) || Pid <- Subscribers]
  end,
  ok = brod:stop_client(?CLIENT_ID).

%%%_* Group subscriber callbacks ===============================================

init(_GroupId, Config) ->
  IsAsyncAck         = maps:get(async_ack, Config, false),
  IsAsyncCommit      = maps:get(async_commit, Config, false),
  IsAssignPartitions = maps:get(assign_partitions, Config, false),
  {ok, #state{ is_async_ack         = IsAsyncAck
             , is_async_commit      = IsAsyncCommit
             , is_assign_partitions = IsAssignPartitions
             }}.

handle_message(Topic, Partition, Message,
               #state{ is_async_ack    = IsAsyncAck
                     , is_async_commit = IsAsyncCommit
                     } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  ?tp(group_subscriber_handle_message,
      #{ topic     => Topic
       , partition => Partition
       , offset    => Offset
       , value     => Value
       , worker    => self()
       }),
  case {IsAsyncAck, IsAsyncCommit} of
    {true,  _}     -> {ok, State};
    {false, false} -> {ok, ack, State};
    {false, true}  -> {ok, ack_no_commit, State}
  end.

get_committed_offsets(_GroupId, _TopicPartitions, State) ->
  %% always return []: always fetch from latest available offset
  {ok, [], State}.

assign_partitions(MemberPid, TopicPartitions,
                  #state{is_assign_partitions = false} = _CbState) ->
  PartitionsAssignments = [{Topic, [PartitionsN]}
                           || {Topic, PartitionsN} <- TopicPartitions],
  [{element(1, hd(MemberPid)), PartitionsAssignments}];
assign_partitions(MemberPid, TopicPartitions,
                  #state{is_assign_partitions = true} = CbState) ->
  PartitionsAssignments = [{Topic, [PartitionsN]}
                           || {Topic, PartitionsN} <- TopicPartitions],
  {CbState, [{element(1, hd(MemberPid)), PartitionsAssignments}]}.

%%%_* Test functions ===========================================================

t_loc_demo(Config) when is_list(Config) ->
  CgId = iolist_to_binary("t_loc_demo-" ++
                          integer_to_list(erlang:system_time())),
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_loc:bootstrap(1, message, CgId),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_loc_demo_message_set(Config) when is_list(Config) ->
  CgId = iolist_to_binary("t_loc_demo_message_set-" ++
                          integer_to_list(erlang:system_time())),
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_loc:bootstrap(1, message_set, CgId),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_koc_demo(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_koc:bootstrap(1),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_koc_demo_message_set(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_koc:bootstrap(1, message_set),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_async_acks(Config) when is_list(Config) ->
  Behavior = ?config(behavior),
  InitArgs = #{async_ack => true},
  Partition = 0,
  SendFun =
    fun(I) ->
      ok = brod:produce_sync(?CLIENT_ID, ?TOPIC1, Partition, <<>>, I)
    end,
  Timeout = 5000,
  L = payloads(Config),
  ?check_trace(
     #{ timeout => Timeout },
     %% Run stage:
     begin
       {ok, SubscriberPid, _ConsumerPids} =
         start_subscriber(Config, [?TOPIC1], InitArgs),
       %% Produce some messages into the topic:
       lists:foreach(SendFun, L),
       %% And ack/commit them asynchronously:
       [begin
          {ok, #{offset := Offset}} = ?wait_message(?TOPIC1, Partition, I),
          ok = Behavior:ack(SubscriberPid, ?TOPIC1, Partition, Offset),
          ok = Behavior:commit(SubscriberPid, ?TOPIC1, Partition, Offset)
        end || I <- L],
       ok
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_all_messages_were_received_once(Trace, L)
     end).

t_consumer_crash(Config) when is_list(Config) ->
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  Behavior = ?config(behavior),
  InitArgs = #{async_ack => true},
  Partition = 0,
  SendFun =
    fun(I) ->
        {ok, Offset} = brod:produce_sync_offset( ?CLIENT_ID, ?TOPIC1, Partition
                                               , <<>> , <<I>>
                                               ),
        Offset
    end,
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       {ok, SubscriberPid, _ConsumerPids} =
         start_subscriber( Config, [?TOPIC1], InitArgs),
       %% send some messages
       [_, _, O3, _, O5] = [SendFun(I) || I <- lists:seq(1, 5)],
       %% Wait until the last one is processed
       {ok, _} = ?wait_message(?TOPIC1, Partition, <<5>>),
       %% Ack the 3rd one and do a sync request to the subscriber, so
       %% that we know it has processed the ack, then kill the
       %% brod_consumer process
       ok = Behavior:ack(SubscriberPid, ?TOPIC1, Partition, O3),
       sys:get_state(SubscriberPid),
       {ok, ConsumerPid} = brod:get_consumer(?CLIENT_ID, ?TOPIC1, Partition),
       kill_process(ConsumerPid),
       %% send more messages (but they should not be received until
       %% re-subscribe)
       [SendFun(I) || I <- lists:seq(6, 8)],
       %% Ack O5:
       ?tp(ack_o5, #{}),
       ok = Behavior:ack(SubscriberPid, ?TOPIC1, Partition, O5)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_all_messages_were_received_once( Trace
                                              , [<<I>> || I <- lists:seq(6, 8)]
                                              ),
         %% Check that messages 6 to 8 were processed after message 5
         %% was acked:
         {_BeforeAck, [_|AfterAck]} = ?split_trace_at(#{kind := ack_o5}, Trace),
         ?assertEqual( [<<I>> || I <- lists:seq(6, 8)]
                     , ?projection(value, handled_messages(AfterAck))
                     )
     end).

t_2_members_subscribe_to_different_topics(Config) when is_list(Config) ->
  InitArgs = #{},
  Partitioner = fun(_Topic, PartitionCnt, _Key, _Value) ->
                    {ok, rand_uniform(PartitionCnt)}
                end,
  SendFun =
    fun(Value) ->
      Topic =
        case rand_uniform(2) of
          0 -> ?TOPIC2;
          1 -> ?TOPIC3
        end,
      ok = brod:produce_sync(?CLIENT_ID, Topic, Partitioner, <<>>, Value)
    end,
  L = payloads(Config),
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       {ok, SubscriberPid1, _ConsumerPids1} =
         start_subscriber(Config, [?TOPIC2], InitArgs),
       {ok, SubscriberPid2, _ConsumerPids2} =
         start_subscriber(Config, [?TOPIC3], InitArgs),
       %% Send messages to random partitions:
       lists:foreach(SendFun, L)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_all_messages_were_received_once(Trace, L),
         %% Check that all messages belong to the topics we expect:
         ?projection_is_subset( topic
                              , handled_messages(Trace)
                              , [?TOPIC2, ?TOPIC3]
                              )
     end).

%% TOPIC4 has only one partition, this case is to test two group members
%% working with only one partition, this makes one member idle but should
%% not crash.
t_2_members_one_partition(Config) when is_list(Config) ->
  Topic = ?TOPIC4,
  MaxSeqNo = 100,
  InitArgs = #{},
  L = payloads(Config),
  SendFun =
    fun(Value) ->
      ok = brod:produce_sync(?CLIENT_ID, Topic, 0, <<>>, Value)
    end,
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       %% Start two subscribers competing for the partition:
       {ok, SubscriberPid1, _ConsumerPids1} =
         start_subscriber(Config, [Topic], InitArgs),
       {ok, SubscriberPid2, _ConsumerPids1} =
         start_subscriber(Config, [Topic], InitArgs),
       %% Send messages:
       lists:foreach(SendFun, L)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_all_messages_were_received_once(Trace, L),
         %% Check that all messages were received by the same process:
         PidsOfWorkers = ?projection(worker, handled_messages(Trace)),
         ?assertMatch( [_]
                     , lists:usort(PidsOfWorkers)
                     )
     end).

t_async_commit({init, Config}) ->
  meck:new(brod_group_coordinator,
           [passthrough, no_passthrough_cover, no_history]),
  GroupId = list_to_binary("one-time-gid-" ++
                           integer_to_list(rand:uniform(1000000000))),
  [{group_id, GroupId} | Config];
t_async_commit(Config) when is_list(Config) ->
  Behavior = ?config(behavior),
  Partition = 0,
  InitArgs = #{async_commit => true},
  GroupConfig = [],
  StartSubscriber =
    fun(Offset) ->
        ConsumerConfig = [ {sleep_timeout, 0}
                         , {max_wait_time, 100}
                         , {partition_restart_delay_seconds, 1}
                         , {begin_offset, Offset}
                         ],
        {ok, SubscriberPid, _ConsumerPids} =
          start_subscriber(Config, [?TOPIC4], GroupConfig, ConsumerConfig,
                           InitArgs),
        SubscriberPid
    end,
  EmulateRestart =
    fun(Pid) ->
        ?tp(emulate_restart, #{old_pid => Pid}),
        stop_subscriber(Config, Pid),
        StartSubscriber(latest)
    end,
  CommitOffset =
    fun(Pid, Offset) ->
        ct:pal("Committing offset = ~p", [Offset]),
        ok = Behavior:commit(Pid, ?TOPIC4, 0, Offset)
    end,
  Msg = <<"test">>,
  ?check_trace(
     #{ timeout => 10000 },
     %% Run stage:
     begin
       %% Produce a message, start group subscriber from offset of
       %% that message and wait for the subscriber to receive it:
       {ok, Offset} = brod:produce_sync_offset(?CLIENT_ID, ?TOPIC4,
                                               Partition, <<>>, Msg),
       ct:pal("Produced at offset = ~p~n", [Offset]),
       Pid1 = StartSubscriber(Offset),
       {ok, _} = ?wait_message(?TOPIC4, Partition, Msg),
       %% Slightly commit _previous_ offset to avoid starting
       %% brod_consumer with `latest' offset and thus losing all data
       %% during restart:
       CommitOffset(Pid1, Offset - 1),
       ct:pal("Pre restart~n", []),
       %% Emulate subscriber restart:
       {Pid2, {ok, _}} =
         ?wait_async_action( EmulateRestart(Pid1)
                           , ?handled_message(?TOPIC4, Partition, Msg)
                           ),
       ct:pal("Post restart~n", []),
       %% Commit offset and restart subscriber again:
       CommitOffset(Pid2, Offset),
       ct:pal("Post commit~n", []),
       EmulateRestart(Pid2)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         {BeforeRestart1, [_|AfterRestart1]} =
           ?split_trace_at(#{kind := emulate_restart}, Trace),
         {BeforeRestart2, [_|AfterRestart2]} =
           ?split_trace_at(#{kind := emulate_restart}, AfterRestart1),
         %% check that the message was processed once before 1st
         %% restart:
         ?assertMatch( [_]
                     , ?projection(value, handled_messages(BeforeRestart1))
                     ),
         %% Check that the message was processed once after 1st
         %% restart:
         ?assertMatch( [_]
                     , ?projection(value, handled_messages(AfterRestart1))
                     ),
         %% Check that the message wasn't processed after its offset
         %% had been committed:
         ?assertMatch( []
                     , ?projection(value, handled_messages(AfterRestart2))
                     )
     end).

t_assign_partitions_handles_updating_state(Config) when is_list(Config) ->
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  GroupConfig = [ {offset_commit_policy, consumer_managed}
                , {partition_assignment_strategy, callback_implemented}
                ],
  ConsumerConfig = [ {prefetch_count, 10}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   , {partition_restart_delay_seconds, 1}
                   ],
  InitArgs = #{ async_ack         => true
              , assign_partitions => true
              },
  {ok, SubscriberPid, _ConsumerPids} =
    start_subscriber(Config, [?TOPIC1], GroupConfig, ConsumerConfig, InitArgs),
  %% Since we only care about the assign_partitions part, we don't need to
  %% send and receive messages.
  ok = stop_subscriber(Config, SubscriberPid).

%%%_* Common checks ============================================================

check_all_messages_were_received_once(Trace, Values) ->
  Handled = handled_messages(Trace),
  %% Check that all messages were handled:
  ?projection_complete(value, Handled, Values),
  %% ...and each message was handled only once:
  snabbkaffe:unique(Handled).

%%%_* Help funtions ============================================================

handled_messages(Trace) ->
  ?of_kind(group_subscriber_handle_message, Trace).

%% To make test deterministic, we wait until all consumer group
%% members are ready to receive messages before start sending messages
%% for tests.
%%
%% `brod:subscribe' and `brod:unsubscribe' calls are mocked (with
%% passthrough). The mocked functions emit a `subscribed_partition'
%% (when partitions are assigned) or `unsubscribed_partition' (when
%% assignments revoked) event.
%%
%% This function maintains a list of subscription states,
%% and returns once all topic-partitions reach `subscribed' state.
wait_for_subscribers([], _) ->
  #{};
wait_for_subscribers(Topics, SubPid) ->
  [A|T] = [ wait_for_subscribers(Topic, SubPid, 10)
          || Topic <- Topics],
  lists:foldl(fun maps:merge/2, A, T).

wait_for_subscribers(Topic, SubPid, TimeoutSecs) ->
  Timeout = TimeoutSecs * 1000,
  {ok, NumPartitions} = brod_client:get_partitions_count(?CLIENT_ID, Topic),
  %% Wait for start of brod_subscribers:
  Events =
    [?block_until( #{ kind      := subscribed_partition
                    , topic     := Topic
                    , partition := Partition
                    }
                 , Timeout
                 , Timeout
                 )
     || Partition <- lists:seq(0, NumPartitions - 1)],
  %% Create a map of consumer pids:
  maps:from_list([ {{Topic, Partition}, Pid}
                 || #{ partition := Partition
                     , consumer_pid := Pid
                     } <- Events
                 ]).

rand_uniform(Max) ->
  {_, _, Micro} = os:timestamp(),
  Micro rem Max.

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

payloads(Config) ->
  [<<I>> || I <- lists:seq(1, ?config(max_seqno))].

start_subscriber(Config, Topics, InitArgs) ->
  %% use consumer managed offset commit behaviour by default, so we
  %% can control where to start fetching messages from
  DefaultGroupConfig = [{offset_commit_policy, consumer_managed}],
  MaxSeqNo = ?config(max_seqno),
  DefaultConsumerConfig = [ {prefetch_count, MaxSeqNo}
                          , {sleep_timeout, 0}
                          , {max_wait_time, 100}
                          , {partition_restart_delay_seconds, 1}
                          ],
  GroupConfig = proplists:get_value( group_subscriber_config
                                   , Config
                                   , DefaultGroupConfig
                                   ),
  ConsumerConfig = proplists:get_value( consumer_config
                                      , Config
                                      , DefaultConsumerConfig
                                      ),
  start_subscriber(Config, Topics, GroupConfig, ConsumerConfig, InitArgs).

start_subscriber(Config, Topics, GroupConfig, ConsumerConfig, InitArgs) ->
  case get(subscribers) of
    Subscribers when is_list(Subscribers) ->
      ok;
    undefined ->
      %% D'oh! Why couldn't they make meck idempotent?
      meck_subscribe_unsubscribe(),
      Subscribers = []
  end,
  GroupId = case ?config(group_id) of
              undefined -> ?GROUP_ID;
              A         -> A
            end,
  {ok, SubscriberPid} =
    case ?config(behavior) of
     brod_group_subscriber_v2 ->
        GSConfig = #{ client          => ?CLIENT_ID
                    , group_id        => GroupId
                    , topics          => Topics
                    , cb_module       => brod_test_group_subscriber
                    , message_type    => message
                    , init_data       => InitArgs
                    , consumer_config => ConsumerConfig
                    , group_config    => GroupConfig
                    },
        brod:start_link_group_subscriber_v2(GSConfig);
      brod_group_subscriber ->
        brod:start_link_group_subscriber(?CLIENT_ID, GroupId, Topics,
                                         GroupConfig, ConsumerConfig,
                                         ?MODULE, InitArgs)
    end,
  put(subscribers, [SubscriberPid | Subscribers]),
  Consumers = wait_for_subscribers(Topics, SubscriberPid),
  {ok, SubscriberPid, Consumers}.

meck_subscribe_unsubscribe() ->
  meck:new(brod, [passthrough, no_passthrough_cover, no_history]),
  meck:expect(brod, subscribe,
              fun(Client, Pid, Topic, Partition, Opts) ->
                  {ok, ConsumerPid} = meck:passthrough([Client, Pid, Topic,
                                                        Partition, Opts]),
                  ?tp(subscribed_partition,
                      #{ topic          => Topic
                       , partition      => Partition
                       , subscriber_pid => Pid
                       , consumer_pid   => ConsumerPid
                       }),
                  {ok, ConsumerPid}
              end),
  meck:expect(brod, unsubscribe,
              fun(ConsumerPid, SubscriberPid) ->
                  meck:passthrough([ConsumerPid, SubscriberPid]),
                  ?tp(unsubscribed_partition,
                      #{ subscriber_pid => SubscriberPid
                       , consumer_pid   => ConsumerPid
                       }),
                  ok
              end),
  ok.

stop_subscriber(Config, Pid) ->
  (?config(behavior)):stop(Pid).

kill_process(Pid) ->
  Mon = monitor(process, Pid),
  ?tp(kill_consumer, #{pid => Pid}),
  exit(Pid, kill),
  receive
    {'DOWN', Mon, process, Pid, killed} ->
      ok
  after 1000 ->
      ct:fail("timed out waiting for the process to die")
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
