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

-include("brod_test_setup.hrl").
-define(CLIENT_ID, ?MODULE). %% Client thats is being tested

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

-define(MESSAGE_TIMEOUT, 30000).

-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include("brod.hrl").
-include("brod_group_subscriber_test.hrl").

-define(handled_message(Topic, Partition, Value, Offset),
        #{ ?snk_kind := group_subscriber_handle_message
         , topic     := Topic
         , partition := Partition
         , value     := Value
         , offset    := Offset
         }).

-define(wait_message(Topic, Partition, Value, Offset),
        ?block_until( ?handled_message(Topic, Partition, Value, Offset)
                    , ?MESSAGE_TIMEOUT, infinity
                    )).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config0) ->
  Config = [ {behavior, brod_group_subscriber}
           , {max_seqno, 100}
           | Config0],
  kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) -> ok.

groups() ->
  [ {brod_group_subscriber_v2,
     [ t_async_acks
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

common_init_per_testcase(Case, Config0) ->
  Config = kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config0),
  BootstrapHosts = bootstrap_hosts(),
  ClientConfig   = client_config(),
  ok = brod:start_client(BootstrapHosts, ?CLIENT_ID, ClientConfig),
  Config.

common_end_per_testcase(Case, Config) ->
  %% Clean up stuff while trying not to be killed by brod
  process_flag(trap_exit, true),
  catch brod:stop_client(?CLIENT_ID),
  kafka_test_helper:common_end_per_testcase(Case, Config),
  receive
    {'EXIT', From, Reason} ->
      ?log(warning, "Refusing to become collateral damage."
                    " Offender: ~p Reason: ~p",
           [From, Reason])
  after 0 ->
      ok
  end.

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
  Topic = ?topic,
  Partition = 0,
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       %% Produce some messages into the topic:
       {_, L} = produce_payloads(Topic, Partition, Config),
       {ok, SubscriberPid} = start_subscriber(Config, [Topic], InitArgs),
       %% And ack/commit them asynchronously:
       [begin
          {ok, #{offset := Offset}} = ?wait_message(Topic, Partition, I, _),
          ok = Behavior:ack(SubscriberPid, ?topic, Partition, Offset),
          ok = Behavior:commit(SubscriberPid, ?topic, Partition, Offset)
        end || I <- L],
       L
     end,
     %% Check stage:
     fun(L, Trace) ->
         check_all_messages_were_received_once(Trace, L)
     end).

t_consumer_crash(Config) when is_list(Config) ->
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  Behavior = ?config(behavior),
  InitArgs = #{async_ack => true},
  Topic = ?topic,
  Partition = 0,
  SendFun =
    fun(I) ->
        produce({Topic, Partition}, <<I>>)
    end,
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       {ok, SubscriberPid} = start_subscriber(Config, [Topic], InitArgs),
       %% send some messages
       [_, _, O3, _, O5] = [SendFun(I) || I <- lists:seq(1, 5)],
       %% Wait until the last one is processed
       {ok, _} = ?wait_message(Topic, Partition, <<5>>, _),
       %% Ack the 3rd one and do a sync request to the subscriber, so
       %% that we know it has processed the ack, then kill the
       %% brod_consumer process
       ok = Behavior:ack(SubscriberPid, Topic, Partition, O3),
       sys:get_state(SubscriberPid),
       {ok, ConsumerPid} = brod:get_consumer(?CLIENT_ID, Topic, Partition),
       kafka_test_helper:kill_process(ConsumerPid),
       %% send more messages (but they should not be received until
       %% re-subscribe)
       [SendFun(I) || I <- lists:seq(6, 8)],
       %% Ack O5:
       ?tp(ack_o5, #{}),
       ok = Behavior:ack(SubscriberPid, Topic, Partition, O5)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_all_messages_were_received_once( Trace
                                              , [<<I>> || I <- lists:seq(6, 8)]
                                              ),
         %% Check that messages 6 to 8 were processed after message 5
         %% was acked:
         {_BeforeAck, [_|AfterAck]} = ?split_trace_at( #{?snk_kind := ack_o5}
                                                     , Trace
                                                     ),
         ?assertEqual( [<<I>> || I <- lists:seq(6, 8)]
                     , ?projection(value, handled_messages(AfterAck))
                     )
     end).

t_2_members_subscribe_to_different_topics(topics) ->
  [{?topic(1), 1}, {?topic(2), 1}];
t_2_members_subscribe_to_different_topics(Config) when is_list(Config) ->
  Topic1 = ?topic(1),
  Topic2 = ?topic(2),
  InitArgs = #{},
  Partitioner = fun(_Topic, PartitionCnt, _Key, _Value) ->
                    {ok, rand_uniform(PartitionCnt)}
                end,
  SendFun =
    fun(Value) ->
      Topic =
        case rand_uniform(2) of
          0 -> Topic1;
          1 -> Topic2
        end,
      ok = brod:produce_sync(?TEST_CLIENT_ID, Topic, Partitioner, <<>>, Value)
    end,
  L = payloads(Config),
  LastMsg = lists:last(L),
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       {ok, SubscriberPid1} = start_subscriber(Config, [Topic1], InitArgs),
       {ok, SubscriberPid2} = start_subscriber(Config, [Topic2], InitArgs),
       %% Send messages to random partitions:
       lists:foreach(SendFun, L),
       ?wait_message(_, _, LastMsg, _)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_all_messages_were_received_once(Trace, L),
         %% Check that all messages belong to the topics we expect:
         ?projection_is_subset( topic
                              , handled_messages(Trace)
                              , [Topic1, Topic2]
                              )
     end).

%% ?topic has only one partition, this case is to test two group members
%% working with only one partition, this makes one member idle but should
%% not crash.
t_2_members_one_partition(Config) when is_list(Config) ->
  Topic = ?topic,
  InitArgs = #{},
  ?check_trace(
     %% Run stage:
     begin
       %% Send messages:
       {LastOffset, L} = produce_payloads(Topic, 0, Config),
       %% Start two subscribers competing for the partition:
       {ok, SubscriberPid1} = start_subscriber(Config, [Topic], InitArgs),
       {ok, SubscriberPid2} = start_subscriber(Config, [Topic], InitArgs),
       ?wait_message(_, _, _, LastOffset),
       L
     end,
     %% Check stage:
     fun(L, Trace) ->
         check_all_messages_were_received_once(Trace, L),
         %% Check that all messages were received by the same process:
         PidsOfWorkers = ?projection(worker, handled_messages(Trace)),
         ?assertMatch( [_]
                     , lists:usort(PidsOfWorkers)
                     )
     end).

t_async_commit({init, Config}) ->
  GroupId = list_to_binary("one-time-gid-" ++
                           integer_to_list(rand:uniform(1000000000))),
  [{group_id, GroupId} | Config];
t_async_commit(Config) when is_list(Config) ->
  Behavior = ?config(behavior),
  Topic = ?topic,
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
        {ok, SubscriberPid} = start_subscriber(Config, [Topic],
                                               GroupConfig, ConsumerConfig,
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
        ok = Behavior:commit(Pid, Topic, 0, Offset)
    end,
  Msg = <<"test">>,
  ?check_trace(
     #{ timeout => 10000 },
     %% Run stage:
     begin
       %% Produce a message, start group subscriber from offset of
       %% that message and wait for the subscriber to receive it:
       produce({Topic, Partition}, Msg),
       Offset = produce({Topic, Partition}, Msg),
       ct:pal("Produced at offset = ~p~n", [Offset]),
       Pid1 = StartSubscriber(Offset),
       {ok, _} = ?wait_message(Topic, Partition, Msg, _),
       %% Commit _previous_ offset to avoid starting brod_consumer
       %% with `latest' offset and thus losing all data during
       %% restart:
       CommitOffset(Pid1, Offset - 1),
       ct:pal("Pre restart~n", []),
       %% Emulate subscriber restart:
       {Pid2, {ok, _}} =
         ?wait_async_action( EmulateRestart(Pid1)
                           , ?handled_message(Topic, Partition, Msg, _)
                           , 20000
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
           ?split_trace_at(#{?snk_kind := emulate_restart}, Trace),
         {BeforeRestart2, [_|AfterRestart2]} =
           ?split_trace_at(#{?snk_kind := emulate_restart}, AfterRestart1),
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
                     ),
         %% Check that terminate callback was called on each restart
         %% (for the new behavior):
         Behavior =:= brod_group_subscriber orelse
           ?assertMatch( [ #{topic := Topic, partition := Partition}
                         , #{topic := Topic, partition := Partition}
                         ]
                       , ?of_kind(brod_test_group_subscriber_terminate, Trace)
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
  {ok, SubscriberPid} = start_subscriber( Config, [?topic], GroupConfig
                                        , ConsumerConfig, InitArgs),
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

rand_uniform(Max) ->
  {_, _, Micro} = os:timestamp(),
  Micro rem Max.

start_subscriber(Config, Topics, InitArgs) ->
  %% use consumer managed offset commit behaviour by default, so we
  %% can control where to start fetching messages from
  DefaultGroupConfig = [ {offset_commit_policy, consumer_managed}
                       ],
  MaxSeqNo = ?config(max_seqno),
  DefaultConsumerConfig = [ {prefetch_count, MaxSeqNo}
                          , {sleep_timeout, 0}
                          , {max_wait_time, 100}
                          , {partition_restart_delay_seconds, 1}
                          , {begin_offset, 0}
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
  GroupId = case ?config(group_id) of
              undefined -> ?group_id;
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
  ?log(notice, "Started subscriber with pid=~p", [SubscriberPid]),
  {ok, SubscriberPid}.

stop_subscriber(Config, Pid) ->
  (?config(behavior)):stop(Pid).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
