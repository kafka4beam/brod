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
        , all/0
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
  ok = brod:stop_client(?CLIENT_ID).

%%%_* Group subscriber callbacks ===============================================

init(_GroupId,
     {CaseRef, CasePid, IsAsyncAck, IsAsyncCommit, IsAssignPartitions}) ->
  {ok, #state{ ct_case_ref          = CaseRef
             , ct_case_pid          = CasePid
             , is_async_ack         = IsAsyncAck
             , is_async_commit      = IsAsyncCommit
             , is_assign_partitions = IsAssignPartitions
             }}.

handle_message(Topic, Partition, Message,
               #state{ ct_case_ref     = Ref
                     , ct_case_pid     = Pid
                     , is_async_ack    = IsAsyncAck
                     , is_async_commit = IsAsyncCommit
                     , is_assign_partitions = IsAssignPartitions
                     } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  ?tp(group_subscriber_handle_message,
      #{ topic     => Topic
       , partition => Partition
       , offset    => Offset
       , value     => Value
       , ref       => Ref
       , worker    => self()
       }),
  case {IsAsyncAck, IsAsyncCommit, IsAssignPartitions} of
    {true, _, _}      -> {ok, State};
    {false, false, _} -> {ok, ack, State};
    {false, true, _}  -> {ok, ack_no_commit, State}
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
  CaseRef = t_async_acks,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true,
              _IsAsyncCommit = false, _IsAssignPartitions = false},
  Partition = 0,
  SendFun =
    fun(I) ->
      ok = brod:produce_sync(?CLIENT_ID, ?TOPIC1, Partition, <<>>, I)
    end,
  Timeout = 5000,
  L = [<<I>> || I <- lists:seq(1, ?config(max_seqno))],
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
          #{offset := Offset} =
            ?block_until(#{ kind  := group_subscriber_handle_message
                          , value := I
                          }, Timeout, Timeout),
          ok = Behavior:ack(SubscriberPid, ?TOPIC1, Partition, Offset),
          ok = Behavior:commit(SubscriberPid, ?TOPIC1, Partition, Offset)
        end || I <- L],
       SubscriberPid
     end,
     %% Check stage:
     fun(SubscriberPid, Trace) ->
         check_all_messages_are_received_once(Trace, L),
         stop_subscriber(Config, SubscriberPid)
     end).

t_consumer_crash(Config) when is_list(Config) ->
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  Behavior = ?config(behavior),
  CaseRef = t_consumer_crash,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true,
              _IsAsyncCommit = false, _IsAssignPartitions = false},
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
       ?assertMatch( #{}
                   , ?block_until( #{ kind      :=
                                        group_subscriber_handle_message
                                    , topic     := ?TOPIC1
                                    , partition := Partition
                                    , value     := <<5>>
                                    }
                                 , 3000, infinity
                                 )),
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
       ok = Behavior:ack(SubscriberPid, ?TOPIC1, Partition, O5),
       SubscriberPid
     end,
     %% Check stage:
     fun(SubscriberPid, Trace) ->
         check_all_messages_are_received_once( Trace
                                             , [<<I>> || I <- lists:seq(6, 8)]
                                             ),
         %% Check that messages 6 to 8 were processed after message 5
         %% was acked:
         {_BeforeAck, [_|AfterAck]} = lists:splitwith( fun(#{kind := ack_o5}) ->
                                                           false;
                                                          (_) ->
                                                           true
                                                       end
                                                     , Trace
                                                     ),
         ?projection_complete( value
                             , ?of_kind( group_subscriber_handle_message
                                       , AfterAck
                                       )
                             , [<<I>> || I <- lists:seq(6, 8)]
                             ),
         stop_subscriber(Config, SubscriberPid)
     end).

t_2_members_subscribe_to_different_topics(Config) when is_list(Config) ->
  CaseRef = t_2_members_subscribe_to_different_topics,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = false,
              _IsAsyncCommit = false, _IsAssignPartitions = false},
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
  L = [<<I>> || I <- lists:seq(1, ?config(max_seqno))],
  ?check_trace(
     #{ timeout => 5000 },
     %% Run stage:
     begin
       {ok, SubscriberPid1, _ConsumerPids1} =
         start_subscriber(Config, [?TOPIC2], InitArgs),
       {ok, SubscriberPid2, _ConsumerPids2} =
         start_subscriber(Config, [?TOPIC3], InitArgs),
       %% Send messages to random partitions:
       lists:foreach(SendFun, L),
       {SubscriberPid1, SubscriberPid2}
     end,
     %% Check stage:
     fun({Sub1, Sub2}, Trace) ->
         check_all_messages_are_received_once(Trace, L),
         %% Check that all messages belong to the topics we expect:
         ?projection_is_subset( topic
                              , ?of_kind( group_subscriber_handle_message
                                        , Trace
                                        )
                              , [?TOPIC2, ?TOPIC3]
                              ),
         %% Stop subscribers:
         stop_subscriber(Config, Sub1),
         stop_subscriber(Config, Sub2)
     end).

%% TOPIC4 has only one partition, this case is to test two group members
%% working with only one partition, this makes one member idle but should
%% not crash.
t_2_members_one_partition(Config) when is_list(Config) ->
  Topic = ?TOPIC4,
  MaxSeqNo = 100,
  GroupConfig = [],
  ConsumerConfig = [ {prefetch_count, MaxSeqNo}
                   , {prefetch_bytes, 0}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 100}
                   ],
  CaseRef = t_2_members_one_partition,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = false,
              _IsAsyncCommit = false, _IsAssignPartitions = false},
  {ok, SubscriberPid1, _ConsumerPids1} =
    start_subscriber(Config, [Topic], GroupConfig, ConsumerConfig, InitArgs),
  {ok, SubscriberPid2, _ConsumerPids1} =
    start_subscriber(Config, [Topic], GroupConfig, ConsumerConfig, InitArgs),
  SendFun =
    fun(I) ->
      Value = integer_to_binary(I),
      ok = brod:produce_sync(?CLIENT_ID, Topic, 0, <<>>, Value)
    end,
  RecvFun =
    fun Continue(Acc) when length(Acc) =:= MaxSeqNo -> lists:reverse(Acc);
        Continue(Acc) ->
          receive
            ?MSG(CaseRef, SubscriberPid, T, _Partition, _Offset, Value) ->
            %% assert subscribers assigned with only topics in subscription list
            ?assert((SubscriberPid =:= SubscriberPid1 andalso T =:= Topic)
                    orelse
                    (SubscriberPid =:= SubscriberPid2 andalso T =:= Topic)
                   ),
            Continue([binary_to_integer(Value) | Acc]);
        Msg ->
          erlang:error({unexpected_msg, Msg})
      after
        4000 ->
          erlang:error({timeout, Acc})
      end
    end,
  L = lists:seq(1, MaxSeqNo),
  ok = lists:foreach(SendFun, L),
  ?assertEqual(L, RecvFun([])),
  ok = stop_subscriber(Config, SubscriberPid1),
  ok = stop_subscriber(Config, SubscriberPid2),
  ok.

t_async_commit({init, Config}) ->
  meck:new(brod_group_coordinator,
           [passthrough, no_passthrough_cover, no_history]),
  Config;
t_async_commit(Config) when is_list(Config) ->
  Behavior = ?config(behavior),
  CaseRef = t_async_commit,
  CasePid = self(),
  Partition = 0,
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = false,
              _IsAsyncCommit = true, _IsAssignPartitions = false},
  %% use a one-time gid for clean test
  GroupId = list_to_binary("one-time-gid-" ++
                           integer_to_list(rand:uniform(1000000000))),
  StartSubscriber =
    fun() ->
        GroupConfig = [],
        ConsumerConfig = [ {sleep_timeout, 0}
                         , {begin_offset, latest}
                         , {prefetch_bytes, 0}
                         , {sleep_timeout, 0}
                         , {max_wait_time, 100}
                         ],
        {ok, SubscriberPid, _ConsumerPids} =
          start_subscriber(Config, [?TOPIC4], GroupConfig, ConsumerConfig, InitArgs),
        SubscriberPid
    end,
  EmulateRestart =
    fun(Pid) ->
        ct:pal("Stopping consumer for ~p", [?TOPIC4]),
        stop_subscriber(Config, Pid),
        StartSubscriber()
    end,
  CommitOffset =
    fun(Pid, Offset) ->
        ct:pal("Acking offset = ~p", [Offset]),
        ok = Behavior:commit(Pid, ?TOPIC4, 0, Offset),
        timer:sleep(5500)
    end,
  Pid1 = StartSubscriber(),
  {ok, Offset} = brod:produce_sync_offset(?CLIENT_ID, ?TOPIC4,
                                          Partition, <<>>, <<"test">>),
  ct:pal("Produced at offset = ~p", [Offset]),
  ?assertEqual([[Offset]],
               receive_match(4000, ?MSG(CaseRef, '_', ?TOPIC4,
                                        Partition, '$1', '_'))),
  %% Slightly unsound: commit _previous_ offset to avoid starting
  %% brod_consumer with `latest' offset and thus losing all data
  %% during restart:
  CommitOffset(Pid1, Offset - 1),
  %% Emulate subscriber restart:
  Pid2 = EmulateRestart(Pid1),
  %% Since we haven't commited offset, our message should be replayed:
  ?assertEqual([[Offset]],
               receive_match(4000, ?MSG(CaseRef, '_', ?TOPIC4,
                                        Partition, '$1', '_'))
              ),
  %% Commit offset and restart subscriber again:
  CommitOffset(Pid2, Offset),
  Pid3 = EmulateRestart(Pid2),
  %% This time we shouldn't receive anything:
  ?assertEqual([],
               receive_match(4000, ?MSG(CaseRef, '_', ?TOPIC4, 0, '$1', '_'))
              ),
  stop_subscriber(Config, Pid3),
  ok.

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
  CaseRef = t_assign_partitions_handles_updating_state,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true,
              _IsAsyncCommit = false, _IsAssignPartitions = true},
  {ok, SubscriberPid, _ConsumerPids} =
    start_subscriber(Config, [?TOPIC1], GroupConfig, ConsumerConfig, InitArgs),
  %% Since we only care about the assign_partitions part, we don't need to
  %% send and receive messages.
  ok = stop_subscriber(Config, SubscriberPid).

%%%_* Common checks ============================================================

check_all_messages_are_received_once(Trace, Values) ->
  Handled = ?of_kind(group_subscriber_handle_message, Trace),
  %% Check that all messages were handled:
  ?projection_complete( value
                      , Handled
                      , [<<I>> || I <- lists:seq(1, 8)]
                      ),
  %% ...and each message was handled only once:
  snabbkaffe:unique(Handled).

%%%_* Help funtions ============================================================

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

receive_match(Timeout, MatchSpec) ->
  MS = ets:match_spec_compile([{MatchSpec, [], ['$$']}]),
  Messages = receive_all(Timeout),
  ets:match_spec_run(Messages, MS).

receive_all(Timeout) ->
  lists:reverse(receive_all([], Timeout)).
receive_all(Msgs, Timeout) ->
  receive
    A ->
      ct:pal("Received ~p", [A]),
      receive_all([A|Msgs], Timeout)
  after Timeout ->
      Msgs
  end.

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
  case get(mocked) of
    true ->
      %% D'oh! Why meck is not idempotent?
      ok;
    _ ->
      meck_subscribe_unsubscribe(),
      put(mocked, true)
  end,
  {ok, SubscriberPid} =
    case ?config(behavior) of
     brod_group_subscriber_v2 ->
        GSConfig = #{ client          => ?CLIENT_ID
                    , group_id        => ?GROUP_ID
                    , topics          => Topics
                    , cb_module       => brod_test_group_subscriber
                    , init_data       => InitArgs
                    , consumer_config => ConsumerConfig
                    , group_config    => GroupConfig
                    },
        brod:start_link_group_subscriber_v2(GSConfig);
      brod_group_subscriber ->
        brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, Topics,
                                         GroupConfig, ConsumerConfig,
                                         ?MODULE, InitArgs)
    end,
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
  case ?config(behavior) of
    brod_group_subscriber_v2 ->
      brod_group_subscriber_v2:stop(Pid);
    brod_group_subscriber ->
      brod_group_subscriber:stop(Pid)
  end.

kill_process(Pid) ->
  Mon = monitor(process, Pid),
  ?tp(kill_consumer, #{}),
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
