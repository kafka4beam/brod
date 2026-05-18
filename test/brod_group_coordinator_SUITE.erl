%%%
%%%   Copyright (c) 2015-2021 Klarna Bank AB (publ)
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
-module(brod_group_coordinator_SUITE).
-define(CLIENT_ID, ?MODULE).
-define(OTHER_CLIENT_ID, other_coordinator_id).
-define(TOPIC, <<"brod-group-coordinator">>).
-define(TOPIC1, <<"brod-group-coordinator-1">>).
-define(GROUP, <<"brod-group-coordinator">>).
-define(PARTITION, 0).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , common_init_per_testcase/2
        , common_end_per_testcase/2
        , suite/0
        ]).

%% brod coordinator callbacks
-export([ assignments_revoked/1
        , assignments_received/4
        ]).

%% Test cases
-export([ t_acks_during_revoke/1
        , t_update_topics_triggers_rebalance/1
        , t_offset_fetch_minus_one_falls_back_to_reset_policy/1
        ]).

-define(assert_receive(Pattern, Return),
  receive
    Pattern -> Return
  after
    30000 -> ct:fail(erlang:process_info(self(), messages))
  end).

-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").
-include("brod.hrl").

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

common_init_per_testcase(_Case, Config) ->
  {ok, _} = application:ensure_all_started(brod),
  BootstrapHosts = kafka_test_helper:bootstrap_hosts(),
  ClientConfig   = client_config(),
  ok = brod:start_client(BootstrapHosts, ?CLIENT_ID, ClientConfig),
  ok = brod:start_client(BootstrapHosts, ?OTHER_CLIENT_ID, ClientConfig),
  ok = brod:start_producer(?CLIENT_ID, ?TOPIC, _ProducerConfig = []),
  Config.

common_end_per_testcase(_Case, Config) when is_list(Config) ->
  ok = brod:stop_client(?CLIENT_ID),
  ok = brod:stop_client(?OTHER_CLIENT_ID),
  ok = application:stop(brod).

client_config() ->
  kafka_test_helper:client_config().

%%%_* Group coordinator callbacks ==============================================

assignments_revoked({Pid, Count}) ->
  Pid ! {assignments_revoked, Count},
  receive continue -> ok end,
  ok.

assignments_received({Pid, Count}, _MemberId, GenerationId, TopicAssignments) ->
  Pid ! {assignments_received, Count, GenerationId, TopicAssignments},
  ok.

%%%_* Test functions ===========================================================

t_acks_during_revoke(Config) when is_list(Config) ->
  {ok, GroupCoordinator1Pid} =
    brod_group_coordinator:start_link(?CLIENT_ID, ?GROUP, [?TOPIC],
                                      _Config = [], ?MODULE, {self(), 1}),

  ?assert_receive({assignments_revoked, 1}, ok),
  GroupCoordinator1Pid ! continue,
  GenerationId = ?assert_receive({assignments_received, 1, GId, _}, GId),

  {ok, Offset} =
    brod:produce_sync_offset(?CLIENT_ID, ?TOPIC, ?PARTITION, <<>>, <<1, 2, 3>>),

  {ok, {_, [_]}} = brod:fetch(?CLIENT_ID, ?TOPIC, ?PARTITION, Offset),

  {ok, GroupCoordinator2Pid} =
    brod_group_coordinator:start_link(?OTHER_CLIENT_ID, ?GROUP, [?TOPIC],
                                      _Config = [], ?MODULE, {self(), 2}),

  %% Allow new partition to be started
  ?assert_receive({assignments_revoked, 2}, ok),
  GroupCoordinator2Pid ! continue,

  %% We only ack when we are inside assignments_revoked
  ?assert_receive({assignments_revoked, 1}, ok),
  brod_group_coordinator:ack(GroupCoordinator1Pid, GenerationId,
                             ?TOPIC, ?PARTITION, Offset),
  GroupCoordinator1Pid ! continue,

  TopicAssignments1 = ?assert_receive({assignments_received, 1, _, TA1}, TA1),
  TopicAssignments2 = ?assert_receive({assignments_received, 2, _, TA2}, TA2),
  Assignments = TopicAssignments1 ++ TopicAssignments2,

  %% The assignment needs to start at the chosen offset.
  ?assertMatch( [ok]
              , [ok || #brod_received_assignment{
                         partition=?PARTITION,
                         begin_offset=BeginOffset
                       } <- Assignments,
                       BeginOffset == Offset + 1]
              ),

  ok.

t_update_topics_triggers_rebalance(Config) when is_list(Config) ->
  {ok, GroupCoordinatorPid} =
    brod_group_coordinator:start_link(?CLIENT_ID, ?GROUP, [?TOPIC],
                                      _Config = [], ?MODULE, {self(), 1}),
  ?assert_receive({assignments_revoked, 1}, ok),
  GroupCoordinatorPid ! continue,
  GenerationId1 = ?assert_receive({assignments_received, 1, GId1, _}, GId1),
  brod_group_coordinator:update_topics(GroupCoordinatorPid, [?TOPIC1]),
  ?assert_receive({assignments_revoked, 1}, ok),
  GroupCoordinatorPid ! continue,
  {GenerationId2, TopicAssignments} =
    ?assert_receive({assignments_received, 1, GId2, TA}, {GId2, TA}),
  ?assert(GenerationId2 > GenerationId1),
  ?assert(lists:all(
            fun(#brod_received_assignment{topic=Topic}) ->
              Topic == ?TOPIC1
            end, TopicAssignments)).

%% When Kafka's OffsetFetch returns committed_offset=-1 (error_code=NONE), it
%% means "no committed offset exists" (e.g. new group, topic recreated,
%% offsets.retention.minutes expired). Previously the partition was silently
%% dropped, begin_offset became `undefined', and brod_group_subscriber fell
%% back to the consumer's default begin_offset with no log.
%%
%% After the fix the assignment carries begin_offset=undefined for those
%% partitions and the subscriber resolves it via the consumer config's
%% begin_offset / offset_reset_policy with a log entry. With
%% reset_to_earliest, the consumer's first Fetch request must target
%% offset 0, not "latest".
t_offset_fetch_minus_one_falls_back_to_reset_policy({init, Config}) ->
  meck:new(brod_utils, [passthrough]),
  meck:new(brod_kafka_request, [passthrough]),
  Config;
t_offset_fetch_minus_one_falls_back_to_reset_policy({'end', _Config}) ->
  meck:unload(brod_kafka_request),
  meck:unload(brod_utils),
  ok;
t_offset_fetch_minus_one_falls_back_to_reset_policy(Config) when is_list(Config) ->
  Topic = ?TOPIC,
  %% Topic has 3 partitions (created by setup-test-env.sh).
  Partitions = [0, 1, 2],
  %% Produce a message so the partition has a non-zero high-watermark.
  %% earliest is still offset 0 regardless of HWM.
  {ok, _} = brod:produce_sync_offset(?CLIENT_ID, Topic, ?PARTITION,
                                     <<>>, <<"msg">>),
  %% Inject a fake OffsetFetch response: committed_offset=-1 for every
  %% partition ("no committed offset").
  FakeBody = fake_offset_fetch_minus_one_body(Topic, Partitions),
  meck:expect(brod_utils, request_sync,
    fun(_Conn, #kpro_req{api = offset_fetch}, _Timeout) ->
        {ok, FakeBody};
       (Conn, Req, Timeout) ->
        meck:passthrough([Conn, Req, Timeout])
    end),
  %% Capture the offset arg of the first Kafka Fetch request per partition.
  Self = self(),
  meck:expect(brod_kafka_request, fetch,
    fun(Pid, T, P, Offset, WT, MinB, MaxB, IL) ->
        Self ! {fetch, T, P, Offset},
        meck:passthrough([Pid, T, P, Offset, WT, MinB, MaxB, IL])
    end),
  %% Start a real v2 subscriber with reset_to_earliest. A unique group id is
  %% not strictly required (the OffsetFetch meck overrides), but it avoids
  %% interacting with offsets committed by earlier test cases.
  GroupId = list_to_binary(
              "brod-grp-coord-no-commit-" ++
              integer_to_list(erlang:unique_integer([positive]))),
  ConsumerConfig = [{offset_reset_policy, reset_to_earliest}],
  {ok, SubscriberPid} =
    brod:start_link_group_subscriber_v2(
      #{ client          => ?CLIENT_ID
       , group_id        => GroupId
       , topics          => [Topic]
       , group_config    => []
       , consumer_config => ConsumerConfig
       , cb_module       => brod_test_group_subscriber
       , init_data       => #{}
       }),
  try
    FetchOffset = receive
                    {fetch, Topic, ?PARTITION, O} -> O
                  after 30000 ->
                    ct:fail({no_fetch_observed,
                             erlang:process_info(self(), messages)})
                  end,
    %% offset_reset_policy=reset_to_earliest => first fetch at offset 0.
    %% Pre-fix: would have been the HWM (>=1) since "latest" is the consumer
    %% default begin_offset.
    ?assertEqual(0, FetchOffset)
  after
    ok = brod_group_subscriber_v2:stop(SubscriberPid)
  end.

%% Build an OffsetFetch response returning committed_offset=-1 for every
%% listed partition of `Topic'.
fake_offset_fetch_minus_one_body(Topic, Partitions) ->
  [ {error_code, ?no_error}
  , {topics,
     [ [ {name, Topic}
       , {partitions,
          [ [ {partition_index, P}
            , {committed_offset, -1}
            , {metadata, <<>>}
            , {error_code, ?no_error}
            ]
            || P <- Partitions
          ]}
       ]
     ]}
  ].


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
