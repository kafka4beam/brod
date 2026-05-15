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
        , t_offset_fetch_minus_one_silently_falls_back_to_begin_offset/1
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
%% means "no committed offset exists" — e.g. the group is new, the topic was
%% deleted and recreated, or offsets.retention.minutes expired.
%%
%% Bug in get_committed_offsets/2: when Offset0 =:= -1, the partition is
%% silently dropped from the result. resolve_begin_offsets/3 then returns
%% begin_offset=undefined, and brod_group_subscriber falls back to consumer
%% config (typically `latest`) with no error, no log, no alert.
%%
%% Desired behaviour: when OffsetFetch returns -1, surface it explicitly so
%% the subscriber can apply offset_reset_policy — the same contract that
%% OFFSET_OUT_OF_RANGE already provides.
%%
%% This test FAILS today — it demonstrates the gap, not the fix.
%% Fix direction: in get_committed_offsets/2, when Offset0 =:= -1, add
%%   {{Topic, Partition}, {error, no_committed_offset}} to the accumulator
%%   and handle it in brod_group_subscriber like OFFSET_OUT_OF_RANGE.
%% Expected result once fixed: assignments_received is called with a concrete
%%   begin_offset for every partition (determined by offset_reset_policy), never
%%   undefined. The subscriber must not silently fall back to consumer config.
t_offset_fetch_minus_one_silently_falls_back_to_begin_offset({init, _Config}) ->
  {skip, "known bug: OffsetFetch -1 silently drops partition, begin_offset=undefined"};
t_offset_fetch_minus_one_silently_falls_back_to_begin_offset({'end', _Config}) ->
  ok;
t_offset_fetch_minus_one_silently_falls_back_to_begin_offset(Config) when is_list(Config) ->
  meck:expect(brod_utils, request_sync,
    fun(_Conn, #kpro_req{api = offset_fetch}, _Timeout) ->
        {ok, fake_offset_fetch_minus_one_body(?TOPIC, ?PARTITION)};
       (Conn, Req, Timeout) ->
        meck:passthrough([Conn, Req, Timeout])
    end),
  {ok, CoordinatorPid} =
    brod_group_coordinator:start_link(?CLIENT_ID, ?GROUP, [?TOPIC],
                                      _Config = [], ?MODULE, {self(), 1}),
  ?assert_receive({assignments_revoked, 1}, ok),
  CoordinatorPid ! continue,
  TopicAssignments = ?assert_receive({assignments_received, 1, _, TA}, TA),
  %% begin_offset = undefined means brod silently fell back to consumer config.
  %% This assertion FAILS today — fix should return {error, no_committed_offset}.
  SilentFallbacks =
    [A || #brod_received_assignment{begin_offset = undefined} = A <- TopicAssignments],
  ?assertEqual([], SilentFallbacks).

fake_offset_fetch_minus_one_body(Topic, Partition) ->
  %% committed_offset=-1 with error_code=no_error is the Kafka protocol signal
  %% for "no committed offset exists" (group is new, topic recreated, etc.).
  [ {error_code, ?no_error}
  , {topics, [ [ {name, Topic}
               , {partitions, [ [ {partition_index, Partition}
                                , {committed_offset, -1}
                                , {metadata, <<>>}
                                , {error_code, ?no_error}
                                ]
                              ]}
               ]
             ]}
  ].


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
