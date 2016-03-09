%%%
%%%   Copyright (c) 2016 Klarna AB
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
%%% Kafka consumer group membership controller
%%%
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_group_controller).
-behaviour(gen_server).

-export([ ack/5
        , commit_offsets/1
        , start_link/4
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-define(PARTITION_ASSIGMENT_STRATEGY_ROUNDROBIN, <<"roundrobin">>). %% default
-type partition_assignment_strategy() :: binary().

-define(SESSION_TIMEOUT_SECONDS, 10). %% default
-define(HEARTBEAT_RATE_SECONDS, 1). %% default heartbeat interval
-define(PROTOCOL_TYPE, <<"consumer">>).
-define(MAX_REJOIN_ATTEMPTS, 5).
-define(REJOIN_DELAY_SECONDS, 1).
-define(OFFSET_COMMIT_POLICY, timer:seconds(60)).

%% by default, start from latest available offset
-define(DEFAULT_BEGIN_OFFSET, -1).

-define(ESCALATE_EC(EC), kpro_ErrorCode:is_error(EC) andalso erlang:throw(EC)).

-define(ESCALATE(Expr), fun() ->
                          case Expr of
                            {ok, Result}    -> Result;
                            {error, Reason} -> throw(Reason)
                          end
                        end()).

%% loopback commands
-define(LO_CMD_SEND_HB, send_heartbeat).
-define(LO_CMD_COMMIT_OFFSETS, commit_offsets).
-define(LO_CMD_JOIN_GROUP(AttemptCount, Reason),
        {join_group, AttemptCount, Reason}).

-type config() :: group_config().
-type ts() :: erlang:timestamp().
-type member() :: kpro_GroupMemberMetadata().
-type offset_commit_policy() :: disabled
                              | {periodic_seconds, pos_integer()}.

-record(state,
        { client :: client()
        , groupId :: group_id()
          %% Group member ID, which should be set to empty in the first
          %% join group request, then a new member id is assigned by the
          %% group coordinator and in join group response.
          %% This filed may change if the member has lost connection
          %% to the coordinator and received 'UnknownMemberId' exception
          %% in response messages.
        , memberId = <<"">> :: member_id()
          %% State#state.memberId =:= State#state.leaderId if
          %% elected as group leader by the coordinator.
        , leaderId :: member_id()
          %% Generation ID is used by the group coordinator to sync state
          %% of the group members, e.g. kick out stale members who have not
          %% kept up with the latest generation ID bumps.
        , generationId = 0 :: integer()
          %% The initial group of topics from which the group members to
          %% should consume messages. Consider the fact that this is most
          %% likely statically configured in brod user's configs, it is
          %% important to have it identical across all group members.
        , topics = [] :: [topic()]
          %% This is the result of group coordinator discovery.
          %% It may change when the coordinator is down then a new one
          %% is elected among the kafka cluster members.
        , coordinator :: endpoint()
          %% The socket pid to the group coordinator broker.
          %% This socket is dedicated for group management and
          %% offset commit requests.
          %% We can not just get a payload socket from client
          %% because the socket might be shared with other group
          %% members in the same client, however group members are
          %% distinguished by connections to coordinator
        , sock_pid  :: pid()
          %% heartbeat reference, to discard stale responses
        , hb_ref :: {corr_id(), ts()}
          %% all group members received in the join group response
          %% this field is currently not used, but the binary encoded
          %% kpro_GroupMemberMetadata.protocolMetadata.userData field
          %% can be useful for 'sticky' assignments etc. in the future
        , members = [] :: [member()]
          %% Set to false before joining the group
          %% then set to true when sucessfully joined the group.
          %% This is by far only used to prevent the timer-triggered
          %% loopback command message sending a HeartbeatRequest to
          %% the group coordinator broker.
        , is_in_group = false :: boolean()
          %% The message-set subscriber which subscribes to all
          %% assigned topic-partitions.
        , subscriber :: pid()
          %% The offsets that has been acknowledged by the subscriber
          %% i.e. the offsets that are ready for commit.
          %% NOTE: this filed is not used if offset_commit_policy is 'disabled'
        , acked_offsets = [] :: [{{topic(), partition()}, offset()}]

          %% configs, see start_link/4 doc for details
        , partition_assignment_strategy :: partition_assignment_strategy()
        , session_timeout_seconds       :: pos_integer()
        , heartbeat_rate_seconds        :: pos_integer()
        , max_rejoin_attempts           :: non_neg_integer()
        , rejoin_delay_seconds          :: non_neg_integer()
        , offset_retention_seconds      :: ?undef | integer()
        , default_begin_offset          :: offset()
        , offset_commit_policy          :: offset_commit_policy()
        }).

-define(IS_LEADER(S), (S#state.leaderId =:= S#state.memberId)).

%%%_* APIs =====================================================================

%% @doc To be called by group subscriber.
%% TODO: add docs for configs
%% @end
-spec start_link(client(), group_id(), [topic()], config()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, Config) ->
  Subscriber = self(),
  Args = {Client, GroupId, Topics, Config, Subscriber},
  gen_server:start_link(?MODULE, Args, []).

%% @doc For group subscriver to call to acknowledge.
-spec ack(pid(), integer(), topic(), partition(), offset()) -> ok.
ack(Pid, GenerationId, Topic, Partition, Offset) ->
  Pid ! {ack, GenerationId, Topic, Partition, Offset},
  ok.

%% @doc Force commit offsets immediately.
-spec commit_offsets(pid()) -> ok | {error, any()}.
commit_offsets(ControllerPid) ->
  gen_server:call(ControllerPid, commit_offsets, infinity).

%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Topics, Config, Subscriber}) ->
  process_flag(trap_exit, true),
  GetCfg = fun(NameOrWithDefault) -> get_config(NameOrWithDefault, Config) end,
  PaStrategy = GetCfg({partition_assignment_strategy,
                       ?PARTITION_ASSIGMENT_STRATEGY_ROUNDROBIN}),
  SessionTimeoutSec =
    GetCfg({session_timeout_seconds, ?SESSION_TIMEOUT_SECONDS}),
  HbRateSec = GetCfg({heartbeat_rate_seconds, ?HEARTBEAT_RATE_SECONDS}),
  MaxRejoinAttempts = GetCfg({max_rejoin_attempts, ?MAX_REJOIN_ATTEMPTS}),
  RejoinDelaySeconds = GetCfg({rejoin_delay_seconds, ?REJOIN_DELAY_SECONDS}),
  OffsetRetentionSeconds = GetCfg({offset_retention_seconds, ?undef}),
  DefaultBeginOffset = GetCfg({default_begin_offset, ?DEFAULT_BEGIN_OFFSET}),
  OffsetCommitPolicy = GetCfg({offset_commit_policy, ?OFFSET_COMMIT_POLICY}),
  self() ! ?LO_CMD_JOIN_GROUP(0, ?undef),
  ok = start_heartbeat_timer(HbRateSec),
  ok = maybe_start_commit_offsets_timer(OffsetCommitPolicy),
  {ok, #state{ client                        = Client
             , groupId                       = GroupId
             , topics                        = Topics
             , subscriber                    = Subscriber
             , partition_assignment_strategy = PaStrategy
             , session_timeout_seconds       = SessionTimeoutSec
             , heartbeat_rate_seconds        = HbRateSec
             , max_rejoin_attempts           = MaxRejoinAttempts
             , rejoin_delay_seconds          = RejoinDelaySeconds
             , offset_retention_seconds      = OffsetRetentionSeconds
             , default_begin_offset          = DefaultBeginOffset
             , offset_commit_policy          = OffsetCommitPolicy
             }}.

handle_info({ack, GenerationId, Topic, Partition, Offset}, State) ->
  case GenerationId < State#state.generationId of
    true  ->
      %% Ignore stale acks
      {noreply, State};
    false ->
      {ok, NewState} = handle_ack(State, Topic, Partition, Offset),
      {noreply, NewState}
  end;
handle_info(?LO_CMD_COMMIT_OFFSETS, State) ->
  NewState =
    case do_commit_offsets(State) of
      {ok, State_} ->
        State_;
      {error, Reason} ->
        {ok, State_} = join_group(State, 0, Reason),
        State_
    end,
  ok = maybe_start_commit_offsets_timer(NewState),
  {noreply, NewState};
handle_info(?LO_CMD_JOIN_GROUP(N, _Reason),
            #state{max_rejoin_attempts = Max} = State) when N >= Max ->
  {stop, max_rejoin_attempts, State};
handle_info(?LO_CMD_JOIN_GROUP(N, Reason), State) ->
  {ok, NewState} = join_group(State, N, Reason),
  {noreply, NewState};
handle_info({'EXIT', Pid, Reason}, #state{sock_pid = Pid} = State) ->
  {ok, NewState} = join_group(State, 0, Reason),
  {noreply, NewState};
handle_info({'EXIT', Pid, _Reason}, #state{subscriber = Pid} = State) ->
  {stop, subscriber_down, State};
handle_info(?LO_CMD_SEND_HB,
            #state{ hb_ref                  = HbRef
                  , session_timeout_seconds = SessionTimeoutSec
                  } = State) ->
  _ = start_heartbeat_timer(State#state.heartbeat_rate_seconds),
  case HbRef of
    undefined ->
      {ok, NewState} = maybe_send_heartbeat(State),
      {noreply, NewState};
    {_HbCorrId, SentTime} ->
      Elapsed = timer:now_diff(os:timestamp(), SentTime),
      case  Elapsed < SessionTimeoutSec * 1000000 of
        true ->
          %% keep waiting for heartbeat response
          {noreply, State};
        false ->
          %% time to re-discover a new coordinator ?
          {ok, NewState} = join_group(State, 0, hb_timeout),
          {noreply, NewState}
      end
  end;
handle_info({msg, _Pid, HbCorrId, #kpro_HeartbeatResponse{errorCode = EC}},
            #state{hb_ref = {HbCorrId, _SentTime}} = State0) ->
  State = State0#state{hb_ref = ?undef},
  case kpro_ErrorCode:is_error(EC) of
    true ->
      {ok, NewState} = join_group(State, 0, EC),
      {noreply, NewState};
    false ->
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

handle_call(commit_offsets, _From,
            #state{offset_commit_policy = disabled} = State) ->
  {reply, {error, disabled}, State};
handle_call(commit_offsets, From, State) ->
  case  do_commit_offsets(State) of
    {ok, NewState} ->
      {reply, ok, NewState};
    {error, Reason} ->
      gen_server:reply(From, {error, Reason}),
      {ok, NewState} = join_group(State, 0, Reason),
      {noreply, NewState}
  end;
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(Reason, #state{ sock_pid = SockPid
                        , groupId  = GroupId
                        , memberId = MemberId
                        } = State) ->
  log(State, info_msg, "leaving group, reason ~p\n", [Reason]),
  Request = #kpro_LeaveGroupRequest
              { groupId = GroupId
              , memberId = MemberId
              },
  try send_sync(SockPid, Request, 1000)
  catch _ : _ -> ok
  end,
  _ = brod_sock:stop(SockPid),
  ok.

%%%_* Internal Functions =======================================================

-spec ensure_connection_to_coordinator(#state{}) -> #state{} | no_return().
ensure_connection_to_coordinator(#state{ client      = Client
                                       , coordinator = Coordinator
                                       , sock_pid    = SockPid
                                       , groupId     = GroupId
                                       } = State) ->
  {Host, Port} = ?ESCALATE(brod_client:get_group_coordinator(Client, GroupId)),
  HasConnectionToCoordinator =
    case Coordinator =:= {Host, Port} of
      true  -> is_pid(SockPid) andalso is_process_alive(SockPid);
      false -> false
    end,
  case HasConnectionToCoordinator of
    true ->
      State;
    false ->
      %% close old socket
      _ = brod_sock:stop(SockPid),
      NewSockPid =
        ?ESCALATE(brod_sock:start_link(self(), Host, Port, GroupId, [])),
      log(State, info_msg, "connected to group coordinator ~s:~p",
          [Host, Port]),
      State#state{ coordinator = {Host, Port}
                 , sock_pid    = NewSockPid
                 }
  end.

-spec join_group(#state{}, integer(), any()) -> {ok, #state{}}.
join_group(#state{ rejoin_delay_seconds = RejoinDelaySeconds
                 , subscriber           = Subscriber
                 , offset_commit_policy = CommitPolicy
                 } = State0, AttemptNo, Reason) ->

  %% log the first failure reason,
  %% retry-failures are logged right after the join_group exception below.
  AttemptNo =:= 0 andalso
    log(State0, info_msg, "re-joining group.\nreason:~p", [Reason]),

  %% 1. unsubscribe all currently assigned partitions
  ok = brod_group_subscriber:unsubscribe_all_partitions(Subscriber),

  %% 2. if it is illegal generation error code received, try to commit current
  %% current offsets before re-joinning the group.
  State1 =
    case AttemptNo     =:= 0                      andalso
          Reason       =:= ?EC_ILLEGAL_GENERATION andalso
          CommitPolicy =/= disabled of
      true ->
        {ok, State1_} = try_commit_offsets(State0),
        State1_;
      false ->
        State0
    end,

  %$ 3. Cleanup old state depending on the error codes.
  State2 =
    case Reason of
      ?EC_UNKNOWN_MEMBER_ID ->
        %% we are likely kicked out from the group
        %% rejoin with empty member id
        State1#state{memberId = <<>>};
      ?EC_NOT_COORDINATOR_FOR_GROUP ->
        %% the coordinator have moved to another broker
        %% set it to ?undef to trigger a socket restart
        _ = brod_sock:stop(State0#state.sock_pid),
        State1#state{sock_pid = ?undef};
      _ ->
        State1
    end,

  %% 4. ensure we have a connection to the (maybe new) group coordinator
  State = ensure_connection_to_coordinator(State2),

  %% 5. send join request, wait for response,
  %%    send sync group request, wait for response
  %%    tell subscriber to carryout new assignments
  try
    do_join_group(State)
  catch throw : NewReason ->
    log(State, error_msg, "failed to join group\nreason:~p\n~p",
        [NewReason, erlang:get_stacktrace()]),
    case AttemptNo =:= 0 of
      true ->
        %% do not delay before the first retry
        self() ! ?LO_CMD_JOIN_GROUP(AttemptNo + 1, NewReason);
      false ->
        erlang:send_after(timer:seconds(RejoinDelaySeconds), self(),
                          ?LO_CMD_JOIN_GROUP(AttemptNo + 1, NewReason))
    end,
    {ok, State#state{is_in_group = false}}
  end.

-spec do_join_group(#state{}) -> {ok, #state{}} | no_return().
do_join_group(#state{ groupId                       = GroupId
                    , memberId                      = MemberId0
                    , topics                        = Topics
                    , sock_pid                      = SockPid
                    , subscriber                    = Subscriber
                    , partition_assignment_strategy = PaStrategy
                    , session_timeout_seconds       = SessionTimeoutSec
                    } = State0) ->
  ConsumerGroupProtocolMeta =
    #kpro_ConsumerGroupProtocolMetadata
      { version     = ?BROD_CONSUMER_GROUP_PROTOCOL_VERSION
      , topicName_L = Topics
      , userData    = make_user_data()
      },
  ConsumerGroupProtocol =
    #kpro_GroupProtocol
      { protocolName     = PaStrategy
      , protocolMetadata = ConsumerGroupProtocolMeta
      },
  SessionTimeout = timer:seconds(SessionTimeoutSec),
  JoinReq =
    #kpro_JoinGroupRequest
      { groupId         = GroupId
      , sessionTimeout  = SessionTimeout
      , memberId        = MemberId0
      , protocolType    = ?PROTOCOL_TYPE
      , groupProtocol_L = [ConsumerGroupProtocol]
      },
  %% send join group request and wait for response
  %% as long as the session timeout config
  JoinRsp = send_sync(SockPid, JoinReq, SessionTimeout),
  ?ESCALATE_EC(JoinRsp#kpro_JoinGroupResponse.errorCode),
  #kpro_JoinGroupResponse
    { generationId          = GenerationId
    , protocolName          = PaStrategy
    , leaderId              = LeaderId
    , memberId              = MemberId
    , groupMemberMetadata_L = Members
    } = JoinRsp,
  IsGroupLeader = (LeaderId =:= MemberId),
  State =
    State0#state{ memberId     = MemberId
                , leaderId     = LeaderId
                , generationId = GenerationId
                , members      = Members
                , is_in_group  = true
                },
  SyncReq =
    #kpro_SyncGroupRequest
      { groupId           = GroupId
      , generationId      = GenerationId
      , memberId          = MemberId
      , groupAssignment_L = assign_partitions(State)
      },
  %% send sync group request and wait for response
  #kpro_SyncGroupResponse
    { errorCode        = SyncErrorCode
    , memberAssignment = Assignment
    } = send_sync(SockPid, SyncReq),
  ?ESCALATE_EC(SyncErrorCode),
  %% get my partition assignments
  TopicAssignments = get_topic_assignments(State, Assignment),
  log(State, info_msg, "elected=~p\nassignments:~p",
      [IsGroupLeader, TopicAssignments]),

  ok = brod_group_subscriber:new_assignments(Subscriber, GenerationId,
                                             TopicAssignments),

  {ok, State}.


-spec handle_ack(#state{}, topic(), partition(), offset()) -> {ok, #state{}}.
handle_ack(#state{ acked_offsets = AckedOffsets
                 } = State, Topic, Partition, Offset) ->
  AckedOffsets =
    lists:keystore({Topic, Partition}, 1, AckedOffsets,
                   {{Topic, Partition}, Offset}),
  {ok, State#state{acked_offsets = AckedOffsets}}.

%% @private Commit the current offsets before re-join the group.
%% NOTE: this is a 'best-effort' attempt, failing to commit offset
%%       at this stage should be fine, after all, the consumers will
%%       refresh their start point offsets when new assignment is
%%       received.
%% @end
-spec try_commit_offsets(#state{}) -> {ok, #state{}}.
try_commit_offsets(#state{} = State) ->
  try
    {ok, _} = do_commit_offsets(State)
  catch _ : _ ->
    {ok, State}
  end.

-spec do_commit_offsets(#state{}) -> {ok, #state{}}.
do_commit_offsets(#state{acked_offsets = []} = State) ->
  {ok, State};
do_commit_offsets(#state{ groupId                  = GroupId
                        , memberId                 = MemberId
                        , generationId             = GenerationId
                        , sock_pid                 = SockPid
                        , offset_retention_seconds = OffsetRetentionSecs
                        , acked_offsets            = AckedOffsets
                        } = State) ->
  TopicOffsets =
    lists:foldl(
      fun({{Topic, Partition}, Offset}, Acc) ->
        PartitionOffset =
          #kpro_OCReqV2Partition{ partition = Partition
                                , offset    = Offset
                                , metadata  = make_metadata()
                                },
        orddict:append_list(Topic, [PartitionOffset], Acc)
      end, [], AckedOffsets),
  Offsets =
    lists:map(
      fun({Topic, PartitionOffsets}) ->
        #kpro_OCReqV2Topic{ topicName          = Topic
                          , oCReqV2Partition_L = PartitionOffsets
                          }
      end, TopicOffsets),
  Req =
    #kpro_OffsetCommitRequestV2
      { consumerGroupId = GroupId
      , consumerGroupGenerationId = GenerationId
      , consumerId = MemberId
      , retentionTime = case OffsetRetentionSecs =/= ?undef of
                          true  -> timer:seconds(OffsetRetentionSecs);
                          false -> -1
                        end
      , oCReqV2Topic_L = Offsets
      },
  Rsp = send_sync(SockPid, Req),
  #kpro_OffsetCommitResponse{oCRspTopic_L = Topics} = Rsp,
  lists:foreach(
    fun(#kpro_OCRspTopic{topicName = Topic, oCRspPartition_L = Partitions}) ->
      lists:foreach(
        fun(#kpro_OCRspPartition{partition = Partition, errorCode = EC}) ->
          kpro_ErrorCode:is_error(EC) andalso
            begin
              log(State, error_msg,
                  "failed to commit offset for topic=~s, partition=~p\n"
                  "~p:~s", [Topic, Partition, EC, kpro_ErrorCode:desc(EC)]),
              erlang:throw(EC)
            end
        end, Partitions)
    end, Topics),
  {ok, State#state{acked_offsets = []}}.

-spec assign_partitions(#state{}) ->
        [kpro_GroupAssignment()] | no_return().
assign_partitions(State) when ?IS_LEADER(State) ->
  #state{ client                        = Client
        , topics                        = Topics
        , members                       = Members
        , partition_assignment_strategy = Strategy
        } = State,
  AllPartitions =
    [ {Topic, Partition}
      || Topic <- Topics,
         Partition <- get_partitions(Client, Topic)
    ],
  Assignments = do_assign_partitions(Strategy, Members, AllPartitions),
  lists:map(
    fun({MemberId, Topics_}) ->
      PartitionAssignments =
        lists:map(fun({Topic, Partitions}) ->
                    #kpro_ConsumerGroupPartitionAssignment
                      { topicName   = Topic
                      , partition_L = Partitions
                      }
                  end, Topics_),
      #kpro_GroupAssignment
        { memberId = MemberId
        , memberAssignment =
            #kpro_ConsumerGroupMemberAssignment
              { version = ?BROD_CONSUMER_GROUP_PROTOCOL_VERSION
              , consumerGroupPartitionAssignment_L = PartitionAssignments
              }
        }
    end, Assignments);
assign_partitions(#state{}) ->
  %% only leader can assign partitions to members
  [].

-spec get_partitions(client(), topic()) -> [partition()] | no_return().
get_partitions(Client, Topic) ->
  Count = ?ESCALATE(brod_client:get_partitions_count(Client, Topic)),
  lists:seq(0, Count-1).

-spec do_assign_partitions(partition_assignment_strategy(),
                           [kpro_GroupMemberMetadata()],
                           [{topic(), partition()}]) -> [member_assignment()].
do_assign_partitions(<<"roundrobin">>, Members, AllPartitions) ->
  %% round robin, we only care about the member id
  F = fun(#kpro_GroupMemberMetadata{memberId = MemberId}) -> MemberId end,
  MemberIds = lists:map(F, Members),
  roundrobin_assign_loop(AllPartitions, MemberIds).

-spec roundrobin_assign_loop([{topic(), partition()}], [member_id()]) ->
        [member_assignment()].
roundrobin_assign_loop([], Members) ->
  %% remove the ones that has no assignments
  lists:filter(fun(M) -> is_tuple(M) end, Members);
roundrobin_assign_loop([{Topic, Partition} | Rest], [Member0 | Members]) ->
  Member = assign_partition(Member0, Topic, Partition),
  roundrobin_assign_loop(Rest, Members ++ [Member]).

-spec assign_partition(Member, topic(), partition()) -> member_assignment()
        when Member :: member_id() | member_assignment().
assign_partition(MemberId, Topic, Partition) when is_binary(MemberId) ->
  assign_partition({MemberId, []}, Topic, Partition);
assign_partition({MemberId, Topics0}, Topic, Partition) ->
  Topics = orddict:append_list(Topic, [Partition], Topics0),
  {MemberId, Topics}.

%% @private Extract the partition assignemts from SyncGroupResponse
%% then fetch the committed offsets of each partition.
%% @end
-spec get_topic_assignments(#state{}, kpro_ConsumerGroupMemberAssignment()) ->
        [topic_assignment()].
get_topic_assignments(#state{}, <<>>) -> [];
get_topic_assignments(#state{ default_begin_offset = DefatultBeginOffset
                            } = State, Assignment) ->
  #kpro_ConsumerGroupMemberAssignment
    { version                            = _VersionIgnored
    , consumerGroupPartitionAssignment_L = PartitionAssignments
    } = Assignment,
  TopicPartitions0 =
    lists:map(
      fun(#kpro_ConsumerGroupPartitionAssignment{ topicName   = Topic
                                                , partition_L = Partitions
                                                }) ->
        [{Topic, Partition} || Partition <- Partitions]
      end, PartitionAssignments),
  TopicPartitions = lists:append(TopicPartitions0),
  CommittedOffsets = get_committed_offsets(State, TopicPartitions),
  resolve_begin_offsets(TopicPartitions, CommittedOffsets,
                        DefatultBeginOffset, orddict:from_list([])).

%% @private Fetch committed offsets from kafka,
%% or call the consumer callback to read committed offsets.
%% @end
-spec get_committed_offsets(#state{}, [{topic(), partition()}]) ->
        [{{topic(), partition()}, OffsetOrWithMetadata}] when
        OffsetOrWithMetadata :: offset()
                              | {offset(), binary(), error_code()}.
get_committed_offsets(#state{ offset_commit_policy = disabled
                            , subscriber           = Subscriber
                            }, TopicPartitions) ->
  brod_group_subscriber:get_committed_offsets(Subscriber, TopicPartitions);
get_committed_offsets(#state{ groupId            = GroupId
                            , sock_pid           = SockPid
                            }, TopicPartitions) ->
  GrouppedPartitions =
    lists:foldl(fun({T, P}, Dict) ->
                  orddict:append_list(T, [P], Dict)
                end, [], TopicPartitions),
  OffsetFetchRequestTopics =
    lists:map(
      fun({Topic, Partitions}) ->
        #kpro_OFReqTopic{ topicName   = Topic
                        , partition_L = Partitions
                        }
      end, GrouppedPartitions),
  OffsetFetchRequest =
    #kpro_OffsetFetchRequest
      { consumerGroup = GroupId
      , oFReqTopic_L  = OffsetFetchRequestTopics
      },
  Rsp = send_sync(SockPid, OffsetFetchRequest),
  #kpro_OffsetFetchResponse{topicOffset_L = TopicOffsets} = Rsp,
  CommittedOffsets0 =
    lists:map(
      fun(#kpro_TopicOffset{ topicName         = Topic
                           , partitionOffset_L = Partitions
                           }) ->
        lists:map(
          fun(#kpro_PartitionOffset{ partition = Partition
                                   , offset    = Offset
                                   , metadata  = Metadata
                                   , errorCode = EC
                                   }) ->
            {{Topic, Partition}, {Offset, Metadata, EC}}
          end, Partitions)
      end, TopicOffsets),
  lists:append(CommittedOffsets0).

-spec resolve_begin_offsets(
        TopicPartitions  :: [{topic(), partition()}],
        CommittedOffsets :: [{{topic(), partition()}, OffsetOrWithMetadata}],
        DefaultOffset    :: offset(),
        [topic_assignment()]) ->
          [topic_assignment()] when
            OffsetOrWithMetadata :: offset()
                                  | {offset(), binary(), error_code()}.
resolve_begin_offsets([], _, _, Acc) -> Acc;
resolve_begin_offsets([{Topic, Partition} | Rest], CommittedOffsets,
                      DefaultBeginOffset, Acc) ->
  {Offset, Metadata, EC} =
    case lists:keyfind({Topic, Partition}, 1, CommittedOffsets) of
      {_, Tuple} when is_tuple(Tuple)     ->
        Tuple;
      {_, Offset_} when is_integer(Offset_) ->
        {Offset_, <<>>, ?EC_NONE};
      false  ->
        {?undef, <<>>, ?EC_UNKNOWN_TOPIC_OR_PARTITION}
    end,
  PartitionAssignment =
    case EC =:= ?EC_UNKNOWN_TOPIC_OR_PARTITION of
      true ->
        %% use default begin offset if not found in committed offsets
        #partition_assignment{ partition    = Partition
                             , begin_offset = DefaultBeginOffset
                             , metadata     = <<>>
                             };
      false ->
        ?ESCALATE_EC(EC),
        true = (Offset >= 0), %% assert
        #partition_assignment{ partition    = Partition
                             , begin_offset = Offset + 1
                             , metadata     = Metadata
                             }
    end,
  NewAcc = orddict:append_list(Topic, [PartitionAssignment], Acc),
  resolve_begin_offsets(Rest, CommittedOffsets, DefaultBeginOffset, NewAcc).

get_config({Name, Default}, Configs) ->
  proplists:get_value(Name, Configs, Default);
get_config(Name, Configs) ->
  case proplists:is_defined(Name, Configs) of
    true  -> proplists:get_value(Name, Configs);
    false -> erlang:error({bad_config, Name})
  end.

%% @private Start a timer to send a loopback command to self() to trigger
%% a heartbeat request to the group coordinator.
%% NOTE: the heartbeat requests are sent only when it is in group,
%%       but the timer is always restarted after expiration.
%% @end
-spec start_heartbeat_timer(pos_integer()) -> ok.
start_heartbeat_timer(HbRateSec) ->
  erlang:send_after(timer:seconds(HbRateSec), self(), ?LO_CMD_SEND_HB),
  ok.

%% @private Start a timer to send a loopback command to self() to trigger
%% a offset commit rquest to group coordinator.
%% @end
-spec maybe_start_commit_offsets_timer(#state{} | offset_commit_policy()) -> ok.
maybe_start_commit_offsets_timer(disabled) -> ok;
maybe_start_commit_offsets_timer(#state{offset_commit_policy = P}) ->
  maybe_start_commit_offsets_timer(P);
maybe_start_commit_offsets_timer({periodic_seconds, Seconds}) ->
  erlang:send_after(timer:seconds(Seconds), self(), ?LO_CMD_COMMIT_OFFSETS),
  ok.

%% @private Send heartbeat request if it has joined the group.
-spec maybe_send_heartbeat(#state{}) -> {ok, #state{}}.
maybe_send_heartbeat(#state{ is_in_group  = true
                           , groupId      = GroupId
                           , memberId     = MemberId
                           , generationId = GenerationId
                           , sock_pid     = SockPid
                           } = State) ->
  Request = #kpro_HeartbeatRequest{ groupId = GroupId
                                  , memberId = MemberId
                                  , generationId = GenerationId
                                  },
  CorrId = ?ESCALATE(brod_sock:send(SockPid, Request)),
  NewState = State#state{hb_ref = {CorrId, os:timestamp()}},
  {ok, NewState};
maybe_send_heartbeat(#state{} = State) ->
  %% do not send heartbeat when not in group
  {ok, State#state{hb_ref = ?undef}}.

send_sync(SockPid, Request) ->
  send_sync(SockPid, Request, 5000).

send_sync(SockPid, Request, Timeout) ->
  ?ESCALATE(brod_sock:send_sync(SockPid, Request, Timeout)).

make_user_data() ->
  iolist_to_binary(io_lib:format("~p ~p", [node(), self()])).

log(#state{ groupId  = GroupId
          , memberId = MemberId
          , generationId = GenerationId
          }, LevelFun, Fmt, Args) ->
  error_logger:LevelFun(
    "groupId=~s memberId=~s generation=~p pid=~p:\n" ++ Fmt,
    [GroupId, MemberId, GenerationId, self() | Args]).

make_metadata() ->
  io_lib:format("~s ~p ~p", [brod_utils:os_time_utc_str(), node(), self()]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
