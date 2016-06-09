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
%%% Kafka consumer group membership coordinator
%%%
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_group_coordinator).

-behaviour(gen_server).

-export([ ack/5
        , commit_offsets/1
        , commit_offsets/2
        , start_link/6
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-define(PARTITION_ASSIGMENT_STRATEGY_ROUNDROBIN, roundrobin). %% default

-type partition_assignment_strategy() :: brod_partition_assignment_strategy().

%% default configs
-define(SESSION_TIMEOUT_SECONDS, 10).
-define(HEARTBEAT_RATE_SECONDS, 2).
-define(PROTOCOL_TYPE, <<"consumer">>).
-define(MAX_REJOIN_ATTEMPTS, 5).
-define(REJOIN_DELAY_SECONDS, 1).
-define(OFFSET_COMMIT_POLICY, commit_to_kafka_v2).
-define(OFFSET_COMMIT_INTERVAL_SECONDS, 5).
%% use kfaka's offset meta-topic retention policy
-define(OFFSET_RETENTION_DEFAULT, -1).

-define(ESCALATE_EC(EC), kpro_ErrorCode:is_error(EC) andalso erlang:throw(EC)).

-define(ESCALATE(Expr), fun() ->
                          case Expr of
                            {ok, Result}    -> Result;
                            {error, Reason} -> throw(Reason)
                          end
                        end()).

%% loopback commands
-define(LO_CMD_SEND_HB, lo_cmd_send_heartbeat).
-define(LO_CMD_COMMIT_OFFSETS, lo_cmd_commit_offsets).
-define(LO_CMD_STABILIZE(AttemptCount, Reason),
        {lo_cmd_stabilize, AttemptCount, Reason}).

-type config() :: group_config().
-type ts() :: erlang:timestamp().
-type member() :: kafka_group_member().
-type offset_commit_policy() :: brod_offset_commit_policy().

-record(state,
        { client :: client()
        , groupId :: group_id()
          %% Group member ID, which should be set to empty in the first
          %% join group request, then a new member id is assigned by the
          %% group coordinator and in join group response.
          %% This field may change if the member has lost connection
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
          %% A set of topic names where the group members consumes from
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
          %% The process which is responsible to subscribe/unsubscribe to all
          %% assigned topic-partitions.
        , member_pid :: pid()
          %% The module which implements group member functionality
        , member_module :: module()
          %% The offsets that has been acknowledged by the member
          %% i.e. the offsets that are ready for commit.
          %% NOTE: this field is not used if offset_commit_policy is
          %% 'consumer_managed'
        , acked_offsets = [] :: [{{topic(), partition()}, offset()}]
          %% The referece of the timer which triggers offset commit
        , offset_commit_timer :: reference()

          %% configs, see start_link/5 doc for details
        , partition_assignment_strategy  :: partition_assignment_strategy()
        , session_timeout_seconds        :: pos_integer()
        , heartbeat_rate_seconds         :: pos_integer()
        , max_rejoin_attempts            :: non_neg_integer()
        , rejoin_delay_seconds           :: non_neg_integer()
        , offset_retention_seconds       :: ?undef | integer()
        , offset_commit_policy           :: offset_commit_policy()
        , offset_commit_interval_seconds :: pos_integer()
        }).

-define(IS_LEADER(S), (S#state.leaderId =:= S#state.memberId)).

%%%_* APIs =====================================================================

%% @doc Start a kafka consumer group coordinator.
%% Client:    ClientId (or pid, but not recommended)
%% GroupId:   Predefined globally unique (in a kafka cluster) binary string.
%% Topics:    Predefined set of topic names to join the group.
%% CbModule:  The module which implements group coordinator callbacks
%% MemberPid: The member process pid.
%% Config: The group coordinator configs in a proplist, possible entries:
%%  - partition_assignment_strategy  (optional, default = roundrobin)
%%      roundrobin (topic-sticky):
%%        Take all topic-offset (sorted [{TopicName, Partition}] list)
%%        assign one to each member in a roundrobin fashion. However only
%%        partitions in the subscription topic list are assiged.
%%      callback_implemented
%%        Call CbModule:assign_partitions/2 to assign partitions.
%%  - session_timeout_seconds (optional, default = 10)
%%      Time in seconds for the group coordinator broker to consider a member
%%      'down' if no heartbeat or any kind of requests received from a broker
%%      in the past N seconds.
%%      A group member may also consider the coordinator broker 'down' if no
%%      heartbeat response response received in the past N seconds.
%%  - heartbeat_rate_seconds (optional, default = 2)
%%      Time in seconds for the member to 'ping' the group coordinator.
%%      OBS: Care should be taken when picking the number, on one hand, we do
%%           not want to flush the broker with requests if we set it too low,
%%           on the other hand, if set it too high, it may take too long for
%%           the members to realise status changes of the group such as
%%           assignment rebalacing or group coordinator switchover etc.
%%  - max_rejoin_attempts (optional, default = 5)
%%      Maximum number of times allowd for a member to re-join the group.
%%      The gen_server will stop if it reached the maximum number of retries.
%%      OBS: 'let it crash' may not be the optimal strategy here because
%%           the group member id is kept in the gen_server looping state and
%%           it is reused when re-joining the group.
%%  - rejoin_delay_seconds (optional, default = 1)
%%      Delay in seconds before re-joining the group.
%%  - offset_commit_policy (optional, default = commit_to_kafka_v2)
%%      How/where to commit offsets, possible values:
%%        - commit_to_kafka_v2:
%%            Group coordinator will commit the offsets to kafka using
%%            version 2 OffsetCommitRequest.
%%        - consumer_managed:
%%            The group member (e.g. brod_group_subscriber.erl) is responsible
%%            for persisting offsets to a local or centralized storage.
%%            And the callback get_committed_offsets should be implemented
%%            to allow group coordinator to retrieve the commited offsets.
%%  - offset_commit_interval_seconds (optional, default = 5)
%%      The time interval between two OffsetCommitRequest messages.
%%      This config is irrelevant if offset_commit_policy is consumer_managed.
%%  - offset_retention_seconds (optional, default = -1)
%%      How long the time is to be kept in kafka before it is deleted.
%%      The default special value -1 indicates that the __consumer_offsets
%%      topic retention policy is used.
%%      This config is irrelevant if offset_commit_policy is consumer_managed.
%% @end
-spec start_link(client(), group_id(), [topic()], config(), module(), pid()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, Config, CbModule, MemberPid) ->
  Args = {Client, GroupId, Topics, Config, CbModule, MemberPid},
  gen_server:start_link(?MODULE, Args, []).

%% @doc For group member to call to acknowledge a consumed message offset.
-spec ack(pid(), integer(), topic(), partition(), offset()) -> ok.
ack(Pid, GenerationId, Topic, Partition, Offset) ->
  Pid ! {ack, GenerationId, Topic, Partition, Offset},
  ok.

%% @doc Force commit collected (acked) offsets immediately.
-spec commit_offsets(pid()) -> ok | {error, any()}.
commit_offsets(CoordinatorPid) ->
  commit_offsets(CoordinatorPid, _Offsets = []).

%% @doc Force commit collected (acked) offsets plus the given extra offsets
%% immediately.
%% NOTE: A lists:usrot is applied on the given extra offsets to commit
%%       meaning if two or more offsets for the same topic-partition exist
%%       in the list, only the one that is closer the head of the list is kept
%% @end
-spec commit_offsets(pid(), [{{topic(), partition()}, offset()}]) ->
        ok | {error, any()}.
commit_offsets(CoordinatorPid, Offsets0) ->
  %% OBS: do not use 'infinity' timeout here.
  %% There is a risk of getting into a dead-lock state e.g. when
  %% coordinator process is making a gen_server:call to the group member
  %% (this call might be implemented in the brod_group_member callbacks)
  Offsets = lists:ukeysort(1, Offsets0),
  try
    gen_server:call(CoordinatorPid, {commit_offsets, Offsets}, 5000)
  catch
    exit : {timeout, _} ->
      {error, timeout}
  end.


%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Topics, Config, CbModule, MemberPid}) ->
  erlang:process_flag(trap_exit, true),
  GetCfg = fun(Name, Default) ->
             proplists:get_value(Name, Config, Default)
           end,
  PaStrategy = GetCfg(partition_assignment_strategy,
                      ?PARTITION_ASSIGMENT_STRATEGY_ROUNDROBIN),
  SessionTimeoutSec = GetCfg(session_timeout_seconds, ?SESSION_TIMEOUT_SECONDS),
  HbRateSec = GetCfg(heartbeat_rate_seconds, ?HEARTBEAT_RATE_SECONDS),
  MaxRejoinAttempts = GetCfg(max_rejoin_attempts, ?MAX_REJOIN_ATTEMPTS),
  RejoinDelaySeconds = GetCfg(rejoin_delay_seconds, ?REJOIN_DELAY_SECONDS),
  OffsetRetentionSeconds = GetCfg(offset_retention_seconds, ?undef),
  OffsetCommitPolicy = GetCfg(offset_commit_policy, ?OFFSET_COMMIT_POLICY),
  OffsetCommitIntervalSeconds = GetCfg(offset_commit_interval_seconds,
                                       ?OFFSET_COMMIT_INTERVAL_SECONDS),
  self() ! ?LO_CMD_STABILIZE(0, ?undef),
  ok = start_heartbeat_timer(HbRateSec),
  State =
    #state{ client                         = Client
          , groupId                        = GroupId
          , topics                         = Topics
          , member_pid                     = MemberPid
          , member_module                  = CbModule
          , partition_assignment_strategy  = PaStrategy
          , session_timeout_seconds        = SessionTimeoutSec
          , heartbeat_rate_seconds         = HbRateSec
          , max_rejoin_attempts            = MaxRejoinAttempts
          , rejoin_delay_seconds           = RejoinDelaySeconds
          , offset_retention_seconds       = OffsetRetentionSeconds
          , offset_commit_policy           = OffsetCommitPolicy
          , offset_commit_interval_seconds = OffsetCommitIntervalSeconds
          },
  {ok, State}.

handle_info({ack, GenerationId, Topic, Partition, Offset}, State) ->
  case GenerationId < State#state.generationId of
    true  ->
      %% Ignore stale acks
      {noreply, State};
    false ->
      {ok, NewState} = handle_ack(State, Topic, Partition, Offset),
      {noreply, NewState}
  end;
handle_info(?LO_CMD_COMMIT_OFFSETS, #state{is_in_group = true} = State) ->
  {ok, NewState} =
    try
      do_commit_offsets(State)
    catch throw : Reason ->
      stabilize(State, 0, Reason)
    end,
  {noreply, NewState};
handle_info(?LO_CMD_STABILIZE(N, _Reason),
            #state{max_rejoin_attempts = Max} = State) when N >= Max ->
  {stop, max_rejoin_attempts, State};
handle_info(?LO_CMD_STABILIZE(N, Reason), State) ->

  {ok, NewState} = stabilize(State, N, Reason),
  {noreply, NewState};
handle_info({'EXIT', Pid, Reason}, #state{sock_pid = Pid} = State) ->
  {ok, NewState} = stabilize(State, 0, {sockent_down, Reason}),
  {noreply, NewState};
handle_info({'EXIT', Pid, Reason}, #state{member_pid = Pid} = State) ->
  case Reason of
    shutdown      -> {stop, shutdown, State};
    {shutdown, _} -> {stop, shutdown, State};
    normal        -> {stop, normal, State};
    _             -> {stop, member_down, State}
  end;
handle_info(?LO_CMD_SEND_HB,
            #state{ hb_ref                  = HbRef
                  , session_timeout_seconds = SessionTimeoutSec
                  } = State) ->
  _ = start_heartbeat_timer(State#state.heartbeat_rate_seconds),
  case HbRef of
    ?undef ->
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
          {ok, NewState} = stabilize(State, 0, hb_timeout),
          {noreply, NewState}
      end
  end;
handle_info({msg, _Pid, HbCorrId, #kpro_HeartbeatResponse{errorCode = EC}},
            #state{hb_ref = {HbCorrId, _SentTime}} = State0) ->
  State = State0#state{hb_ref = ?undef},
  case kpro_ErrorCode:is_error(EC) of
    true ->
      {ok, NewState} = stabilize(State, 0, EC),
      {noreply, NewState};
    false ->
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

handle_call({commit_offsets, ExtraOffsets}, From, State) ->
  try
    Offsets = merge_acked_offsets(State#state.acked_offsets, ExtraOffsets),
    {ok, NewState} = do_commit_offsets(State#state{acked_offsets = Offsets}),
    {reply, ok, NewState}
  catch throw : Reason ->
    gen_server:reply(From, {error, Reason}),
    {ok, NewState_} = stabilize(State, 0, Reason),
    {noreply, NewState_}
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
  log(State, info, "leaving group, reason ~p\n", [Reason]),
  Request = #kpro_LeaveGroupRequest
              { groupId = GroupId
              , memberId = MemberId
              },
  try send_sync(SockPid, Request, 1000)
  catch _ : _ -> ok
  end,
  ok = stop_socket(SockPid).

%%%_* Internal Functions =======================================================

-spec discover_coordinator(#state{}) -> {ok, #state{}}.
discover_coordinator(#state{ client      = Client
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
      {ok, State};
    false ->
      %% close old socket
      _ = brod_sock:stop(SockPid),
      ClientId = make_group_connection_client_id(),
      NewSockPid =
        ?ESCALATE(brod_sock:start_link(self(), Host, Port, ClientId, [])),
      log(State, info, "connected to group coordinator ~s:~p",
          [Host, Port]),
      NewState =
        State#state{ coordinator = {Host, Port}
                   , sock_pid    = NewSockPid
                   },
      {ok, NewState}
  end.

-spec stabilize(#state{}, integer(), any()) -> {ok, #state{}}.
stabilize(#state{ rejoin_delay_seconds = RejoinDelaySeconds
                , member_module        = MemberModule
                , member_pid           = MemberPid
                , offset_commit_timer  = Timer
                } = State0, AttemptNo, Reason) ->
  is_reference(Timer) andalso erlang:cancel_timer(Timer),
  Reason =/= ?undef andalso
    log(State0, info, "re-joining group, reason:~p", [Reason]),

  %% 1. unsubscribe all currently assigned partitions
  ok = MemberModule:assignments_revoked(MemberPid),

  %% 2. try to commit current offsets before re-joinning the group.
  %%    try only on the first re-join attempt
  %%    do not try if it was illegal generation exception received
  %%    because it will fail on the same exception again
  State1 =
    case AttemptNo =:= 0 andalso
         Reason    =/= ?EC_ILLEGAL_GENERATION of
      true ->
        {ok, #state{} = State1_} = try_commit_offsets(State0),
        State1_;
      false ->
        State0
    end,
  State2 = State1#state{is_in_group = false},

  %$ 3. Clean up state based on the last failure reason
  State3 = maybe_reset_member_id(State2, Reason),
  State  = maybe_reset_socket(State3, Reason),

  %% 4. ensure we have a connection to the (maybe new) group coordinator
  F1 = fun discover_coordinator/1,
  %% 5. join group
  F2 = fun join_group/1,
  %% 6. sync assignemnts
  F3 = fun sync_group/1,

  RetryFun =
    fun(StateIn, NewReason) ->
      log(StateIn, info, "failed to join group\nreason:~p", [NewReason]),
      _ = case AttemptNo =:= 0 of
        true ->
          %% do not delay before the first retry
          self() ! ?LO_CMD_STABILIZE(AttemptNo + 1, NewReason);
        false ->
          erlang:send_after(timer:seconds(RejoinDelaySeconds), self(),
                            ?LO_CMD_STABILIZE(AttemptNo + 1, NewReason))
      end,
      {ok, StateIn}
    end,
  do_stabilize([F1, F2, F3], RetryFun, State).

do_stabilize([], _RetryFun, State) ->
  {ok, State};
do_stabilize([F | Rest], RetryFun, State) ->
  try
    {ok, #state{} = NewState} = F(State),
    do_stabilize(Rest, RetryFun, NewState)
  catch throw : Reason ->
    RetryFun(State, Reason)
  end.

maybe_reset_member_id(State, Reason) ->
  case should_reset_member_id(Reason) of
    true  -> State#state{memberId = <<>>};
    false -> State
  end.

should_reset_member_id(?EC_UNKNOWN_MEMBER_ID) ->
  %% we are likely kicked out from the group
  %% rejoin with empty member id
  true;
should_reset_member_id(?EC_NOT_COORDINATOR_FOR_GROUP) ->
  %% the coordinator have moved to another broker
  %% set it to ?undef to trigger a socket restart
  true;
should_reset_member_id({socket_down, _Reason}) ->
  %% old socket was down, new connection will lead
  %% to a new member id
  true;
should_reset_member_id(_) ->
  false.

maybe_reset_socket(State, ?EC_NOT_COORDINATOR_FOR_GROUP) ->
  ok = stop_socket(State#state.sock_pid),
  State#state{sock_pid = ?undef};
maybe_reset_socket(State, _OtherReason) ->
  State.

stop_socket(SockPid) ->
  catch unlink(SockPid),
  ok = brod_sock:stop(SockPid).

-spec join_group(#state{}) -> {ok, #state{}}.
join_group(#state{ groupId                       = GroupId
                 , memberId                      = MemberId0
                 , topics                        = Topics
                 , sock_pid                      = SockPid
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
      { protocolName     = atom_to_list(PaStrategy)
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
    , protocolName          = _PaStrategyBinStr
    , leaderId              = LeaderId
    , memberId              = MemberId
    , groupMemberMetadata_L = Members
    } = JoinRsp,
  IsGroupLeader = (LeaderId =:= MemberId),
  State =
    State0#state{ memberId     = MemberId
                , leaderId     = LeaderId
                , generationId = GenerationId
                , members      = translate_members(Members)
                },
  log(State, info, "elected=~p", [IsGroupLeader]),
  {ok, State}.

-spec sync_group(#state{}) -> {ok, #state{}}.
sync_group(#state{ groupId       = GroupId
                 , generationId  = GenerationId
                 , memberId      = MemberId
                 , sock_pid      = SockPid
                 , member_pid    = MemberPid
                 , member_module = MemberModule
                 } = State) ->
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
  ok = MemberModule:assignments_received(MemberPid, MemberId,
                                         GenerationId, TopicAssignments),
  NewState = State#state{is_in_group = true},
  log(NewState, info, "assignments received:~s",
      [format_assignments(TopicAssignments)]),
  start_offset_commit_timer(NewState).

-spec handle_ack(#state{}, topic(), partition(), offset()) -> {ok, #state{}}.
handle_ack(#state{ acked_offsets = AckedOffsets
                 } = State, Topic, Partition, Offset) ->
  NewAckedOffsets =
    merge_acked_offsets(AckedOffsets, [{{Topic, Partition}, Offset}]),
  {ok, State#state{acked_offsets = NewAckedOffsets}}.

%% @private Add new offsets to be acked into the acked offsets collection.
-spec merge_acked_offsets(Offsets, Offsets) -> Offsets when
        Offsets :: [{{topic(), partition()}, offset()}].
merge_acked_offsets(AckedOffsets, OffsetsToAck) ->
  lists:ukeymerge(1, OffsetsToAck, AckedOffsets).

-spec format_assignments(brod_received_assignments()) -> iodata().
format_assignments(Assignments) ->
  Groupped =
    lists:foldl(
      fun(#brod_received_assignment{ topic        = Topic
                                   , partition    = Partition
                                   , begin_offset = Offset
                                   }, Acc) ->
        orddict:append_list(Topic, [{Partition, Offset}], Acc)
      end, [], Assignments),
  lists:map(
    fun({Topic, Partitions}) ->
      ["\n", Topic, ":", format_partition_assignments(Partitions) ]
    end, Groupped).

-spec format_partition_assignments([{partition(), offset()}]) -> iodata().
format_partition_assignments([]) -> [];
format_partition_assignments([{Partition, BeginOffset} | Rest]) ->
  [ io_lib:format("~n    partition=~p begin_offset=~p",
                  [Partition, BeginOffset])
  , format_partition_assignments(Rest)
  ].

%% @private Commit the current offsets before re-join the group.
%% NOTE: this is a 'best-effort' attempt, failing to commit offset
%%       at this stage should be fine, after all, the consumers will
%%       refresh their start point offsets when new assignment is
%%       received.
%% @end
-spec try_commit_offsets(#state{}) -> {ok, #state{}}.
try_commit_offsets(#state{} = State) ->
  try
    {ok, #state{}} = do_commit_offsets(State)
  catch _ : _ ->
    {ok, State}
  end.

%% @private Commit collected offsets, stop old commit timer, start new timer.
-spec do_commit_offsets(#state{}) -> {ok, #state{}}.
do_commit_offsets(State) ->
  {ok, NewState} = do_commit_offsets_(State),
  start_offset_commit_timer(NewState).

-spec do_commit_offsets_(#state{}) -> {ok, #state{}}.
do_commit_offsets_(#state{acked_offsets = []} = State) ->
  {ok, State};
do_commit_offsets_(#state{offset_commit_policy = consumer_managed} = State) ->
  {ok, State};
do_commit_offsets_(#state{ groupId                  = GroupId
                         , memberId                 = MemberId
                         , generationId             = GenerationId
                         , sock_pid                 = SockPid
                         , offset_retention_seconds = OffsetRetentionSecs
                         , acked_offsets            = AckedOffsets
                         } = State) ->
  Metadata = make_offset_commit_metadata(),
  TopicOffsets =
    lists:foldl(
      fun({{Topic, Partition}, Offset}, Acc) ->
        PartitionOffset =
          #kpro_OCReqV2Partition{ partition = Partition
                                , offset    = Offset
                                , metadata  = Metadata
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
                          false -> ?OFFSET_RETENTION_DEFAULT
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
              log(State, error,
                  "failed to commit offset for topic=~s, partition=~p\n"
                  "~p:~s", [Topic, Partition, EC, kpro_ErrorCode:desc(EC)]),
              erlang:error(EC)
            end
        end, Partitions)
    end, Topics),
  NewState = State#state{acked_offsets = []},
  {ok, NewState}.

-spec assign_partitions(#state{}) -> [kpro_GroupAssignment()].
assign_partitions(State) when ?IS_LEADER(State) ->
  #state{ client                        = Client
        , members                       = Members
        , partition_assignment_strategy = Strategy
        , member_pid                    = MemberPid
        , member_module                 = MemberModule
        } = State,
  AllTopics = all_topics(Members),
  AllPartitions =
    [ {Topic, Partition}
      || Topic <- AllTopics,
         Partition <- get_partitions(Client, Topic)
    ],
  Assignments =
    case Strategy =:= callback_implemented of
      true  ->
        MemberModule:assign_partitions(MemberPid, Members, AllPartitions);
      false ->
        do_assign_partitions(Strategy, Members, AllPartitions)
    end,
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
              , userData = <<0>> %% null is not allowed before 0.9.0.1
              }
        }
    end, Assignments);
assign_partitions(#state{}) ->
  %% only leader can assign partitions to members
  [].

-spec translate_members([kpro_GroupMemberMetadata()]) -> [member()].
translate_members(Members) ->
  lists:map(
    fun(#kpro_GroupMemberMetadata{ memberId         = MemberId
                                 , protocolMetadata = Meta
                                 }) ->
      #kpro_ConsumerGroupProtocolMetadata
        { version     = Version
        , topicName_L = Topics
        , userData    = UserData
        } = Meta,
      {MemberId, #kafka_group_member_metadata{ version   = Version
                                             , topics    = Topics
                                             , user_data = UserData
                                             }}
    end, Members).

%% collect topics from all members
-spec all_topics([member()]) -> [topic()].
all_topics(Members) ->
  lists:usort(
    lists:append(
      lists:map(
        fun({_MemberId, M}) ->
          M#kafka_group_member_metadata.topics
        end, Members))).

-spec get_partitions(client(), topic()) -> [partition()].
get_partitions(Client, Topic) ->
  Count = ?ESCALATE(brod_client:get_partitions_count(Client, Topic)),
  lists:seq(0, Count-1).

-spec do_assign_partitions(roundrobin, [member()],
                           [{topic(), partition()}]) ->
                              [{member_id(), [brod_partition_assignment()]}].
do_assign_partitions(roundrobin, Members, AllPartitions) ->
  F = fun({MemberId, M}) ->
        SubscribedTopics = M#kafka_group_member_metadata.topics,
        IsValidAssignment = fun(Topic, _Partition) ->
                              lists:member(Topic, SubscribedTopics)
                            end,
        {MemberId, IsValidAssignment, []}
      end,
  MemberAssignment = lists:map(F, Members),
  [ {MemberId, Assignments}
    || {MemberId, _ValidationFun, Assignments}
         <- roundrobin_assign_loop(AllPartitions, MemberAssignment, [])
  ].

roundrobin_assign_loop([], PendingMembers, AssignedMembers) ->
  lists:reverse(AssignedMembers) ++ PendingMembers;
roundrobin_assign_loop(Partitions, [], AssignedMembers) ->
  %% all members have received assignments, continue the next round
  roundrobin_assign_loop(Partitions, lists:reverse(AssignedMembers), []);
roundrobin_assign_loop([{Topic, Partition} | Rest] = TopicPartitions,
                       [{MemberId, IsValidAssignment, AssignedTopics} = Member0
                        | PendingMembers], AssignedMembers) ->
  case IsValidAssignment(Topic, Partition) of
    true ->
      NewTopics = orddict:append_list(Topic, [Partition], AssignedTopics),
      Member = {MemberId, IsValidAssignment, NewTopics},
      roundrobin_assign_loop(Rest, PendingMembers, [Member | AssignedMembers]);
    false ->
      %% The fist member in the pending list is not interested in this
      %% topic-partition,
      roundrobin_assign_loop(TopicPartitions, PendingMembers,
                             [Member0 | AssignedMembers])
  end.

%% @private Extract the partition assignemts from SyncGroupResponse
%% then fetch the committed offsets of each partition.
%% @end
-spec get_topic_assignments(#state{}, kpro_ConsumerGroupMemberAssignment()) ->
        brod_received_assignments().
get_topic_assignments(#state{}, <<>>) -> [];
get_topic_assignments(#state{} = State, Assignment) ->
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
  resolve_begin_offsets(TopicPartitions, CommittedOffsets).

%% @private Fetch committed offsets from kafka,
%% or call the consumer callback to read committed offsets.
%% @end
-spec get_committed_offsets(#state{}, [{topic(), partition()}]) ->
        [{{topic(), partition()}, offset()}].
get_committed_offsets(#state{ offset_commit_policy = consumer_managed
                            , member_pid           = MemberPid
                            , member_module        = MemberModule
                            }, TopicPartitions) ->
  MemberModule:get_committed_offsets(MemberPid, TopicPartitions);
get_committed_offsets(#state{ offset_commit_policy = commit_to_kafka_v2
                            , groupId              = GroupId
                            , sock_pid             = SockPid
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
        lists:foldl(
          fun(#kpro_PartitionOffset{ partition = Partition
                                   , offset    = Offset
                                   , errorCode = EC
                                   }, Acc) ->
            case EC =:= ?EC_UNKNOWN_TOPIC_OR_PARTITION of
              true ->
                %% OffsetFetchResponse v0 if no commit history found
                Acc;
              false ->
                case EC =:= ?EC_NONE andalso Offset =:= -1 of
                  true ->
                    %% OffsetFetchResponse v1 if no commit history found
                    Acc;
                  false ->
                    ?ESCALATE_EC(EC),
                    [{{Topic, Partition}, Offset} | Acc]
                end
            end
          end, [], Partitions)
      end, TopicOffsets),
  lists:append(CommittedOffsets0).

-spec resolve_begin_offsets(
        TopicPartitions  :: [{topic(), partition()}],
        CommittedOffsets :: [{{topic(), partition()}, offset()}]) ->
            brod_received_assignments().
resolve_begin_offsets([], _) -> [];
resolve_begin_offsets([{Topic, Partition} | Rest], CommittedOffsets) ->
  Offset =
    case lists:keyfind({Topic, Partition}, 1, CommittedOffsets) of
      {_, Offset_} when is_integer(Offset_) ->
        Offset_;
      false  ->
        %% No commit history found
        ?undef
    end,
  BeginOffset = case is_integer(Offset) andalso Offset > 0 of
                  true  -> Offset + 1;
                  false -> Offset
                end,
  Assignment =
    #brod_received_assignment{ topic        = Topic
                             , partition    = Partition
                             , begin_offset = BeginOffset
                             },
  [Assignment | resolve_begin_offsets(Rest, CommittedOffsets)].

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
%% a offset commit request to group coordinator broker.
%% @end
-spec start_offset_commit_timer(#state{}) -> {ok, #state{}}.
start_offset_commit_timer(#state{offset_commit_timer = OldTimer} = State) ->
  #state{ offset_commit_policy           = Policy
        , offset_commit_interval_seconds = Seconds
        } = State,
  case Policy of
    consumer_managed ->
      {ok, State};
    commit_to_kafka_v2 ->
      is_reference(OldTimer) andalso erlang:cancel_timer(OldTimer),
      %% make sure no more than one timer started
      receive
        ?LO_CMD_COMMIT_OFFSETS ->
          ok
      after 0 ->
        ok
      end,
      Timeout = timer:seconds(Seconds),
      Timer = erlang:send_after(Timeout, self(), ?LO_CMD_COMMIT_OFFSETS),
      {ok, State#state{offset_commit_timer = Timer}}
  end.

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
  {ok, CorrId} = brod_sock:request_async(SockPid, Request),
  NewState = State#state{hb_ref = {CorrId, os:timestamp()}},
  {ok, NewState};
maybe_send_heartbeat(#state{} = State) ->
  %% do not send heartbeat when not in group
  {ok, State#state{hb_ref = ?undef}}.

send_sync(SockPid, Request) ->
  send_sync(SockPid, Request, 5000).

send_sync(SockPid, Request, Timeout) ->
  ?ESCALATE(brod_sock:request_sync(SockPid, Request, Timeout)).

log(#state{ groupId  = GroupId
          , memberId = MemberId
          , generationId = GenerationId
          }, Level, Fmt, Args) ->
  brod_utils:log(
    Level,
    "group coordinator (groupId=~s,memberId=~s,generation=~p,pid=~p):\n" ++ Fmt,
    [GroupId, MemberId, GenerationId, self() | Args]).

%% @private Make metata to be committed together with offsets.
-spec make_offset_commit_metadata() -> iodata().
make_offset_commit_metadata() ->
  io_lib:format("~s ~p ~p", [brod_utils:os_time_utc_str(), node(), self()]).

%% @private Make group member's user data in JoinGroupRequest
-spec make_user_data() -> iodata().
make_user_data() -> coordinator_id().

%% @private Make a client_id() to be used in the requests sent over the group
%% coordinator's socket (group coordinator on the other end), this id will be
%% displayed when describing the group status with admin client/script.
%% e.g. brod@localhost/<0.45.0>_/172.18.0.1
%% @end
-spec make_group_connection_client_id() -> binary().
make_group_connection_client_id() -> coordinator_id().

%% @private Use 'node()/pid()' as unique identifier of each group coordinator.
-spec coordinator_id() -> binary().
coordinator_id() ->
  iolist_to_binary(io_lib:format("~p/~p", [node(), self()])).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

merge_acked_offsets_test() ->
  ?assertEqual([{{<<"topic1">>, 1}, 1}],
               merge_acked_offsets([], [{{<<"topic1">>, 1}, 1}])),
  ?assertEqual([{{<<"topic1">>, 1}, 1}, {{<<"topic1">>, 2}, 1}],
               merge_acked_offsets([{{<<"topic1">>, 1}, 1}],
                                   [{{<<"topic1">>, 2}, 1}])),
  ?assertEqual([{{<<"topic1">>, 1}, 2}, {{<<"topic1">>, 2}, 1}],
               merge_acked_offsets([{{<<"topic1">>, 1}, 1},
                                    {{<<"topic1">>, 2}, 1}],
                                   [{{<<"topic1">>, 1}, 2}])),
  ok.

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
