%%%
%%%   Copyright (c) 2016-2017 Klarna AB
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

-type brod_offset_commit_policy() :: commit_to_kafka_v2 % default
                                   | consumer_managed.
-type brod_partition_assignment_strategy() :: roundrobin
                                            | callback_implemented.
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

-define(ESCALATE_EC(EC), ?IS_ERROR(EC) andalso erlang:throw(EC)).

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

-define(INITIAL_MEMBER_ID, <<>>).

-type config() :: brod:group_config().
-type ts() :: erlang:timestamp().
-type member() :: brod:group_member().
-type offset_commit_policy() :: brod_offset_commit_policy().
-type member_id() :: brod:group_member_id().

-record(state,
        { client :: brod:client()
        , groupId :: brod:group_id()
          %% Group member ID, which should be set to empty in the first
          %% join group request, then a new member id is assigned by the
          %% group coordinator and in join group response.
          %% This field may change if the member has lost connection
          %% to the coordinator and received 'UnknownMemberId' exception
          %% in response messages.
        , memberId = <<"">> :: member_id()
          %% State#state.memberId =:= State#state.leaderId if
          %% elected as group leader by the coordinator.
        , leaderId :: ?undef | member_id()
          %% Generation ID is used by the group coordinator to sync state
          %% of the group members, e.g. kick out stale members who have not
          %% kept up with the latest generation ID bumps.
        , generationId = 0 :: integer()
          %% A set of topic names where the group members consumes from
        , topics = [] :: [brod:topic()]
          %% This is the result of group coordinator discovery.
          %% It may change when the coordinator is down then a new one
          %% is elected among the kafka cluster members.
        , coordinator :: ?undef | brod:endpoint()
          %% The socket pid to the group coordinator broker.
          %% This socket is dedicated for group management and
          %% offset commit requests.
          %% We can not just get a payload socket from client
          %% because the socket might be shared with other group
          %% members in the same client, however group members are
          %% distinguished by connections to coordinator
        , sock_pid :: ?undef | pid()
          %% heartbeat reference, to discard stale responses
        , hb_ref :: ?undef | {brod:corr_id(), ts()}
          %% all group members received in the join group response
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
        , acked_offsets = [] :: [{{brod:topic(), brod:partition()}
                                 , brod:offset()}]
          %% The referece of the timer which triggers offset commit
        , offset_commit_timer :: ?undef | reference()

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
-spec start_link(brod:client(), brod:group_id(), [brod:topic()],
                 config(), module(), pid()) -> {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, Config, CbModule, MemberPid) ->
  Args = {Client, GroupId, Topics, Config, CbModule, MemberPid},
  gen_server:start_link(?MODULE, Args, []).

%% @doc For group member to call to acknowledge a consumed message offset.
-spec ack(pid(), integer(), brod:topic(), brod:partition(), brod:offset()) ->
             ok.
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
-spec commit_offsets(pid(),
                     [{{brod:topic(), brod:partition()}, brod:offset()}]) ->
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
handle_info({msg, _Pid, #kpro_rsp{ tag     = heartbeat_response
                                 , corr_id = HbCorrId
                                 , msg     = Body
                                 }},
            #state{hb_ref = {HbCorrId, _SentTime}} = State0) ->
  EC = kpro:find(error_code, Body),
  State = State0#state{hb_ref = ?undef},
  case ?IS_ERROR(EC) of
    true ->
      {ok, NewState} = stabilize(State, 0, EC),
      {noreply, NewState};
    false ->
      {noreply, State}
  end;
handle_info(_Info, #state{} = State) ->
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

handle_cast(_Cast, #state{} = State) ->
  {noreply, State}.

code_change(_OldVsn, #state{} = State, _Extra) ->
  {ok, State}.

terminate(Reason, #state{ sock_pid = SockPid
                        , groupId  = GroupId
                        , memberId = MemberId
                        } = State) ->
  log(State, info, "leaving group, reason ~p\n", [Reason]),
  Body = [{group_id, GroupId}, {member_id, MemberId}],
  Request = kpro:req(leave_group_request, _V = 0, Body),
  try
    _ = send_sync(SockPid, Request, 1000),
    ok
  catch
    _ : _ ->
      ok
  end,
  ok = stop_socket(SockPid).

%%%_* Internal Functions =======================================================

-spec discover_coordinator(#state{}) -> {ok, #state{}}.
discover_coordinator(#state{ client      = Client
                           , coordinator = Coordinator
                           , sock_pid    = SockPid
                           , groupId     = GroupId
                           } = State) ->
  {{Host, Port}, Config} =
    ?ESCALATE(brod_client:get_group_coordinator(Client, GroupId)),
  HasConnectionToCoordinator = Coordinator =:= {Host, Port}
    andalso brod_utils:is_pid_alive(SockPid),
  case HasConnectionToCoordinator of
    true ->
      {ok, State};
    false ->
      %% close old socket
      _ = brod_sock:stop(SockPid),
      ClientId = make_group_connection_client_id(),
      NewSockPid =
        ?ESCALATE(brod_sock:start_link(self(), Host, Port, ClientId, Config)),
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
    true  -> State#state{memberId = ?INITIAL_MEMBER_ID};
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
  Meta =
    [ {version, ?BROD_CONSUMER_GROUP_PROTOCOL_VERSION}
    , {topics, Topics}
    , {user_data, user_data(join)}
    ],
  Protocol =
    [ {protocol_name, PaStrategy}
    , {protocol_metadata, Meta}
    ],
  SessionTimeout = timer:seconds(SessionTimeoutSec),
  Body =
    [ {group_id, GroupId}
    , {session_timeout, SessionTimeout}
    , {member_id, MemberId0}
    , {protocol_type, ?PROTOCOL_TYPE}
    , {group_protocols, [Protocol]}
    ],
  %% TODO: support version 1
  Vsn = 0,
  Req = kpro:req(join_group_request, Vsn, Body),
  %% send join group request and wait for response
  %% as long as the session timeout config
  #kpro_rsp{ tag = join_group_response
           , vsn = Vsn
           , msg = RspBody
           } = send_sync(SockPid, Req, SessionTimeout),
  ?ESCALATE_EC(kpro:find(error_code, RspBody)),
  GenerationId = kpro:find(generation_id,RspBody),
  LeaderId = kpro:find(leader_id, RspBody),
  MemberId = kpro:find(member_id, RspBody),
  Members = kpro:find(members, RspBody),
  IsGroupLeader = (LeaderId =:= MemberId),
  State =
    State0#state{ memberId     = MemberId
                , leaderId     = LeaderId
                , generationId = GenerationId
                , members      = translate_members(Members)
                },
  log(State, info, "elected=~p", [IsGroupLeader]),
  {ok, State}.

%% @private
-spec sync_group(#state{}) -> {ok, #state{}}.
sync_group(#state{ groupId       = GroupId
                 , generationId  = GenerationId
                 , memberId      = MemberId
                 , sock_pid      = SockPid
                 , member_pid    = MemberPid
                 , member_module = MemberModule
                 } = State) ->
  ReqBody =
    [ {group_id, GroupId}
    , {generation_id, GenerationId}
    , {member_id, MemberId}
    , {group_assignment, assign_partitions(State)}
    ],
  Vsn = 0,
  SyncReq = kpro:req(sync_group_request, Vsn, ReqBody),
  %% send sync group request and wait for response
  #kpro_rsp{ tag = sync_group_response
           , vsn = Vsn
           , msg = RspBody
           } = send_sync(SockPid, SyncReq),
  ?ESCALATE_EC(kpro:find(error_code, RspBody)),
  %% get my partition assignments
  Assignment = kpro:find(member_assignment, RspBody),
  TopicAssignments = get_topic_assignments(State, Assignment),
  ok = MemberModule:assignments_received(MemberPid, MemberId,
                                         GenerationId, TopicAssignments),
  NewState = State#state{is_in_group = true},
  log(NewState, info, "assignments received:~s",
      [format_assignments(TopicAssignments)]),
  start_offset_commit_timer(NewState).

%% @private
-spec handle_ack(#state{}, brod:topic(), brod:partition(), brod:offset()) ->
        {ok, #state{}}.
handle_ack(#state{ acked_offsets = AckedOffsets
                 } = State, Topic, Partition, Offset) ->
  NewAckedOffsets =
    merge_acked_offsets(AckedOffsets, [{{Topic, Partition}, Offset}]),
  {ok, State#state{acked_offsets = NewAckedOffsets}}.

%% @private Add new offsets to be acked into the acked offsets collection.
-spec merge_acked_offsets(Offsets, Offsets) -> Offsets when
        Offsets :: [{{brod:topic(), brod:partition()}, brod:offset()}].
merge_acked_offsets(AckedOffsets, OffsetsToAck) ->
  lists:ukeymerge(1, OffsetsToAck, AckedOffsets).

-spec format_assignments(brod:received_assignments()) -> iodata().
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

-spec format_partition_assignments([{brod:partition(), brod:offset()}]) ->
                                      iodata().
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
  TopicOffsets0 =
    lists:foldl(
      fun({{Topic, Partition}, Offset}, Acc) ->
        PartitionOffset =
          [ {partition, Partition}
          , {offset, Offset}
          , {metadata, Metadata}
          ],
        orddict:append_list(Topic, [PartitionOffset], Acc)
      end, [], AckedOffsets),
  TopicOffsets =
    lists:map(
      fun({Topic, PartitionOffsets}) ->
          [ {topic, Topic}
          , {partitions, PartitionOffsets}
          ]
      end, TopicOffsets0),
  Retention =
    case OffsetRetentionSecs =/= ?undef of
      true  -> timer:seconds(OffsetRetentionSecs);
      false -> ?OFFSET_RETENTION_DEFAULT
    end,
  ReqBody =
    [ {group_id, GroupId}
    , {group_generation_id, GenerationId}
    , {member_id, MemberId}
    , {retention_time, Retention}
    , {topics, TopicOffsets}
    ],
  Vsn = 2, %% supports only version 2 (since kafka 0.9)
  Req = kpro:req(offset_commit_request, Vsn, ReqBody),
  #kpro_rsp{ tag = offset_commit_response
           , vsn = Vsn
           , msg = RspBody
           } = send_sync(SockPid, Req),
  Topics = kpro:find(responses, RspBody),
  ok = assert_commit_response(Topics),
  NewState = State#state{acked_offsets = []},
  {ok, NewState}.

%% @private Check commit response. If no error returns ok,
%% if all error codes are the same, raise throw, otherwise error.
%% %% @end
-spec assert_commit_response([kpro:struct()]) -> ok | no_return().
assert_commit_response(Topics) ->
  ErrorSet = collect_commit_response_error_codes(Topics),
  case gb_sets:to_list(ErrorSet) of
    []   -> ok;
    [EC] -> ?ESCALATE_EC(EC);
    _    -> erlang:error({commit_offset_failed, Topics})
  end.

%% @private
-spec collect_commit_response_error_codes([kpro:struct()]) -> gb_sets:set().
collect_commit_response_error_codes(Topics) ->
  lists:foldl(
    fun(Topic, Acc1) ->
        Partitions = kpro:find(partition_responses, Topic),
        lists:foldl(
          fun(Partition, Acc2) ->
              EC = kpro:find(error_code, Partition),
              case ?IS_ERROR(EC) of
                true -> gb_sets:add_element(EC, Acc2);
                false -> Acc2
              end
          end, Acc1, Partitions)
      end, gb_sets:new(), Topics).

%% @private
-spec assign_partitions(#state{}) -> [kpro:struct()].
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
                      [ {topic, Topic}
                      , {partitions, Partitions}
                      ]
                  end, Topics_),
      [ {member_id, MemberId}
      , {member_assignment,
         [ {version, ?BROD_CONSUMER_GROUP_PROTOCOL_VERSION}
         , {topic_partitions, PartitionAssignments}
         , {user_data, user_data(assign)}
         ]}
      ]
    end, Assignments);
assign_partitions(#state{}) ->
  %% only leader can assign partitions to members
  [].

%% @private
-spec translate_members([kpro:struct()]) -> [member()].
translate_members(Members) ->
  lists:map(
    fun(Member) ->
        MemberId = kpro:find(member_id, Member),
        Meta = kpro:find(member_metadata, Member),
        Version = kpro:find(version, Meta),
        Topics = kpro:find(topics, Meta),
        UserData = kpro:find(user_data, Meta),
        {MemberId, #kafka_group_member_metadata{ version   = Version
                                               , topics    = Topics
                                               , user_data = UserData
                                               }}
    end, Members).

%% collect topics from all members
-spec all_topics([member()]) -> [brod:topic()].
all_topics(Members) ->
  lists:usort(
    lists:append(
      lists:map(
        fun({_MemberId, M}) ->
          M#kafka_group_member_metadata.topics
        end, Members))).

-spec get_partitions(brod:client(), brod:topic()) -> [brod:partition()].
get_partitions(Client, Topic) ->
  Count = ?ESCALATE(brod_client:get_partitions_count(Client, Topic)),
  lists:seq(0, Count-1).

%% @private
-spec do_assign_partitions(roundrobin, [member()],
                           [{brod:topic(), brod:partition()}]) ->
                              [{member_id(), [brod:partition_assignment()]}].
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

%% @private
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
-spec get_topic_assignments(#state{}, binary() | [kpro:struct()]) ->
        brod:received_assignments().
get_topic_assignments(#state{}, ?kpro_cg_no_assignment) -> []; %% no assignments
get_topic_assignments(#state{} = State, Assignment) ->
  PartitionAssignments = kpro:find(topic_partitions, Assignment),
  TopicPartitions0 =
    lists:map(
      fun(PartitionAssignment) ->
          Topic = kpro:find(topic, PartitionAssignment),
          Partitions = kpro:find(partitions, PartitionAssignment),
          [{Topic, Partition} || Partition <- Partitions]
      end, PartitionAssignments),
  TopicPartitions = lists:append(TopicPartitions0),
  CommittedOffsets = get_committed_offsets(State, TopicPartitions),
  resolve_begin_offsets(TopicPartitions, CommittedOffsets).

%% @private Fetch committed offsets from kafka,
%% or call the consumer callback to read committed offsets.
%% @end
-spec get_committed_offsets(#state{}, [{brod:topic(), brod:partition()}]) ->
        [{{brod:topic(), brod:partition()}, brod:offset()}].
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
        [ {topic, Topic}
        , {partitions, [[{partition, P}] || P <- Partitions]}
        ]
      end, GrouppedPartitions),
  ReqBody =
    [ {group_id, GroupId}
    , {topics, OffsetFetchRequestTopics}
    ],
  Vsn = 1, %% TODO: pick version
  Req = kpro:req(offset_fetch_request, Vsn, ReqBody),
  #kpro_rsp{ tag = offset_fetch_response
           , msg = RspBody
           } = send_sync(SockPid, Req),
  %% error_code is introduced in version 2
  ?ESCALATE_EC(kpro:find(error_code, RspBody, ?EC_NONE)),
  TopicOffsets = kpro:find(responses, RspBody),
  CommittedOffsets0 =
    lists:map(
      fun(TopicOffset) ->
        Topic = kpro:find(topic, TopicOffset),
        PartitionOffsets = kpro:find(partition_responses, TopicOffset),
        lists:foldl(
          fun(PartitionOffset, Acc) ->
            Partition = kpro:find(partition, PartitionOffset),
            Offset = kpro:find(offset, PartitionOffset),
            EC = kpro:find(error_code, PartitionOffset),
            case (EC =:= ?EC_UNKNOWN_TOPIC_OR_PARTITION) orelse
                 (EC =:= ?EC_NONE andalso Offset =:= -1) of
              true ->
                %% EC_UNKNOWN_TOPIC_OR_PARTITION is kept for version 0
                Acc;
              false ->
                ?ESCALATE_EC(EC),
                [{{Topic, Partition}, Offset} | Acc]
            end
          end, [], PartitionOffsets)
      end, TopicOffsets),
  lists:append(CommittedOffsets0).

%% @private
-spec resolve_begin_offsets([TP], [{TP, brod:offset()}]) ->
        brod:received_assignments()
          when TP :: {brod:topic(), brod:partition()}.
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
  BeginOffset = case is_integer(Offset) andalso Offset >= 0 of
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
  ReqBody =
    [ {group_id, GroupId}
    , {group_generation_id, GenerationId}
    , {member_id, MemberId}
    ],
  Req = kpro:req(heartbeat_request, 0, ReqBody),
  {ok, CorrId} = brod_sock:request_async(SockPid, Req),
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
-spec make_offset_commit_metadata() -> binary().
make_offset_commit_metadata() -> coordinator_id().

%% @private Make group member's user data in join_group_request
%% This piece of data is currently just a dummy string.
%%
%% user_data can be used to share state between group members.
%% It is originally sent by group members in join_group_request:s,
%% then received by group leader, group leader (maybe mutate) assigns
%% it back to members.
%%
%% Currently user_data is created in this module and terminated in this
%% module, when needed for advanced features, we can originate it from
%% member's init callback, and pass it to members via
%% `brod_received_assignments()'
%% @end
-spec user_data(_) -> binary().
user_data(_) -> <<"nothing">>.

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
  bin(io_lib:format("~p/~p", [node(), self()])).

%% @private
-spec bin(iodata()) -> binary().
bin(X) -> iolist_to_binary(X).

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
