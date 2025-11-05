%%%
%%%   Copyright (c) 2016-2021 Klarna Bank AB (publ)
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
        , update_topics/2
        , stop/1
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-export_type([protocol_name/0]).

-include("brod_int.hrl").

-define(PARTITION_ASSIGMENT_STRATEGY_ROUNDROBIN, roundrobin_v2). %% default

-type protocol_name() :: string().
-type brod_offset_commit_policy() :: commit_to_kafka_v2 % default
                                   | consumer_managed.
-type brod_partition_assignment_strategy() :: roundrobin_v2
                                            | callback_implemented.
-type partition_assignment_strategy() :: brod_partition_assignment_strategy().

%% default configs
-define(SESSION_TIMEOUT_SECONDS, 30).
-define(REBALANCE_TIMEOUT_SECONDS, ?SESSION_TIMEOUT_SECONDS).
-define(HEARTBEAT_RATE_SECONDS, 5).
-define(PROTOCOL_TYPE, <<"consumer">>).
-define(MAX_REJOIN_ATTEMPTS, 5).
-define(REJOIN_DELAY_SECONDS, 1).
-define(OFFSET_COMMIT_POLICY, commit_to_kafka_v2).
-define(OFFSET_COMMIT_INTERVAL_SECONDS, 5).
%% use kafka's offset meta-topic retention policy
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
-define(CALL_MEMBER(MemberPid, EXPR),
  try
    EXPR
  catch
    exit:{noproc, _} ->
      exit({shutdown, member_down});
    exit:{Reason, _} ->
      exit({shutdown, {member_down, Reason}})
  end).

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
          %% This socket is dedicated for group management requests for
          %% join group, sync group, offset commit, and heartbeat.
          %% We can not use a payload connection managed by brod_client
          %% because connections in brod_client are shared resources,
          %% but the connection to group coordinator has to be dedicated
          %% to one group member.
        , connection :: ?undef | kpro:connection()
          %% heartbeat reference, to discard stale responses
        , hb_ref :: ?undef | {reference(), ts()}
          %% all group members received in the join group response
        , members = [] :: [member()]
          %% Set to false before joining the group
          %% then set to true when successfully joined the group.
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
          %% The reference of the timer which triggers offset commit
        , offset_commit_timer :: ?undef | reference()

          %% configs, see start_link/6 doc for details
        , partition_assignment_strategy  :: partition_assignment_strategy()
        , session_timeout_seconds        :: pos_integer()
        , rebalance_timeout_seconds      :: pos_integer()
        , heartbeat_rate_seconds         :: pos_integer()
        , max_rejoin_attempts            :: non_neg_integer()
        , rejoin_delay_seconds           :: non_neg_integer()
        , offset_retention_seconds       :: ?undef | integer()
        , offset_commit_policy           :: offset_commit_policy()
        , offset_commit_interval_seconds :: pos_integer()
        , protocol_name                  :: protocol_name()
          %% Static member ID
        , group_instance_id              :: binary()
        }).

-type state() :: #state{}.

-define(IS_LEADER(S), (S#state.leaderId =:= S#state.memberId)).

%%%_* APIs =====================================================================

%% @doc Start a kafka consumer group coordinator.
%%
%% `Client': `ClientId' (or pid, but not recommended)
%%
%% `GroupId': Predefined globally unique (in a kafka cluster) binary string.
%%
%% `Topics': Predefined set of topic names to join the group.
%%
%% `CbModule':  The module which implements group coordinator callbacks
%%
%% `MemberPid': The member process pid.
%%
%% `Config': The group coordinator configs in a proplist, possible entries:
%%
%%  <ul><li>`partition_assignment_strategy' (optional, default =
%%  `roundrobin_v2')
%%
%%  Possible values:
%%
%%      <ul><li>`roundrobin_v2' (topic-sticky)
%%
%%        Take all topic-offset (sorted `topic_partition()' list),
%%        assign one to each member in a roundrobin fashion. Only
%%        partitions in the subscription topic list are assigned.</li>
%%
%%      <li>`callback_implemented'
%%
%%        Call `CbModule:assign_partitions/2' to assign
%%        partitions.</li>
%%  </ul></li>
%%
%%  <li>`session_timeout_seconds' (optional, default = 30)
%%
%%      Time in seconds for the group coordinator broker to consider a member
%%      'down' if no heartbeat or any kind of requests received from a broker
%%      in the past N seconds.
%%      A group member may also consider the coordinator broker 'down' if no
%%      heartbeat response response received in the past N seconds.</li>
%%
%%
%%  <li>`rebalance_timeout_seconds' (optional, default = 30)
%%
%%      Time in seconds for each worker to join the group once a rebalance
%%      has begun. If the timeout is exceeded, then the worker will be
%%      removed from the group, which will cause offset commit failures.</li>
%%
%%
%%  <li>`heartbeat_rate_seconds' (optional, default = 5)
%%
%%      Time in seconds for the member to 'ping' the group coordinator.
%%      OBS: Care should be taken when picking the number, on one hand, we do
%%           not want to flush the broker with requests if we set it too low,
%%           on the other hand, if set it too high, it may take too long for
%%           the members to realise status changes of the group such as
%%           assignment rebalacing or group coordinator switchover etc.</li>
%%
%%  <li>`max_rejoin_attempts' (optional, default = 5)
%%
%%      Maximum number of times allowed for a member to re-join the group.
%%      The gen_server will stop if it reached the maximum number of retries.
%%      OBS: 'let it crash' may not be the optimal strategy here because
%%           the group member id is kept in the gen_server looping state and
%%           it is reused when re-joining the group.</li>
%%
%%  <li>`rejoin_delay_seconds' (optional, default = 1)
%%
%%      Delay in seconds before re-joining the group.</li>
%%
%%  <li>`offset_commit_policy' (optional, default = `commit_to_kafka_v2')
%%
%%      How/where to commit offsets, possible values:<ul>
%%        <li>`commit_to_kafka_v2': Group coordinator will commit the
%%            offsets to kafka using version 2
%%            OffsetCommitRequest.</li>
%%        <li> `consumer_managed': The group member
%%            (e.g. brod_group_subscriber.erl) is responsible for
%%            persisting offsets to a local or centralized storage.
%%            And the callback `get_committed_offsets' should be
%%            implemented to allow group coordinator to retrieve the
%%            committed offsets.</li>
%%  </ul></li>
%%
%%  <li>`offset_commit_interval_seconds' (optional, default = 5)
%%
%%      The time interval between two OffsetCommitRequest messages.
%%      This config is irrelevant if `offset_commit_policy' is
%%      `consumer_managed'.</li>
%%
%%  <li>`offset_retention_seconds' (optional, default = -1)
%%
%%      How long the time is to be kept in kafka before it is deleted.
%%      The default special value -1 indicates that the
%%      __consumer_offsets topic retention policy is used. This config
%%      is irrelevant if `offset_commit_policy' is
%%      `consumer_managed'.</li>
%%
%%  <li>`protocol_name' (optional, default = "roundrobin_v2")
%%
%%      This is the protocol name used when join a group, if not given,
%%      by default `partition_assignment_strategy' is used as the protocol name.
%%      Setting a protocol name allows to interact with consumer group members
%%      designed in other programming languages. For example, 'range' is the most
%%      commonly used protocol name for JAVA client. However, brod only supports
%%      roundrobin protocol out of the box, in order to mimic 'range' protocol
%%      one will have to do it via `callback_implemented' assignment strategy
%%  </li>
%%
%%  <li>`group_instance_id' (optional)
%%      This is the static group member ID.
%%      Default value is `node()/pid()'. For example, `bord@host.domain/<0.1293.0>'
%%  </li>
%% </ul>
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
%%
%% NOTE: `lists:usort/1' is applied on the given extra offsets to commit,
%%       meaning if two or more offsets for the same topic-partition exist
%%       in the list, only the one closest the head of the list is kept
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

%% @doc Update the list of topics the brod_group_coordinator follow which
%% triggers a join group rebalance
-spec update_topics(pid(), [brod:topic()]) -> ok.
update_topics(CoordinatorPid, Topics) ->
  gen_server:cast(CoordinatorPid, {update_topics, Topics}).

%% @doc Stop group coordinator, wait for pid `DOWN' before return.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  exit(Pid, shutdown),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Topics, Config, CbModule, MemberPid}) ->
  erlang:process_flag(trap_exit, true),
  GetCfg = fun(Name, Default) ->
             case proplists:get_value(Name, Config, undefined) of
               undefined ->
                 Default;
               Value ->
                 Value
             end
           end,
  PaStrategy = GetCfg(partition_assignment_strategy,
                      ?PARTITION_ASSIGMENT_STRATEGY_ROUNDROBIN),
  SessionTimeoutSec = GetCfg(session_timeout_seconds, ?SESSION_TIMEOUT_SECONDS),
  RebalanceTimeoutSec = GetCfg(rebalance_timeout_seconds, ?REBALANCE_TIMEOUT_SECONDS),
  HbRateSec = GetCfg(heartbeat_rate_seconds, ?HEARTBEAT_RATE_SECONDS),
  MaxRejoinAttempts = GetCfg(max_rejoin_attempts, ?MAX_REJOIN_ATTEMPTS),
  RejoinDelaySeconds = GetCfg(rejoin_delay_seconds, ?REJOIN_DELAY_SECONDS),
  OffsetRetentionSeconds = GetCfg(offset_retention_seconds, ?undef),
  OffsetCommitPolicy = GetCfg(offset_commit_policy, ?OFFSET_COMMIT_POLICY),
  OffsetCommitIntervalSeconds = GetCfg(offset_commit_interval_seconds,
                                       ?OFFSET_COMMIT_INTERVAL_SECONDS),
  ProtocolName = GetCfg(protocol_name, bin(PaStrategy)),
  StaticMemberID = GetCfg(group_instance_id, default_group_instance_id()),
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
          , rebalance_timeout_seconds      = RebalanceTimeoutSec
          , heartbeat_rate_seconds         = HbRateSec
          , max_rejoin_attempts            = MaxRejoinAttempts
          , rejoin_delay_seconds           = RejoinDelaySeconds
          , offset_retention_seconds       = OffsetRetentionSeconds
          , offset_commit_policy           = OffsetCommitPolicy
          , offset_commit_interval_seconds = OffsetCommitIntervalSeconds
          , protocol_name                  = ProtocolName
          , group_instance_id              = StaticMemberID
          },
  {ok, State}.

handle_info({ack, GenerationId, Topic, Partition, Offset}, State) ->
  {noreply, handle_ack(State, GenerationId, Topic, Partition, Offset)};
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
handle_info({'EXIT', Pid, Reason},
            #state{connection = Pid} = State) ->
  {ok, NewState} = stabilize(State, 0, {connection_down, Reason}),
  {noreply, NewState#state{connection = ?undef}};
handle_info({'EXIT', Pid, Reason}, #state{member_pid = Pid} = State) ->
  case Reason of
    shutdown      -> {stop, shutdown, State};
    {shutdown, _} -> {stop, shutdown, State};
    normal        -> {stop, normal, State};
    _             -> {stop, member_down, State}
  end;
handle_info({'EXIT', _Pid, _Reason}, State) ->
  {stop, shutdown, State};
handle_info(?LO_CMD_SEND_HB,
            #state{ hb_ref                  = HbRef
                  , session_timeout_seconds = SessionTimeoutSec
                  } = State) ->
  _ = start_heartbeat_timer(State#state.heartbeat_rate_seconds),
  case HbRef of
    ?undef ->
      {ok, NewState} = maybe_send_heartbeat(State),
      {noreply, NewState};
    {_Ref, SentTime} ->
      Elapsed = timer:now_diff(os:timestamp(), SentTime),
      case Elapsed < SessionTimeoutSec * 1000000 of
        true ->
          %% keep waiting for heartbeat response
          {noreply, State};
        false ->
          %% try leave group and re-join when restarted by supervisor
          {stop, hb_timeout, State}
      end
  end;
handle_info({msg, _Pid, #kpro_rsp{ api = heartbeat
                                 , ref = HbRef
                                 , msg = Body
                                 }},
            #state{hb_ref = {HbRef, _SentTime}} = State0) ->
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

handle_cast({update_topics, Topics}, State) ->
  NewState0 = State#state{ topics = Topics},
  {ok, NewState} = stabilize(NewState0, 0, topics),
  {noreply, NewState};
handle_cast(_Cast, #state{} = State) ->
  {noreply, State}.

code_change(_OldVsn, #state{} = State, _Extra) ->
  {ok, State}.

terminate(Reason, #state{ connection = Connection
                        , groupId  = GroupId
                        , memberId = MemberId
                        } = State) ->
  log(State, info, "Leaving group, reason: ~p\n", [Reason]),
  Body = [{group_id, GroupId}, {member_id, MemberId}],
  _ = try_commit_offsets(State),
  Request = kpro:make_request(leave_group, _V = 0, Body),
  try
    _ = send_sync(Connection, Request, 1000),
    ok
  catch
    _ : _ ->
      ok
  end.

%%%_* Internal Functions =======================================================

-spec discover_coordinator(state()) -> {ok, state()}.
discover_coordinator(#state{ client     = Client
                           , connection = Connection0
                           , groupId    = GroupId
                           } = State) ->
  {Endpoint, ConnConfig0} =
    ?ESCALATE(brod_client:get_group_coordinator(Client, GroupId)),
  case is_already_connected(State, Endpoint) of
    true ->
      {ok, State};
    false ->
      is_pid(Connection0) andalso kpro:close_connection(Connection0),
      ClientId = make_group_connection_client_id(),
      ConnConfig = ConnConfig0#{client_id => ClientId},
      Connection = ?ESCALATE(kpro:connect(Endpoint, ConnConfig)),
      {ok, State#state{connection = Connection}}
  end.

%% Return true if there is already a connection to the given endpoint.
is_already_connected(#state{connection = Conn}, _) when not is_pid(Conn) ->
  false;
is_already_connected(#state{connection = Conn}, {Host, Port}) ->
  {Host0, Port0} = ?ESCALATE(kpro_connection:get_endpoint(Conn)),
  iolist_to_binary(Host0) =:= iolist_to_binary(Host) andalso
  Port0 =:= Port.

-spec stabilize(state(), integer(), any()) -> {ok, state()}.
stabilize(#state{ rejoin_delay_seconds = RejoinDelaySeconds
                , member_module        = MemberModule
                , member_pid           = MemberPid
                , offset_commit_timer  = Timer
                } = State0, AttemptNo, Reason) ->
  is_reference(Timer) andalso erlang:cancel_timer(Timer),
  Reason =/= ?undef andalso
    log(State0, info, "re-joining group, reason:~p", [Reason]),

  %% 1. unsubscribe all currently assigned partitions
  ?CALL_MEMBER(MemberPid, MemberModule:assignments_revoked(MemberPid)),

  %% 2. some brod_group_member implementations may wait for messages
  %%    to finish processing when assignments_revoked is called.
  %%    The acknowledments of those messages would then be sitting
  %%    in our inbox. So we do an explicit pass to collect all pending
  %%    acks so they are included in the best-effort commit below.
  State1 = receive_pending_acks(State0),

  %% 3. try to commit current offsets before re-joinning the group.
  %%    try only on the first re-join attempt
  %%    do not try if it was illegal generation or unknown member id
  %%    exception received because it will fail on the same exception
  %%    again
  State2 =
    case AttemptNo =:= 0 andalso
         Reason    =/= ?illegal_generation andalso
         Reason    =/= ?unknown_member_id of
      true ->
        {ok, #state{} = State2_} = try_commit_offsets(State1),
        State2_;
      false ->
        State1
    end,
  State3 = State2#state{is_in_group = false},

  %$ 4. Clean up state based on the last failure reason
  State4 = maybe_reset_member_id(State3, Reason),

  %% 5. Clean up ongoing heartbeat request ref if connection
  %%    was closed
  State = maybe_reset_hb_ref(State4, Reason),

  %% 5. ensure we have a connection to the (maybe new) group coordinator
  F1 = fun discover_coordinator/1,
  %% 6. join group
  F2 = fun join_group/1,
  %% 7. sync assignments
  F3 = fun sync_group/1,

  RetryFun =
    fun(StateIn, NewReason) ->
      log(StateIn, info, "failed to join group\nreason: ~p", [NewReason]),
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

-spec receive_pending_acks(state()) -> state().
receive_pending_acks(State) ->
  receive
    {ack, GenerationId, Topic, Partition, Offset} ->
      NewState = handle_ack(State, GenerationId, Topic, Partition, Offset),
      receive_pending_acks(NewState)
  after
    0 -> State
  end.

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

should_reset_member_id(?unknown_member_id) ->
  %% we are likely kicked out from the group
  %% rejoin with empty member id
  true;
should_reset_member_id(?not_coordinator) ->
  %% the coordinator have moved to another broker
  %% set it to ?undef to trigger a re-discover
  true;
should_reset_member_id({connection_down, _Reason}) ->
  %% old connection was down, new connection will lead
  %% to a new member id
  true;
should_reset_member_id(_) ->
  false.

%% When connection goes down while waiting for heartbeat
%% response, the response will never be received.
%% Reset heartbeat ref to let new heartbeat request to
%% be sent over new connection.
maybe_reset_hb_ref(State, {connection_down, _Reason}) ->
  State#state{hb_ref = ?undef};
maybe_reset_hb_ref(State, _) ->
  State.

-spec join_group(state()) -> {ok, state()}.
join_group(#state{ groupId                    = GroupId
                 , memberId                   = MemberId0
                 , topics                     = Topics
                 , connection                 = Connection
                 , session_timeout_seconds    = SessionTimeoutSec
                 , rebalance_timeout_seconds  = RebalanceTimeoutSec
                 , protocol_name              = ProtocolName
                 , member_module              = MemberModule
                 , member_pid                 = MemberPid
                 , group_instance_id          = StaticMemberID
                 } = State0) ->
  Meta =
    [ {version, ?BROD_CONSUMER_GROUP_PROTOCOL_VERSION}
    , {topics, Topics}
    , {user_data, user_data(MemberModule, MemberPid)}
    ],
  Protocol =
    [ {name, ProtocolName}
    , {metadata, Meta}
    ],
  SessionTimeout = timer:seconds(SessionTimeoutSec),
  RebalanceTimeout = timer:seconds(RebalanceTimeoutSec),
  Body =
    [ {group_id, GroupId}
    , {session_timeout_ms, SessionTimeout}
    , {rebalance_timeout_ms, RebalanceTimeout}
    , {member_id, MemberId0}
    , {protocol_type, ?PROTOCOL_TYPE}
    , {protocols, [Protocol]}
    , {group_instance_id, StaticMemberID}
    ],
  Req = brod_kafka_request:join_group(Connection, Body),
  %% send join group request and wait for response
  %% as long as the session timeout config
  RspBody = send_sync(Connection, Req, SessionTimeout),
  GenerationId = kpro:find(generation_id, RspBody),
  LeaderId = kpro:find(leader, RspBody),
  MemberId = kpro:find(member_id, RspBody),
  Members0 = kpro:find(members, RspBody),
  Members1 = translate_members(Members0),
  Members  = ensure_leader_at_hd(LeaderId, Members1),
  IsGroupLeader = (LeaderId =:= MemberId),
  State =
    State0#state{ memberId     = MemberId
                , leaderId     = LeaderId
                , generationId = GenerationId
                , members      = Members
                },
  log(State, info, "elected=~p", [IsGroupLeader]),
  {ok, State}.

-spec sync_group(state()) -> {ok, state()}.
sync_group(#state{ groupId       = GroupId
                 , generationId  = GenerationId
                 , memberId      = MemberId
                 , connection    = Connection
                 , member_pid    = MemberPid
                 , member_module = MemberModule
                 } = State) ->
  ReqBody =
    [ {group_id, GroupId}
    , {generation_id, GenerationId}
    , {member_id, MemberId}
    , {assignments, assign_partitions(State)}
    ],
  SyncReq = brod_kafka_request:sync_group(Connection, ReqBody),
  %% send sync group request and wait for response
  RspBody = send_sync(Connection, SyncReq),
  %% get my partition assignments
  Assignment = kpro:find(assignment, RspBody),
  TopicAssignments = get_topic_assignments(State, Assignment),
  ?CALL_MEMBER(MemberPid,
    MemberModule:assignments_received(MemberPid, MemberId, GenerationId, TopicAssignments)),
  NewState = State#state{is_in_group = true},
  log(NewState, info, "assignments received:~s",
      [format_assignments(TopicAssignments)]),
  start_offset_commit_timer(NewState).

-spec handle_ack(state(), brod:group_generation_id(), brod:topic(),
                 brod:partition(), brod:offset()) -> state().
handle_ack(State, GenerationId, _Topic, _Partition, _Offset)
    when GenerationId < State#state.generationId ->
  State;
handle_ack(#state{acked_offsets = AckedOffsets} = State,
           _GenerationId, Topic, Partition, Offset) ->
  NewAckedOffsets =
    merge_acked_offsets(AckedOffsets, [{{Topic, Partition}, Offset}]),
  State#state{acked_offsets = NewAckedOffsets}.

%% Add new offsets to be acked into the acked offsets collection.
-spec merge_acked_offsets(Offsets, Offsets) -> Offsets when
        Offsets :: [{{brod:topic(), brod:partition()}, brod:offset()}].
merge_acked_offsets(AckedOffsets, OffsetsToAck) ->
  lists:ukeymerge(1, OffsetsToAck, AckedOffsets).

-spec format_assignments(brod:received_assignments()) -> iodata().
format_assignments([]) -> "[]";
format_assignments(Assignments) ->
  Groupped =
    brod_utils:group_per_key(
      fun(#brod_received_assignment{ topic        = Topic
                                   , partition    = Partition
                                   , begin_offset = Offset
                                   }) ->
          {Topic, {Partition, Offset}}
      end, Assignments),
  lists:map(
    fun({Topic, Partitions}) ->
      ["\n  ", Topic, ":", format_partition_assignments(Partitions) ]
    end, Groupped).

-spec format_partition_assignments([{brod:partition(), brod:offset()}]) ->
                                      iodata().
format_partition_assignments([]) -> [];
format_partition_assignments([{Partition, BeginOffset} | Rest]) ->
  [ io_lib:format("\n    partition=~p begin_offset=~p",
                  [Partition, BeginOffset])
  , format_partition_assignments(Rest)
  ].

%% Commit the current offsets before re-join the group.
%% NOTE: this is a 'best-effort' attempt, failing to commit offset
%%       at this stage should be fine, after all, the consumers will
%%       refresh their start point offsets when new assignment is
%%       received.
-spec try_commit_offsets(state()) -> {ok, state()}.
try_commit_offsets(#state{} = State) ->
  try
    {ok, #state{}} = do_commit_offsets(State)
  catch _ : _ ->
    {ok, State}
  end.

%% Commit collected offsets, stop old commit timer, start new timer.
-spec do_commit_offsets(state()) -> {ok, state()}.
do_commit_offsets(State) ->
  {ok, NewState} = do_commit_offsets_(State),
  start_offset_commit_timer(NewState).

-spec do_commit_offsets_(state()) -> {ok, state()}.
do_commit_offsets_(#state{acked_offsets = []} = State) ->
  {ok, State};
do_commit_offsets_(#state{offset_commit_policy = consumer_managed} = State) ->
  {ok, State};
do_commit_offsets_(#state{ groupId                  = GroupId
                         , memberId                 = MemberId
                         , generationId             = GenerationId
                         , connection               = Connection
                         , offset_retention_seconds = OffsetRetentionSecs
                         , acked_offsets            = AckedOffsets
                         } = State) ->
  Metadata = make_offset_commit_metadata(),
  TopicOffsets0 =
    brod_utils:group_per_key(
      fun({{Topic, Partition}, Offset}) ->
        PartitionOffset =
          [ {partition_index, Partition}
          , {committed_offset, Offset + 1} %% +1 since roundrobin_v2 protocol
          , {committed_metadata, Metadata}
          ],
        {Topic, PartitionOffset}
      end, AckedOffsets),
  TopicOffsets =
    lists:map(
      fun({Topic, PartitionOffsets}) ->
          [ {name, Topic}
          , {partitions, PartitionOffsets}
          ]
      end, TopicOffsets0),
  Retention =
    case is_default_offset_retention(OffsetRetentionSecs) of
      true -> ?OFFSET_RETENTION_DEFAULT;
      false -> timer:seconds(OffsetRetentionSecs)
    end,
  ReqBody =
    [ {group_id, GroupId}
    , {generation_id, GenerationId}
    , {member_id, MemberId}
    , {retention_time_ms, Retention}
    , {topics, TopicOffsets}
    ],
  Req = brod_kafka_request:offset_commit(Connection, ReqBody),
  RspBody = send_sync(Connection, Req),
  Topics = kpro:find(topics, RspBody),
  ok = assert_commit_response(Topics),
  NewState = State#state{acked_offsets = []},
  {ok, NewState}.

%% Check commit response. If no error returns ok,
%% if all error codes are the same, raise throw, otherwise error.
-spec assert_commit_response([kpro:struct()]) -> ok | no_return().
assert_commit_response(Topics) ->
  ErrorSet = collect_commit_response_error_codes(Topics),
  case gb_sets:to_list(ErrorSet) of
    []   -> ok;
    [EC] -> ?ESCALATE_EC(EC);
    _    -> erlang:error({commit_offset_failed, Topics})
  end.

-spec collect_commit_response_error_codes([kpro:struct()]) -> gb_sets:set().
collect_commit_response_error_codes(Topics) ->
  lists:foldl(
    fun(Topic, Acc1) ->
        Partitions = kpro:find(partitions, Topic),
        lists:foldl(
          fun(Partition, Acc2) ->
              EC = kpro:find(error_code, Partition),
              case ?IS_ERROR(EC) of
                true -> gb_sets:add_element(EC, Acc2);
                false -> Acc2
              end
          end, Acc1, Partitions)
      end, gb_sets:new(), Topics).

-spec assign_partitions(state()) -> [kpro:struct()].
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
        ?CALL_MEMBER(MemberPid, MemberModule:assign_partitions(MemberPid, Members, AllPartitions));
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
      , {assignment,
         [ {version, ?BROD_CONSUMER_GROUP_PROTOCOL_VERSION}
         , {topic_partitions, PartitionAssignments}
         , {user_data, <<>>}
         ]}
      ]
    end, Assignments);
assign_partitions(#state{}) ->
  %% only leader can assign partitions to members
  [].

%% Ensure leader member is positioned at head of the list.
%% This is sort of a hidden feature but the easiest way to ensure
%% backward compatibility for MemberModule:assign_partitions
ensure_leader_at_hd(LeaderId, Members) ->
  case lists:keytake(LeaderId, 1, Members) of
    {value, Leader, Followers} ->
      [Leader | Followers];
    false ->
      %% leader is not elected yet.
      Members
  end.

-spec translate_members([kpro:struct()]) -> [member()].
translate_members(Members) ->
  lists:map(
    fun(Member) ->
        MemberId = kpro:find(member_id, Member),
        Meta = kpro:find(metadata, Member),
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
  lists:seq(0, Count - 1).

-spec do_assign_partitions(roundrobin_v2, [member()],
                           [{brod:topic(), brod:partition()}]) ->
                              [{member_id(), [brod:partition_assignment()]}].
do_assign_partitions(roundrobin_v2, Members, AllPartitions) ->
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

%% Extract the partition assignemts from SyncGroupResponse
%% then fetch the committed offsets of each partition.
-spec get_topic_assignments(state(), binary() | [kpro:struct()]) ->
        brod:received_assignments().
get_topic_assignments(#state{}, ?kpro_cg_no_assignment) -> [];
get_topic_assignments(#state{}, #{topic_partitions := []}) -> [];
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
  IsConsumerManaged = State#state.offset_commit_policy =:= consumer_managed,
  resolve_begin_offsets(TopicPartitions, CommittedOffsets, IsConsumerManaged).

%% Fetch committed offsets from kafka,
%% or call the consumer callback to read committed offsets.
-spec get_committed_offsets(state(), [{brod:topic(), brod:partition()}]) ->
        [{{brod:topic(), brod:partition()},
          brod:offset() | {begin_offset, brod:offset_time()}}].
get_committed_offsets(#state{ offset_commit_policy = consumer_managed
                            , member_pid           = MemberPid
                            , member_module        = MemberModule
                            }, TopicPartitions) ->
  {ok, R} = MemberModule:get_committed_offsets(MemberPid, TopicPartitions),
  R;
get_committed_offsets(#state{ offset_commit_policy = commit_to_kafka_v2
                            , groupId              = GroupId
                            , connection           = Conn
                            }, TopicPartitions) ->
  GrouppedPartitions = brod_utils:group_per_key(TopicPartitions),
  Req = brod_kafka_request:offset_fetch(Conn, GroupId, GrouppedPartitions),
  RspBody = send_sync(Conn, Req),
  %% error_code is introduced in version 2
  ?ESCALATE_EC(kpro:find(error_code, RspBody, ?no_error)),
  TopicOffsets = kpro:find(topics, RspBody),
  CommittedOffsets0 =
    lists:map(
      fun(TopicOffset) ->
        Topic = kpro:find(name, TopicOffset),
        PartitionOffsets = kpro:find(partitions, TopicOffset),
        lists:foldl(
          fun(PartitionOffset, Acc) ->
            Partition = kpro:find(partition_index, PartitionOffset),
            Offset0 = kpro:find(committed_offset, PartitionOffset),
            Metadata = kpro:find(metadata, PartitionOffset),
            EC = kpro:find(error_code, PartitionOffset),
            ?ESCALATE_EC(EC),
            %% Offset -1 in offset_fetch_response is an indicator of 'no-value'
            case Offset0 =:= -1 of
              true ->
                Acc;
              false ->
                Offset = maybe_upgrade_from_roundrobin_v1(Offset0, Metadata),
                [{{Topic, Partition}, Offset} | Acc]
            end
          end, [], PartitionOffsets)
      end, TopicOffsets),
  lists:append(CommittedOffsets0).

-spec resolve_begin_offsets(
        [TP],
        [{TP, brod:offset() | {begin_offset, brod:offset_time()}}],
        boolean()) ->
          brod:received_assignments()
            when TP :: {brod:topic(), brod:partition()}.
resolve_begin_offsets([], _CommittedOffsets, _IsConsumerManaged) -> [];
resolve_begin_offsets([{Topic, Partition} | Rest], CommittedOffsets,
                      IsConsumerManaged) ->
  BeginOffset =
    case lists:keyfind({Topic, Partition}, 1, CommittedOffsets) of
      {_, {begin_offset, Offset}} ->
        %% already resolved
        resolve_special_offset(Offset);
      {_, Offset} when IsConsumerManaged ->
        %% roundrobin_v2 is only for kafka commits
        %% for consumer managed offsets, it's still acked offsets
        %% therefore we need to +1 as begin_offset
        Offset + 1;
      {_, Offset} ->
        %% offsets committed to kafka is already begin_offset
        %% since the introduction of 'roundrobin_v2' protocol
        Offset;
      false  ->
        %% No commit history found
        %% Should use default begin_offset in consumer config
        ?undef
    end,
  Assignment =
    #brod_received_assignment{ topic        = Topic
                             , partition    = Partition
                             , begin_offset = BeginOffset
                             },
  [ Assignment
  | resolve_begin_offsets(Rest, CommittedOffsets, IsConsumerManaged)
  ].

%% Nothing is earlier than '0'.
%% use an atom here to avoid getting turned into '-1' which means 'latest',
%% or turned into '+1' in e.g. brod_topic_subscriber because it expects acked offsets.
resolve_special_offset(0) -> ?OFFSET_EARLIEST;
resolve_special_offset(Other) -> Other.

%% Start a timer to send a loopback command to self() to trigger
%% a heartbeat request to the group coordinator.
%% NOTE: the heartbeat requests are sent only when it is in group,
%%       but the timer is always restarted after expiration.
-spec start_heartbeat_timer(pos_integer()) -> ok.
start_heartbeat_timer(HbRateSec) ->
  erlang:send_after(timer:seconds(HbRateSec), self(), ?LO_CMD_SEND_HB),
  ok.

%% Start a timer to send a loopback command to self() to trigger
%% a offset commit request to group coordinator broker.
-spec start_offset_commit_timer(state()) -> {ok, state()}.
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

%% Send heartbeat request if it has joined the group.
-spec maybe_send_heartbeat(state()) -> {ok, state()}.
maybe_send_heartbeat(#state{ is_in_group  = true
                           , groupId      = GroupId
                           , memberId     = MemberId
                           , generationId = GenerationId
                           , connection   = Connection
                           } = State) ->
  ReqBody =
    [ {group_id, GroupId}
    , {generation_id, GenerationId}
    , {member_id, MemberId}
    ],
  Req = kpro:make_request(heartbeat, 0, ReqBody),
  ok = kpro:request_async(Connection, Req),
  NewState = State#state{hb_ref = {Req#kpro_req.ref, os:timestamp()}},
  {ok, NewState};
maybe_send_heartbeat(#state{} = State) ->
  %% do not send heartbeat when not in group
  {ok, State#state{hb_ref = ?undef}}.

send_sync(Connection, Request) ->
  send_sync(Connection, Request, 5000).

send_sync(Connection, Request, Timeout) ->
  ?ESCALATE(brod_utils:request_sync(Connection, Request, Timeout)).

log(#state{ groupId  = GroupId
          , generationId = GenerationId
          , member_pid = MemberPid
          }, Level, Fmt, Args) ->
  ?BROD_LOG(Level,
            "Group member (~s,coor=~p,cb=~p,generation=~p):\n" ++ Fmt,
            [GroupId, self(), MemberPid, GenerationId | Args]).

%% Make metata to be committed together with offsets.
-spec make_offset_commit_metadata() -> binary().
make_offset_commit_metadata() ->
  %% Use a '+1/' prefix as a commit from group member which supports
  %% roundrobin_v2 protocol
  bin(["+1/", coordinator_id()]).

%% Make group member's user data in join_group_request
%%
%% user_data can be used to share state between group members.
%% It is originally sent by group members in join_group_request:s,
%% then received by group leader, group leader (may mutate it and)
%% assigns it back to members in topic-partition assignments.
%%
%% Currently user_data in join requests can be created from a
%% callback to member module.
%% user_data assigned back to members is always an empty binary
%% if assign_partitions callback is not implemented, otherwise
%% should be set in the assign_partitions callback implementation.
user_data(Module, Pid) ->
  %% Module is ensured to be loaded already
  brod_utils:optional_callback(Module, user_data, [Pid], <<>>).

%% Make a client ID to be used in the requests sent over the group
%% coordinator's connection (group coordinator broker on the other end),
%% this id will be displayed when describing the group status with admin
%% client/script. e.g. brod@localhost/<0.45.0>_/172.18.0.1
-spec make_group_connection_client_id() -> binary().
make_group_connection_client_id() -> coordinator_id().

%% Use 'node()/pid()' as unique identifier of each group coordinator.
-spec coordinator_id() -> binary().
coordinator_id() ->
  bin(io_lib:format("~p/~p", [node(), self()])).

-spec bin(atom() | iodata()) -> binary().
bin(A) when is_atom(A) -> atom_to_binary(A);
bin(X) -> iolist_to_binary(X).

%% Before roundrobin_v2, brod had two versions of commit metadata:
%% 1. "ts() node() pid()"
%%    e.g. "2017-10-24:18:20:55.475670 'nodename@host-name' <0.18980.6>"
%% 2. "node()/pid()"
%%    e.g. "'nodename@host-name'/<0.18980.6>"
%% Then roundrobin_v2:
%%    "+1/node()/pid()"
%%    e.g. "+1/'nodename@host-name'/<0.18980.6>"
%% Here we try to recognize brod commits using a regexp,
%% then check the +1 prefix to exclude roundrobin_v2.
-spec is_roundrobin_v1_commit(?kpro_null | binary()) -> boolean().
is_roundrobin_v1_commit(?kpro_null) -> false;
is_roundrobin_v1_commit(<<"+1/", _/binary>>) -> false;
is_roundrobin_v1_commit(Metadata) ->
  case re:run(Metadata, ".*@.*[/|\s]<0\.[0-9]+\.[0-9]+>$") of
    nomatch -> false;
    {match, _} -> true
  end.

%% Upgrade offset from old roundrobin protocol to new.
%% old (before roundrobin_v2) brod commits acked offsets not begin_offset
-spec maybe_upgrade_from_roundrobin_v1(brod:offset(), binary()) ->
        brod:offset().
maybe_upgrade_from_roundrobin_v1(Offset, Metadata) ->
  case is_roundrobin_v1_commit(Metadata) of
    true  -> Offset + 1;
    false -> Offset
  end.

%% Return true if it should be default retention to be used in offset commit
is_default_offset_retention(-1) -> true;
is_default_offset_retention(?undef) -> true;
is_default_offset_retention(_) -> false.

default_group_instance_id() ->
    bin([bin(node()), "/", pid_to_list(self())]).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

merge_acked_offsets_test() ->
  ?assertEqual([{{<<"topic1">>, 1}, 1}],
               merge_acked_offsets([], [{{<<"topic1">>, 1}, 1}])),
  ?assertEqual([{{<<"topic2">>, 1}, 1}, {{<<"topic2">>, 2}, 1}],
               merge_acked_offsets([{{<<"topic2">>, 1}, 1}],
                                   [{{<<"topic2">>, 2}, 1}])),
  ?assertEqual([{{<<"topic3">>, 1}, 2}, {{<<"topic3">>, 2}, 1}],
               merge_acked_offsets([{{<<"topic3">>, 1}, 1},
                                    {{<<"topic3">>, 2}, 1}],
                                   [{{<<"topic3">>, 1}, 2}])),
  ok.

is_roundrobin_v1_commit_test() ->
  M1 = bin("2017-10-24:18:20:55.475670 'nodename@host-name' <0.18980.6>"),
  M2 = bin("'nodename@host-name'/<0.18980.6>"),
  ?assert(is_roundrobin_v1_commit(M1)),
  ?assert(is_roundrobin_v1_commit(M2)),
  ?assertNot(is_roundrobin_v1_commit(<<"">>)),
  ?assertNot(is_roundrobin_v1_commit(?kpro_null)),
  ?assertNot(is_roundrobin_v1_commit(<<"+1/not-node()-pid()">>)),
  ?assertNot(is_roundrobin_v1_commit(make_offset_commit_metadata())),
  ok.

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
