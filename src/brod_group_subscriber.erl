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
%%% A group subscriber is a gen_server which subscribes to partition consumers
%%% (poller) and calls the user-defined callback functions for message
%%% processing.
%%%
%%% An overview of what it does behind the scene:
%%%  1. Start a consumer group coordinator to manage the consumer group states,
%%%     @see brod_group_coordinator:start_link/4.
%%%  2. Start (if not already started) topic-consumers (pollers) and subscribe
%%%     to the partition workers when group assignment is received from the
%%      group leader, @see brod:start_consumer/3.
%%%  3. Call CallbackModule:handle_message/4 when messages are received from
%%%     the partition consumers.
%%%  4. Send acknowledged offsets to group coordinator which will be committed
%%%     to kafka periodically.
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_group_subscriber).

-behaviour(gen_server).
-behaviour(brod_group_member).

-export([ ack/4
        , commit/1
        , start_link/7
        , stop/1
        ]).

%% callbacks for brod_group_coordinator
-export([ get_committed_offsets/2
        , assignments_received/4
        , assignments_revoked/1
        , assign_partitions/3
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-type cb_state() :: term().

%% Initialize the callback module s state.
-callback init(group_id(), term()) -> {ok, cb_state()}.

%% Handle a message. Return one of:
%%
%% {ok, NewCallbackState}:
%%   The subscriber has received the message for processing async-ly.
%%   It should call brod_group_subscriber:ack/4 to acknowledge later.
%%
%% {ok, ack, NewCallbackState}
%%   The subscriber has completed processing the message
%%
%% NOTE: While this callback function is being evaluated, the fetch-ahead
%%       partition-consumers are fetching more messages behind the scene
%%       unless prefetch_count is set to 0 in consumer config.
%%
-callback handle_message(topic(), partition(), #kafka_message{}, cb_state()) ->
            {ok, cb_state()} | {ok, ack, cb_state()}.

%% This callback is called only when subscriber is to commit offsets locally
%% instead of kafka.
%% Return {ok, Offsets, cb_state()} where Offsets can be [],
%% or only the ones that are found in e.g. local storage or database.
%% For the topic-partitions which have no committed offset found,
%% the consumer will take 'begin_offset' in consumer config as the start point
%% of data stream. If 'begin_offset' is not found in consumer config, the
%% default value -1 (latest) is used.
%
% commented out as it's an optional callback
%-callback get_committed_offsets(group_id(), [{topic(), partition()}],
%                                cb_state()) ->
%            {ok, [{{topic(), partition()}, offset()}], cb_state()}.


%% This function is called only when 'partition_assignment_strategy' is
%% 'callback_implemented' in group config.
%
% commented out as it's an optional callback
%-callback assign_partitions([kafka_group_member()],
%                            [{topic(), partition()}]
%                            cb_state()) -> [{kafka_group_member_id(),
%                                            [brod_partition_assignment()]}].

-define(DOWN(Reason), {down, brod_utils:os_time_utc_str(), Reason}).

-record(consumer, { topic_partition :: {topic(), partition()}
                  , consumer_pid    :: pid() | {down, string(), any()}
                  , consumer_mref   :: reference()
                  , begin_offset    :: offset()
                  , acked_offset    :: offset()
                  }).

-type cb_fun() :: fun((cb_state()) -> {AckNow :: boolean(), cb_state()}).

-type ack_ref() :: {topic(), partition(), offset()}.

-record(state,
        { client                :: client()
        , groupId               :: group_id()
        , memberId              :: member_id()
        , generationId          :: integer()
        , coordinator           :: pid()
        , consumers = []        :: [#consumer{}]
        , consumer_config       :: consumer_config()
        , is_blocked = false    :: boolean()
        , cb_module             :: module()
        , cb_state              :: cb_state()
        , pending_ack = ?undef  :: ?undef | ack_ref()
        , pending_messages = [] :: [{ack_ref(), cb_fun()}]
        }).

%% delay 2 seconds retry the failed subscription to partiton consumer process
-define(RESUBSCRIBE_DELAY, 2000).

-define(LO_CMD_SUBSCRIBE_PARTITIONS, '$subscribe_partitions').
-define(LO_CMD_PROCESS_MESSAGE, '$process_message').

%%%_* APIs =====================================================================

%% @doc Start (link) a group subscriber.
%% Client:
%%   Client ID (or pid, but not recommended) of the brod client.
%% GroupId:
%%   Consumer group ID which should be unique per kafka cluster
%% Topics:
%%   Predefined set of topic names to join the group.
%%   NOTE: The group leader member will collect topics from all members and
%%         assign all collected topic-partitions to members in the group.
%%         i.e. members can join with arbitrary set of topics.
%% GroupConfig:
%%   For group coordinator, @see brod_group_coordinator:start_link/5
%% ConsumerConfig:
%%   For partition consumer, @see brod_consumer:start_link/4
%% CbModule:
%%   Callback module which should have the callback functions
%%   implemented for message processing.
%% CbInitArg:
%%   The term() that is going to be passed to CbModule:init/1 when
%%   initializing the subscriger.
%% @end
-spec start_link(client(), group_id(), [topic()],
                 group_config(), consumer_config(), module(), term()) ->
                    {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, GroupConfig,
           ConsumerConfig, CbModule, CbInitArg) ->
  Args = {Client, GroupId, Topics, GroupConfig,
          ConsumerConfig, CbModule, CbInitArg},
  gen_server:start_link(?MODULE, Args, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Acknowledge a message.
-spec ack(pid(), topic(), partition(), offset()) -> ok.
ack(Pid, Topic, Partition, Offset) ->
  gen_server:cast(Pid, {ack, Topic, Partition, Offset}).

%% @doc Commit all acked offsets. NOTE: This is an async call.
-spec commit(pid()) -> ok.
commit(Pid) ->
  gen_server:cast(Pid, commit_offsets).

%%%_* APIs for group coordinator ===============================================

%% @doc Called by group coordinator when there is new assignemnt received.
-spec assignments_received(pid(), member_id(), integer(),
                           brod_received_assignments()) -> ok.
assignments_received(Pid, MemberId, GenerationId, TopicAssignments) ->
  gen_server:cast(Pid, {new_assignments, MemberId,
                        GenerationId, TopicAssignments}).

%% @doc Called by group coordinator before re-joinning the consumer group.
-spec assignments_revoked(pid()) -> ok.
assignments_revoked(Pid) ->
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

-spec assign_partitions(pid(), [kpro_GroupMemberMetadata()],
                        [{topic(), partition()}]) ->
                            [{kafka_group_member_id(),
                              [brod_partition_assignment()]}].
assign_partitions(Pid, MemberMetadataList, TopicPartitionList) ->
  Call = {assign_partitions, MemberMetadataList, TopicPartitionList},
  gen_server:call(Pid, Call, infinity).

%% @doc Called by group coordinator when initializing the assignments
%% for subscriber.
%% NOTE: this function is called only when it is DISABLED to commit offsets
%%       to kafka.
%% @end
-spec get_committed_offsets(pid(), [{topic(), partition()}]) ->
        {ok, [{{topic(), partition()}, offset()}]}.
get_committed_offsets(Pid, TopicPartitions) ->
  gen_server:call(Pid, {get_committed_offsets, TopicPartitions}, infinity).

%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Topics, GroupConfig,
      ConsumerConfig, CbModule, CbInitArg}) ->
  {ok, CbState} = CbModule:init(GroupId, CbInitArg),
  {ok, Pid} = brod_group_coordinator:start_link(Client, GroupId, Topics,
                                                GroupConfig, ?MODULE, self()),
  State = #state{ client          = Client
                , groupId         = GroupId
                , coordinator     = Pid
                , consumer_config = ConsumerConfig
                , cb_module       = CbModule
                , cb_state        = CbState
                },
  {ok, State}.

handle_info({_ConsumerPid,
             #kafka_message_set{ topic     = Topic
                               , partition = Partition
                               , messages  = Messages
                               }},
            #state{ cb_module        = CbModule
                  , pending_messages = Pendings
                  } = State) ->
  MapFun =
    fun(#kafka_message{offset = Offset} = Msg) ->
      AckRef = {Topic, Partition, Offset},
      CbFun =
        fun(CbState) ->
          case CbModule:handle_message(Topic, Partition, Msg, CbState) of
            {ok, NewCbState} ->
              {_AckNow = false, NewCbState};
            {ok, ack, NewCbState} ->
              {_AckNow = true, NewCbState};
            Unknown ->
              erlang:error({bad_return_value,
                           {CbModule, handle_message, Unknown}})
          end
        end,
      {AckRef, CbFun}
    end,
  NewPendings = Pendings ++ lists:map(MapFun, Messages),
  NewState = State#state{pending_messages = NewPendings},
  _ = send_lo_cmd(?LO_CMD_PROCESS_MESSAGE),
  {noreply, NewState};
handle_info({'DOWN', _Mref, process, Pid, Reason},
            #state{consumers = Consumers} = State) ->
  case lists:keyfind(Pid, #consumer.consumer_pid, Consumers) of
    #consumer{topic_partition = TP} = Consumer ->
      NewConsumer = Consumer#consumer{ consumer_pid  = ?DOWN(Reason)
                                     , consumer_mref = ?undef
                                     },
      NewConsumers = lists:keyreplace(TP, #consumer.topic_partition,
                                      Consumers, NewConsumer),
      NewState = State#state{consumers = NewConsumers},
      {noreply, NewState};
    false ->
      {noreply, State}
  end;
handle_info(?LO_CMD_PROCESS_MESSAGE, State) ->
  {ok, NewState} = maybe_process_message(State),
  {noreply, NewState};
handle_info(?LO_CMD_SUBSCRIBE_PARTITIONS, State) ->
  NewState =
    case State#state.is_blocked of
      true ->
        State;
      false ->
        {ok, #state{} = NewState_} = subscribe_partitions(State),
        NewState_
    end,
  _ = send_lo_cmd(?LO_CMD_SUBSCRIBE_PARTITIONS, ?RESUBSCRIBE_DELAY),
  {noreply, NewState};
handle_info(Info, State) ->
  log(State, info, "discarded message:~p", [Info]),
  {noreply, State}.

handle_call({get_committed_offsets, TopicPartitions}, _From,
            #state{ groupId   = GroupId
                  , cb_module = CbModule
                  , cb_state  = CbState
                  } = State) ->
  case CbModule:get_committed_offsets(GroupId, TopicPartitions, CbState) of
    {ok, Result, NewCbState} ->
      NewState = State#state{cb_state = NewCbState},
      {reply, Result, NewState};
    Unknown ->
      erlang:error({bad_return_value,
                    {CbModule, get_committed_offsets, Unknown}})
  end;
handle_call({assign_partitions, MemberMetadataList, TopicPartitions}, _From,
            #state{ cb_module = CbModule
                  , cb_state  = CbState
                  } = State) ->
  Result = CbModule:assign_partitions(MemberMetadataList,
                                      TopicPartitions, CbState),
  {reply, Result, State};
handle_call(unsubscribe_all_partitions, _From,
            #state{ consumers = Consumers
                  } = State) ->
  lists:foreach(
    fun(#consumer{ consumer_pid  = ConsumerPid
                 , consumer_mref = ConsumerMref
                 }) ->
      _ = brod:unsubscribe(ConsumerPid, self()),
      _ = erlang:demonitor(ConsumerMref, [flush])
    end, Consumers),
  {reply, ok, State#state{ consumers        = []
                         , is_blocked       = true
                         , pending_ack      = ?undef
                         , pending_messages = []
                         }};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({ack, Topic, Partition, Offset}, State) ->
  AckRef = {Topic, Partition, Offset},
  {ok, NewState} = handle_ack(AckRef, State),
  {noreply, NewState};
handle_cast(commit_offsets, State) ->
  ok = brod_group_coordinator:commit_offsets(State#state.coordinator),
  {noreply, State};
handle_cast({new_assignments, MemberId, GenerationId, Assignments},
            #state{ client          = Client
                  , consumer_config = ConsumerConfig
                  } = State) ->
  AllTopics =
    lists:map(fun(#brod_received_assignment{topic = Topic}) ->
                Topic
              end, Assignments),
  lists:foreach(
    fun(Topic) ->
      ok = brod:start_consumer(Client, Topic, ConsumerConfig)
    end, lists:usort(AllTopics)),
  Consumers =
    [ #consumer{ topic_partition = {Topic, Partition}
               , consumer_pid    = ?undef
               , begin_offset    = BeginOffset
               , acked_offset    = ?undef
               }
    || #brod_received_assignment{ topic        = Topic
                                , partition    = Partition
                                , begin_offset = BeginOffset
                                } <- Assignments
    ],
  _ = send_lo_cmd(?LO_CMD_SUBSCRIBE_PARTITIONS),
  NewState = State#state{ consumers    = Consumers
                        , is_blocked   = false
                        , memberId     = MemberId
                        , generationId = GenerationId
                        },
  {noreply, NewState};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, #state{}) ->
  ok.

%%%_* Internal Functions =======================================================

-spec maybe_process_message(#state{}) -> {ok, #state{}}.
maybe_process_message(#state{ pending_ack      = ?undef
                            , pending_messages = [{AckRef, F} | Rest]
                            , cb_state         = CbState
                            } = State) ->
  %% process new message only when there is no pending ack
  {AckNow, NewCbState} = F(CbState),
  NewState =
    State#state{ pending_ack      = AckRef
               , pending_messages = Rest
               , cb_state         = NewCbState
               },
  case AckNow of
    true  -> handle_ack(AckRef, NewState);
    false -> {ok, NewState}
  end;
maybe_process_message(State) ->
  {ok, State}.

-spec handle_ack(ack_ref(), #state{}) -> {ok, #state{}}.
handle_ack(AckRef, #state{ pending_ack      = AckRef
                         , pending_messages = Messages
                         , generationId     = GenerationId
                         , consumers        = Consumers
                         , coordinator      = Coordinator
                         } = State0) ->
  {Topic, Partition, Offset} = AckRef,
  State1 =
    case lists:keyfind({Topic, Partition},
                       #consumer.topic_partition, Consumers) of
      #consumer{consumer_pid = ConsumerPid} = Consumer ->
        ok = brod:consume_ack(ConsumerPid, Offset),
        ok = brod_group_coordinator:ack(Coordinator, GenerationId,
                                        Topic, Partition, Offset),
        NewConsumer = Consumer#consumer{acked_offset = Offset},
        NewConsumers = lists:keyreplace({Topic, Partition},
                                        #consumer.topic_partition,
                                        Consumers, NewConsumer),
      State0#state{consumers = NewConsumers};
    false ->
      %% stale ack, ignore.
      State0
    end,
  Messages =/= [] andalso send_lo_cmd(?LO_CMD_PROCESS_MESSAGE),
  State = State1#state{pending_ack = ?undef},
  {ok, State};
handle_ack(_AckRef, State) ->
  %% stale ack, ignore.
  {ok, State}.

send_lo_cmd(CMD) -> send_lo_cmd(CMD, 0).

send_lo_cmd(CMD, 0)       -> self() ! CMD;
send_lo_cmd(CMD, DelayMS) -> erlang:send_after(DelayMS, self(), CMD).

subscribe_partitions(#state{ client    = Client
                           , consumers = Consumers0
                           } = State) ->
  Consumers =
    lists:map(fun(C) -> subscribe_partition(Client, C) end, Consumers0),
  {ok, State#state{consumers = Consumers}}.

subscribe_partition(Client, Consumer) ->
  #consumer{ topic_partition = {Topic, Partition}
           , consumer_pid    = Pid
           , begin_offset    = BeginOffset0
           , acked_offset    = AckedOffset
           } = Consumer,
  case is_pid(Pid) andalso is_process_alive(Pid) of
    true ->
      Consumer;
    false ->
      %% fetch from the last committed offset + 1
      %% otherwise fetch from the begin offset
      BeginOffset = case AckedOffset of
                      ?undef        -> BeginOffset0;
                      N when N >= 0 -> N + 1
                    end,
      Options =
        case BeginOffset =:= ?undef of
          true  -> []; %% fetch from 'begin_offset' in consumer config
          false -> [{begin_offset, BeginOffset}]
        end,
      case brod:subscribe(Client, self(), Topic, Partition, Options) of
        {ok, ConsumerPid} ->
          Mref = erlang:monitor(process, ConsumerPid),
          Consumer#consumer{ consumer_pid  = ConsumerPid
                           , consumer_mref = Mref
                           };
        {error, Reason} ->
          Consumer#consumer{ consumer_pid  = ?DOWN(Reason)
                           , consumer_mref = ?undef
                           }
      end
  end.

log(#state{ groupId  = GroupId
          , memberId = MemberId
          , generationId = GenerationId
          }, Level, Fmt, Args) ->
  brod_utils:log(
    Level,
    "group subscriber (groupId=~s,memberId=~s,generation=~p,pid=~p):\n" ++ Fmt,
    [GroupId, MemberId, GenerationId, self() | Args]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
