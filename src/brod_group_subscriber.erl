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

%%%=============================================================================
%%% @doc
%%% A group subscriber is a gen_server which subscribes to partition consumers
%%% (poller) and calls the user-defined callback functions for message
%%% processing.
%%%
%%% An overview of what it does behind the scene:
%%% <ol>
%%% <li>Start a consumer group coordinator to manage the consumer group states,
%%%     see {@link brod_group_coordinator:start_link/6}</li>
%%% <li>Start (if not already started) topic-consumers (pollers) and subscribe
%%%     to the partition workers when group assignment is received from the
%%      group leader, see {@link brod:start_consumer/3}</li>
%%% <li>Call `CallbackModule:handle_message/4' when messages are received from
%%%     the partition consumers.</li>
%%% <li>Send acknowledged offsets to group coordinator which will be committed
%%%     to kafka periodically.</li>
%%% </ol>
%%%
%%% Callbacks are documented in the source code of this module.
%%% @end
%%%=============================================================================

-module(brod_group_subscriber).

-behaviour(gen_server).
-behaviour(brod_group_member).

-export([ ack/4
        , ack/5
        , commit/1
        , commit/4
        , start_link/7
        , start_link/8
        , stop/1
        ]).

%% callbacks for brod_group_coordinator
-export([ get_committed_offsets/2
        , assignments_received/4
        , assignments_revoked/1
        , assign_partitions/3
        , user_data/1
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
-type member_id() :: brod:group_member_id().

%% Initialize the callback module s state.
-callback init(brod:group_id(), term()) -> {ok, cb_state()}.

%% @doc Handle a message. Return one of:
%%
%% `{ok, NewCallbackState}':
%%   The subscriber has received the message for processing async-ly.
%%   It should call {@link brod_group_subscriber:ack/4} to acknowledge later.
%%
%% `{ok, ack, NewCallbackState}':
%%   The subscriber has completed processing the message.
%%
%% `{ok, ack_no_commit, NewCallbackState}':
%%   The subscriber has completed processing the message, but it
%%   is not ready to commit offset yet. It should call
%%   {@link brod_group_subscriber:commit/4} later.
%%
%% While this callback function is being evaluated, the fetch-ahead
%% partition-consumers are fetching more messages behind the scene
%% unless prefetch_count and prefetch_bytes are set to 0 in consumer config.
%% @end
-callback handle_message(brod:topic(),
                         brod:partition(),
                         brod:message() | brod:message_set(),
                         cb_state()) -> {ok, cb_state()} |
                                        {ok, ack, cb_state()} |
                                        {ok, ack_no_commit, cb_state()}.

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
%-callback get_committed_offsets(brod:group_id(),
%                                [{brod:topic(), brod:partition()}],
%                                cb_state()) ->  {ok,
%                                                 [{{brod:topic(),
%                                                    brod:partition()},
%                                                   brod:offset()}],
%                                                 cb_state()}.
%
%% This function is called only when 'partition_assignment_strategy' is
%% 'callback_implemented' in group config.
%% The first element in the group member list is ensured to be the group leader.
%
% commented out as it's an optional callback
%-callback assign_partitions([brod:group_member()],
%                            [{brod:topic(), brod:partition()}],
%                            cb_state()) -> [{brod:group_member_id(),
%                                             [brod:partition_assignment()]}].

-define(DOWN(Reason), {down, brod_utils:os_time_utc_str(), Reason}).

-record(consumer,
        { topic_partition :: {brod:topic(), brod:partition()}
        , consumer_pid    :: ?undef                  %% initial state
                           | pid()                   %% normal state
                           | {down, string(), any()} %% consumer restarting
        , consumer_mref   :: ?undef | reference()
        , begin_offset    :: ?undef | brod:offset()
        , acked_offset    :: ?undef | brod:offset()
        , last_offset     :: ?undef | brod:offset()
        }).

-type consumer() :: #consumer{}.

-type ack_ref() :: {brod:topic(), brod:partition(), brod:offset()}.

-record(state,
        { client             :: brod:client()
        , client_mref        :: reference()
        , groupId            :: brod:group_id()
        , memberId           :: ?undef | member_id()
        , generationId       :: ?undef | brod:group_generation_id()
        , coordinator        :: pid()
        , consumers = []     :: [consumer()]
        , consumer_config    :: brod:consumer_config()
        , is_blocked = false :: boolean()
        , subscribe_tref     :: ?undef | reference()
        , cb_module          :: module()
        , cb_state           :: cb_state()
        , message_type       :: message | message_set
        }).

-type state() :: #state{}.

%% delay 2 seconds retry the failed subscription to partition consumer process
-define(RESUBSCRIBE_DELAY, 2000).

-define(LO_CMD_SUBSCRIBE_PARTITIONS, '$subscribe_partitions').

%%%_* APIs =====================================================================

%% @equiv start_link(Client, GroupId, Topics, GroupConfig, ConsumerConfig,
%%             message, CbModule, CbInitArg)
-spec start_link(brod:client(), brod:group_id(), [brod:topic()],
                 brod:group_config(), brod:consumer_config(),
                 module(), term()) -> {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, GroupConfig,
           ConsumerConfig, CbModule, CbInitArg) ->
  start_link(Client, GroupId, Topics, GroupConfig, ConsumerConfig,
             message, CbModule, CbInitArg).


%% @doc Start (link) a group subscriber.
%%
%% `Client': Client ID (or pid, but not recommended) of the brod client.
%%
%% `GroupId': Consumer group ID which should be unique per kafka cluster
%%
%% `Topics': Predefined set of topic names to join the group.
%%
%%   NOTE: The group leader member will collect topics from all members and
%%         assign all collected topic-partitions to members in the group.
%%         i.e. members can join with arbitrary set of topics.
%%
%% `GroupConfig': For group coordinator, see
%%    {@link brod_group_coordinator:start_link/6}
%%
%% `ConsumerConfig': For partition consumer, see
%% {@link brod_consumer:start_link/4}
%%
%% `MessageType':
%%   The type of message that is going to be handled by the callback
%%   module. Can be either `message' or `message_set'.
%%
%% `CbModule':
%%   Callback module which should have the callback functions
%%   implemented for message processing.
%%
%% `CbInitArg':
%%   The term() that is going to be passed to `CbModule:init/2' as a
%%   second argument when initializing the subscriber.
%% @end
-spec start_link(brod:client(), brod:group_id(), [brod:topic()],
                 brod:group_config(), brod:consumer_config(),
                 message | message_set,
                 module(), term()) -> {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, GroupConfig,
           ConsumerConfig, MessageType, CbModule, CbInitArg) ->
  Args = {Client, GroupId, Topics, GroupConfig,
          ConsumerConfig, MessageType, CbModule, CbInitArg},
  gen_server:start_link(?MODULE, Args, []).

%% @doc Stop group subscriber, wait for pid `DOWN' before return.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Acknowledge and commit an offset.
%% The subscriber may ack a later (greater) offset which will be considered
%% as multi-acking the earlier (smaller) offsets. This also means that
%% disordered acks may overwrite offset commits and lead to unnecessary
%% message re-delivery in case of restart.
%% @end
-spec ack(pid(), brod:topic(), brod:partition(), brod:offset()) -> ok.
ack(Pid, Topic, Partition, Offset) ->
  ack(Pid, Topic, Partition, Offset, true).

%% @doc Acknowledge an offset.
%% This call may or may not commit group subscriber offset depending on
%% the value of `Commit' argument
%% @end
-spec ack(pid(), brod:topic(), brod:partition(), brod:offset(), boolean()) ->
             ok.
ack(Pid, Topic, Partition, Offset, Commit) ->
  gen_server:cast(Pid, {ack, Topic, Partition, Offset, Commit}).

%% @doc Commit all acked offsets. NOTE: This is an async call.
-spec commit(pid()) -> ok.
commit(Pid) ->
  gen_server:cast(Pid, commit_offsets).

%% @doc Commit offset for a topic. This is an asynchronous call
-spec commit(pid(), brod:topic(), brod:partition(), brod:offset()) -> ok.
commit(Pid, Topic, Partition, Offset) ->
  gen_server:cast(Pid, {commit_offset, Topic, Partition, Offset}).

user_data(_Pid) -> <<>>.

%%%_* APIs for group coordinator ===============================================

%% @doc Called by group coordinator when there is new assignment received.
-spec assignments_received(pid(), member_id(), integer(),
                           brod:received_assignments()) -> ok.
assignments_received(Pid, MemberId, GenerationId, TopicAssignments) ->
  gen_server:cast(Pid, {new_assignments, MemberId,
                        GenerationId, TopicAssignments}).

%% @doc Called by group coordinator before re-joining the consumer group.
-spec assignments_revoked(pid()) -> ok.
assignments_revoked(Pid) ->
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

%% @doc This function is called only when `partition_assignment_strategy'
%% is set for `callback_implemented' in group config.
%% @end
-spec assign_partitions(pid(), [brod:group_member()],
                        [{brod:topic(), brod:partition()}]) ->
        [{member_id(), [brod:partition_assignment()]}].
assign_partitions(Pid, Members, TopicPartitionList) ->
  Call = {assign_partitions, Members, TopicPartitionList},
  gen_server:call(Pid, Call, infinity).

%% @doc Called by group coordinator when initializing the assignments
%% for subscriber.
%%
%% NOTE: This function is called only when `offset_commit_policy' is set to
%%       `consumer_managed' in group config.
%%
%% NOTE: The committed offsets should be the offsets for successfully processed
%%       (acknowledged) messages, not the `begin_offset' to start fetching from.
%% @end
-spec get_committed_offsets(pid(), [{brod:topic(), brod:partition()}]) ->
        {ok, [{{brod:topic(), brod:partition()}, brod:offset()}]}.
get_committed_offsets(Pid, TopicPartitions) ->
  gen_server:call(Pid, {get_committed_offsets, TopicPartitions}, infinity).

%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Topics, GroupConfig,
      ConsumerConfig, MessageType, CbModule, CbInitArg}) ->
  ok = brod_utils:assert_client(Client),
  ok = brod_utils:assert_group_id(GroupId),
  ok = brod_utils:assert_topics(Topics),
  {ok, CbState} = CbModule:init(GroupId, CbInitArg),
  {ok, Pid} = brod_group_coordinator:start_link(Client, GroupId, Topics,
                                                GroupConfig, ?MODULE, self()),
  State = #state{ client          = Client
                , client_mref     = erlang:monitor(process, Client)
                , groupId         = GroupId
                , coordinator     = Pid
                , consumer_config = ConsumerConfig
                , cb_module       = CbModule
                , cb_state        = CbState
                , message_type    = MessageType
                },
  {ok, State}.

handle_info({_ConsumerPid, #kafka_message_set{} = MsgSet}, State0) ->
  State = handle_consumer_delivery(MsgSet, State0),
  {noreply, State};
handle_info({'DOWN', Mref, process, _Pid, _Reason},
            #state{client_mref = Mref} = State) ->
  %% restart, my supervisor should restart me
  %% brod_client DOWN reason is discarded as it should have logged
  %% in its crash log
  {stop, client_down, State};
handle_info({'DOWN', _Mref, process, Pid, Reason},
            #state{consumers = Consumers} = State) ->
  case get_consumer(Pid, Consumers) of
    #consumer{} = Consumer ->
      NewConsumer = Consumer#consumer{ consumer_pid  = ?DOWN(Reason)
                                     , consumer_mref = ?undef
                                     },
      NewConsumers = put_consumer(NewConsumer, Consumers),
      NewState = State#state{consumers = NewConsumers},
      {noreply, NewState};
    false ->
      {noreply, State}
  end;
handle_info(?LO_CMD_SUBSCRIBE_PARTITIONS, State) ->
  NewState =
    case State#state.is_blocked of
      true ->
        State;
      false ->
        {ok, #state{} = St} = subscribe_partitions(State),
        St
    end,
  Tref = start_subscribe_timer(?undef, ?RESUBSCRIBE_DELAY),
  {noreply, NewState#state{subscribe_tref = Tref}};
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
      {reply, {ok, Result}, NewState};
    Unknown ->
      erlang:error({bad_return_value,
                    {CbModule, get_committed_offsets, Unknown}})
  end;
handle_call({assign_partitions, Members, TopicPartitions}, _From,
            #state{ cb_module = CbModule
                  , cb_state  = CbState
                  } = State) ->
  case CbModule:assign_partitions(Members, TopicPartitions, CbState) of
    {NewCbState, Result} ->
      {reply, Result, State#state{ cb_state = NewCbState }};
    %% Returning an updated cb_state is optional and clients that implemented
    %% brod prior to version 3.7.1 need this backwards compatibly case clause
    Result when is_list(Result) ->
      {reply, Result, State}
  end;
handle_call(unsubscribe_all_partitions, _From,
            #state{ consumers = Consumers
                  } = State) ->
  lists:foreach(
    fun(#consumer{ consumer_pid  = ConsumerPid
                 , consumer_mref = ConsumerMref
                 }) ->
        case is_pid(ConsumerPid) of
          true ->
            _ = brod:unsubscribe(ConsumerPid, self()),
            _ = erlang:demonitor(ConsumerMref, [flush]);
          false ->
            ok
        end
    end, Consumers),
  {reply, ok, State#state{ consumers  = []
                         , is_blocked = true
                         }};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({ack, Topic, Partition, Offset, Commit}, State) ->
  AckRef = {Topic, Partition, Offset},
  NewState = handle_ack(AckRef, State, Commit),
  {noreply, NewState};
handle_cast(commit_offsets, State) ->
  ok = brod_group_coordinator:commit_offsets(State#state.coordinator),
  {noreply, State};
handle_cast({commit_offset, Topic, Partition, Offset}, State) ->
  #state{ coordinator  = Coordinator
        , generationId = GenerationId
        } = State,
  do_commit_ack(Coordinator, GenerationId, Topic, Partition, Offset),
  {noreply, State};
handle_cast({new_assignments, MemberId, GenerationId, Assignments},
            #state{ client          = Client
                  , consumer_config = ConsumerConfig
                  , subscribe_tref  = Tref
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
  NewState = State#state{ consumers      = Consumers
                        , is_blocked     = false
                        , memberId       = MemberId
                        , generationId   = GenerationId
                        , subscribe_tref = start_subscribe_timer(Tref, 0)
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

handle_consumer_delivery(#kafka_message_set{ topic     = Topic
                                           , partition = Partition
                                           , messages  = Messages
                                           } = MsgSet,
                         #state{ message_type = MsgType
                               , consumers = Consumers0
                               } = State0) ->
  case get_consumer({Topic, Partition}, Consumers0) of
    #consumer{} = C ->
      Consumers = update_last_offset(Messages, C, Consumers0),
      State = State0#state{consumers = Consumers},
      case MsgType of
        message -> handle_messages(Topic, Partition, Messages, State);
        message_set -> handle_message_set(MsgSet, State)
      end;
    false ->
      State0
  end.

update_last_offset(Messages, Consumer0, Consumers) ->
  %% brod_consumer never delivers empty message set, lists:last is safe
  #kafka_message{offset = LastOffset} = lists:last(Messages),
  Consumer = Consumer0#consumer{last_offset = LastOffset},
  put_consumer(Consumer, Consumers).

-spec start_subscribe_timer(?undef | reference(), timeout()) -> reference().
start_subscribe_timer(?undef, Delay) ->
  erlang:send_after(Delay, self(), ?LO_CMD_SUBSCRIBE_PARTITIONS);
start_subscribe_timer(Ref, _Delay) when is_reference(Ref) ->
  %% The old timer is not expired, keep waiting
  %% A bit delay on subscribing to brod_consumer is fine
  Ref.

handle_message_set(MessageSet, State) ->
  #kafka_message_set{ topic     = Topic
                    , partition = Partition
                    , messages  = Messages
                    } = MessageSet,
  #state{cb_module = CbModule, cb_state = CbState} = State,
  {AckNow, CommitNow, NewCbState} =
    case CbModule:handle_message(Topic, Partition, MessageSet, CbState) of
      {ok, NewCbState_} ->
        {false, false, NewCbState_};
      {ok, ack, NewCbState_} ->
        {true, true, NewCbState_};
      {ok, ack_no_commit, NewCbState_} ->
        {true, false, NewCbState_};
      Unknown ->
        erlang:error({bad_return_value,
                      {CbModule, handle_message, Unknown}})
    end,
  State1 = State#state{cb_state = NewCbState},
  case AckNow of
    true  ->
      LastMessage = lists:last(Messages),
      LastOffset  = LastMessage#kafka_message.offset,
      AckRef      = {Topic, Partition, LastOffset},
      handle_ack(AckRef, State1, CommitNow);
    false -> State1
  end.

handle_messages(_Topic, _Partition, [], State) ->
  State;
handle_messages(Topic, Partition, [Msg | Rest], State) ->
  #kafka_message{offset = Offset} = Msg,
  #state{cb_module = CbModule, cb_state = CbState} = State,
  AckRef = {Topic, Partition, Offset},
  {AckNow, CommitNow, NewCbState} =
    case CbModule:handle_message(Topic, Partition, Msg, CbState) of
      {ok, NewCbState_} ->
        {false, false, NewCbState_};
      {ok, ack, NewCbState_} ->
        {true, true, NewCbState_};
      {ok, ack_no_commit, NewCbState_} ->
        {true, false, NewCbState_};
      Unknown ->
        erlang:error({bad_return_value,
                     {CbModule, handle_message, Unknown}})
    end,
  State1 = State#state{cb_state = NewCbState},
  NewState =
    case AckNow of
      true  -> handle_ack(AckRef, State1, CommitNow);
      false -> State1
    end,
  handle_messages(Topic, Partition, Rest, NewState).

-spec handle_ack(ack_ref(), state(), boolean()) -> state().
handle_ack(AckRef, #state{ generationId = GenerationId
                         , consumers    = Consumers
                         , coordinator  = Coordinator
                         } = State, CommitNow) ->
  {Topic, Partition, Offset} = AckRef,
  case get_consumer({Topic, Partition}, Consumers) of
    #consumer{consumer_pid = ConsumerPid} = Consumer when CommitNow ->
      ok = consume_ack(ConsumerPid, Offset),
      ok = do_commit_ack(Coordinator, GenerationId, Topic, Partition, Offset),
      NewConsumer = Consumer#consumer{acked_offset = Offset},
      NewConsumers = put_consumer(NewConsumer, Consumers),
      State#state{consumers = NewConsumers};
    #consumer{consumer_pid = ConsumerPid} ->
      ok = consume_ack(ConsumerPid, Offset),
      State;
    false ->
      %% Stale async-ack, discard.
      State
  end.

%% Tell consumer process to fetch more (if pre-fetch count/byte limit allows).
consume_ack(Pid, Offset) ->
  is_pid(Pid) andalso brod:consume_ack(Pid, Offset),
  ok.

%% Send an async message to group coordinator for offset commit.
do_commit_ack(Pid, GenerationId, Topic, Partition, Offset) ->
  ok = brod_group_coordinator:ack(Pid, GenerationId, Topic, Partition, Offset).

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
           , last_offset     = LastOffset
           } = Consumer,
  case brod_utils:is_pid_alive(Pid) of
    true ->
      Consumer;
    false when AckedOffset =/= LastOffset andalso LastOffset =/= ?undef ->
      %% The last fetched offset is not yet acked,
      %% do not re-subscribe now to keep it simple and slow.
      %% Otherwise if we subscribe with {begin_offset, LastOffset + 1}
      %% we may exceed pre-fetch window size.
      Consumer;
    false ->
      %% fetch from the last acked offset + 1
      %% otherwise fetch from the assigned begin_offset
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
  ?BROD_LOG(
     Level,
     "group subscriber (groupId=~s,memberId=~s,generation=~p,pid=~p):\n" ++ Fmt,
     [GroupId, MemberId, GenerationId, self() | Args]).

get_consumer(Pid, Consumers) when is_pid(Pid) ->
  lists:keyfind(Pid, #consumer.consumer_pid, Consumers);
get_consumer({_, _} = TP, Consumers) ->
  lists:keyfind(TP, #consumer.topic_partition, Consumers).

put_consumer(#consumer{topic_partition = TP} = C, Consumers) ->
  lists:keyreplace(TP, #consumer.topic_partition, Consumers, C).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
