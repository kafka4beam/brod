%%%
%%%   Copyright (c) 2016-2018 Klarna Bank AB (publ)
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
%%% A topic subscriber is a gen_server which subscribes to all or a given set
%%% of partition consumers (pollers) of a given topic and calls the user-defined
%%% callback functions for message processing.
%%% @end
%%%=============================================================================

-module(brod_topic_subscriber).
-behaviour(gen_server).

-export([ ack/3
        , start_link/6
        , start_link/7
        , start_link/8
        , stop/1
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
-type cb_ret() :: {ok, cb_state()} | {ok, ack, cb_state()}.
-type cb_fun() :: fun(( brod:partition()
                      , brod:message() | brod:message_set()
                      , cb_state()) -> cb_ret()).
-type committed_offsets() :: [{brod:partition(), brod:offset()}].
-type ack_ref()  :: {brod:partition(), brod:offset()}.

%%%_* behaviour callbacks ======================================================

%% Initialize the callback modules state.
%% Return {ok, CommittedOffsets, CbState} where CommitedOffset is
%% the 'last seen' before start/restart offsets of each topic in a tuple list
%% The offset+1 of each partition will be used as the start point when fetching
%% messages from kafka.
%% OBS: If there is no offset committed before for certain (or all) partitions
%%      e.g. CommittedOffsets = [], the consumer will use 'latest' by default,
%%      or 'begin_offset' in consumer config (if found) to start fetching.
%% CbState is the user's looping state for message processing.
-callback init(brod:topic(), term()) -> {ok, committed_offsets(), cb_state()}.

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
%%       partition-consumers are polling for more messages behind the scene
%%       unless prefetch_count and prefetch_bytes are set to 0 in consumer
%%       config.
-callback handle_message(brod:partition(),
                         brod:message() | brod:message_set(),
                         cb_state()) -> cb_ret().

%%%_* Types and macros =========================================================

-record(consumer,
        { partition     :: brod:partition()
        , consumer_pid  :: ?undef | pid() | {down, string(), any()}
        , consumer_mref :: ?undef | reference()
        , acked_offset  :: ?undef | brod:offset()
        , last_offset   :: ?undef | brod:offset() % last offset in last fetch
        }).

-type consumer() :: #consumer{}.

-record(state,
        { client         :: brod:client()
        , client_mref    :: reference()
        , topic          :: brod:topic()
        , consumers = [] :: [consumer()]
        , cb_fun         :: cb_fun()
        , cb_state       :: cb_state()
        , message_type   :: message | message_set
        }).

-type state() :: #state{}.

%% delay 2 seconds retry the failed subscription to partiton consumer process
-define(RESUBSCRIBE_DELAY, 2000).

-define(LO_CMD_START_CONSUMER(ConsumerConfig, CommittedOffsets, Partitions),
        {'$start_consumer', ConsumerConfig, CommittedOffsets, Partitions}).
-define(LO_CMD_SUBSCRIBE_PARTITIONS, '$subscribe_partitions').

-define(DOWN(Reason), {down, brod_utils:os_time_utc_str(), Reason}).

%%%_* APIs =====================================================================

%% @doc Start (link) a topic subscriber which receives and processes the
%% messages from the given partition set. Use atom 'all' to subscribe to all
%% partitions. Messages are handled by calling CbModule:handle_message
-spec start_link(brod:client(), brod:topic(), all | [brod:partition()],
                 brod:consumer_config(), module(), term()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, Topic, Partitions, ConsumerConfig,
           CbModule, CbInitArg) ->
  Args = {Client, Topic, Partitions, ConsumerConfig,
          message, CbModule, CbInitArg},
  gen_server:start_link(?MODULE, Args, []).

%% @doc Start (link) a topic subscriber which receives and processes the
%% messages or message sets from the given partition set. Use atom 'all'
%% to subscribe to all partitions. Messages are handled by calling
%% CbModule:handle_message
-spec start_link(brod:client(), brod:topic(), all | [brod:partition()],
                 brod:consumer_config(), message | message_set,
                 module(), term()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, Topic, Partitions, ConsumerConfig,
           MessageType, CbModule, CbInitArg) ->
  Args = {Client, Topic, Partitions, ConsumerConfig,
          MessageType, CbModule, CbInitArg},
  gen_server:start_link(?MODULE, Args, []).

%% @doc Start (link) a topic subscriber which receives and processes the
%% messages from the given partition set. Use atom 'all' to subscribe to all
%% partitions. Messages are handled by calling the callback function.
%%
%% NOTE: CommittedOffsets are the offsets for the messages that are successfully
%%       processed (acknoledged), not the begin-offset ot start fetching from.
-spec start_link(brod:client(), brod:topic(), all | [brod:partition()],
                 brod:consumer_config(), committed_offsets(),
                 message | message_set, cb_fun(), cb_state()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, Topic, Partitions, ConsumerConfig,
           CommittedOffsets, MessageType, CbFun, CbInitialState) ->
  Args = {Client, Topic, Partitions, ConsumerConfig,
          CommittedOffsets, MessageType, CbFun, CbInitialState},
  gen_server:start_link(?MODULE, Args, []).


%% @doc Stop topic subscriber.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Acknowledge that message has been sucessfully consumed.
-spec ack(pid(), brod:partition(), brod:offset()) -> ok.
ack(Pid, Partition, Offset) ->
  gen_server:cast(Pid, {ack, Partition, Offset}).

%%%_* gen_server callbacks =====================================================

init({Client, Topic, Partitions, ConsumerConfig,
      MessageType, CbModule, CbInitArg}) ->
  {ok, CommittedOffsets, CbState} = CbModule:init(Topic, CbInitArg),
  CbFun = fun(Partition, Msg, CbStateIn) ->
                CbModule:handle_message(Partition, Msg, CbStateIn)
          end,
  init({Client, Topic, Partitions, ConsumerConfig,
        CommittedOffsets, MessageType, CbFun, CbState});
init({Client, Topic, Partitions, ConsumerConfig,
      CommittedOffsets, MessageType, CbFun, CbState}) ->
  ok = brod_utils:assert_client(Client),
  ok = brod_utils:assert_topic(Topic),
  self() ! ?LO_CMD_START_CONSUMER(ConsumerConfig, CommittedOffsets, Partitions),
  State =
    #state{ client       = Client
          , client_mref  = erlang:monitor(process, Client)
          , topic        = Topic
          , cb_fun       = CbFun
          , cb_state     = CbState
          , message_type = MessageType
          },
  {ok, State}.

handle_info({_ConsumerPid, #kafka_message_set{} = MsgSet}, State0) ->
  State = handle_consumer_delivery(MsgSet, State0),
  {noreply, State};
handle_info(?LO_CMD_START_CONSUMER(ConsumerConfig, CommittedOffsets,
                                   Partitions0),
             #state{ client = Client
                   , topic  = Topic
                   } = State) ->
  ok = brod:start_consumer(Client, Topic, ConsumerConfig),
  {ok, PartitionsCount} = brod:get_partitions_count(Client, Topic),
  AllPartitions = lists:seq(0, PartitionsCount - 1),
  Partitions =
    case Partitions0 of
      all ->
        AllPartitions;
      L when is_list(L) ->
        PS = lists:usort(L),
        case lists:min(PS) >= 0 andalso lists:max(PS) < PartitionsCount of
          true  -> PS;
          false -> erlang:error({bad_partitions, Partitions0, PartitionsCount})
        end
    end,
  Consumers =
    lists:map(
      fun(Partition) ->
        AckedOffset = case lists:keyfind(Partition, 1, CommittedOffsets) of
                        {Partition, Offset} -> Offset;
                        false               -> ?undef
                      end,
        #consumer{ partition    = Partition
                 , acked_offset = AckedOffset
                 }
      end, Partitions),
  NewState = State#state{consumers = Consumers},
  _ = send_lo_cmd(?LO_CMD_SUBSCRIBE_PARTITIONS),
  {noreply, NewState};
handle_info(?LO_CMD_SUBSCRIBE_PARTITIONS, State) ->
  {ok, #state{} = NewState} = subscribe_partitions(State),
  _ = send_lo_cmd(?LO_CMD_SUBSCRIBE_PARTITIONS, ?RESUBSCRIBE_DELAY),
  {noreply, NewState};
handle_info({'DOWN', Mref, process, _Pid, _Reason},
            #state{client_mref = Mref} = State) ->
  %% restart, my supervisor should restart me
  %% brod_client DOWN reason is discarded as it should have logged
  %% in its crash log
  {stop, client_down, State};
handle_info({'DOWN', _Mref, process, Pid, Reason},
            #state{consumers = Consumers} = State) ->
  case get_consumer(Pid, Consumers) of
    #consumer{} = C ->
      Consumer = C#consumer{ consumer_pid  = ?DOWN(Reason)
                           , consumer_mref = ?undef
                           },
      NewConsumers = put_consumer(Consumer, Consumers),
      NewState = State#state{consumers = NewConsumers},
      {noreply, NewState};
    false ->
      %% not a consumer pid
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({ack, Partition, Offset}, State) ->
  AckRef = {Partition, Offset},
  NewState = handle_ack(AckRef, State),
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
                         #state{ topic = Topic
                               , message_type = MsgType
                               } = State0) ->
  State = update_last_offset(Partition, Messages, State0),
  case MsgType of
    message -> handle_messages(Partition, Messages, State);
    message_set -> handle_message_set(MsgSet, State)
  end.

update_last_offset(Partition, Messages,
                   #state{consumers = Consumers} = State) ->
  %% brod_consumer never delivers empty message set, lists:last is safe
  #kafka_message{offset = LastOffset} = lists:last(Messages),
  C = get_consumer(Partition, Consumers),
  Consumer = C#consumer{last_offset = LastOffset},
  State#state{consumers = put_consumer(Consumer, Consumers)}.

subscribe_partitions(#state{ client    = Client
                           , topic     = Topic
                           , consumers = Consumers0
                           } = State) ->
  Consumers =
    lists:map(fun(C) -> subscribe_partition(Client, Topic, C) end, Consumers0),
  {ok, State#state{consumers = Consumers}}.

subscribe_partition(Client, Topic, Consumer) ->
  #consumer{ partition    = Partition
           , consumer_pid = Pid
           , acked_offset = AckedOffset
           , last_offset  = LastOffset
           } = Consumer,
  case brod_utils:is_pid_alive(Pid) of
    true ->
      %% already subscribed
      Consumer;
    false when AckedOffset =/= LastOffset andalso LastOffset =/= ?undef ->
      %% The last fetched offset is not yet acked,
      %% do not re-subscribe now to keep it simple and slow.
      %% Otherwise if we subscribe with {begin_offset, LastOffset + 1}
      %% we may exceed pre-fetch window size.
      Consumer;
    false ->
      Options =
        case AckedOffset =:= ?undef of
          true ->
            %% the default or configured 'begin_offset' will be used
            [];
          false ->
            StartOffset = AckedOffset + 1,
            StartOffset >= 0 orelse erlang:error({invalid_offset, AckedOffset}),
            [{begin_offset, StartOffset}]
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

handle_message_set(MessageSet, State) ->
  #kafka_message_set{ partition = Partition
                    , messages  = Messages
                    } = MessageSet,
  #state{cb_fun = CbFun, cb_state = CbState} = State,
  {AckNow, NewCbState} =
    case CbFun(Partition, MessageSet, CbState) of
      {ok, NewCbState_} ->
        {false, NewCbState_};
      {ok, ack, NewCbState_} ->
        {true, NewCbState_}
    end,
  State1 = State#state{cb_state = NewCbState},
  case AckNow of
    true  ->
      LastMessage = lists:last(Messages),
      LastOffset  = LastMessage#kafka_message.offset,
      AckRef      = {Partition, LastOffset},
      handle_ack(AckRef, State1);
    false -> State1
  end.

handle_messages(_Partition, [], State) ->
  State;
handle_messages(Partition, [Msg | Rest], State) ->
  #kafka_message{offset = Offset} = Msg,
  #state{cb_fun = CbFun, cb_state = CbState} = State,
  AckRef = {Partition, Offset},
  {AckNow, NewCbState} =
    case CbFun(Partition, Msg, CbState) of
      {ok, NewCbState_} ->
        {false, NewCbState_};
      {ok, ack, NewCbState_} ->
        {true, NewCbState_}
    end,
  State1 = State#state{cb_state = NewCbState},
  NewState =
    case AckNow of
      true  -> handle_ack(AckRef, State1);
      false -> State1
    end,
  handle_messages(Partition, Rest, NewState).

-spec handle_ack(ack_ref(), state()) -> state().
handle_ack(AckRef, #state{consumers = Consumers} = State) ->
  {Partition, Offset} = AckRef,
  #consumer{consumer_pid = Pid} = Consumer = get_consumer(Partition, Consumers),
  ok = consume_ack(Pid, Offset),
  NewConsumer = Consumer#consumer{acked_offset = Offset},
  NewConsumers = put_consumer(NewConsumer, Consumers),
  State#state{consumers = NewConsumers}.

get_consumer(Partition, Consumers) when is_integer(Partition) ->
  lists:keyfind(Partition, #consumer.partition, Consumers);
get_consumer(Pid, Consumers) when is_pid(Pid) ->
  lists:keyfind(Pid, #consumer.consumer_pid, Consumers).

put_consumer(#consumer{partition = P} = Consumer, Consumers) ->
  lists:keyreplace(P, #consumer.partition, Consumers, Consumer).

%% Tell consumer process to fetch more (if pre-fetch count/byte limit allows).
consume_ack(Pid, Offset) ->
  is_pid(Pid) andalso brod:consume_ack(Pid, Offset),
  ok.

send_lo_cmd(CMD) -> send_lo_cmd(CMD, 0).

send_lo_cmd(CMD, 0)       -> self() ! CMD;
send_lo_cmd(CMD, DelayMS) -> erlang:send_after(DelayMS, self(), CMD).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
