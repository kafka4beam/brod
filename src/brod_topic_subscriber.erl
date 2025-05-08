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
%%% A topic subscriber is a gen_server which subscribes to all or a given set
%%% of partition consumers (pollers) of a given topic and calls the user-defined
%%% callback functions for message processing.
%%%
%%% Callbacks are documented in the source code of this module.
%%% @end
%%%=============================================================================

-module(brod_topic_subscriber).
-behaviour(gen_server).

%% API:
-export([ ack/3
        , start_link/1
        , stop/1
        ]).

%% Deprecated APIs kept for backward compatibility:
-export([ start_link/6
        , start_link/7
        , start_link/8
        ]).

%% gen_server callback
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , handle_continue/2
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

-export_type([ cb_ret/0
             , cb_fun/0
             , committed_offsets/0
             , topic_subscriber_config/0
             ]).

-type topic_subscriber_config() ::
        #{ client            := brod:client()
         , topic             := brod:topic()
         , cb_module         := module()
         , init_data         => term()
         , message_type      => message | message_set
         , consumer_config   => brod:consumer_config()
         , partitions        => all | [brod:partition()]
         }.

%%%_* behaviour callbacks ======================================================

%% Initialize the callback modules state.
%% Return `{ok, CommittedOffsets, CbState}' where `CommitedOffset' is
%% the "last seen" before start/restart offsets of each topic in a tuple list
%% The offset+1 of each partition will be used as the start point when fetching
%% messages from kafka.
%%
%% OBS: If there is no offset committed before for certain (or all) partitions
%%      e.g. CommittedOffsets = [], the consumer will use 'latest' by default,
%%      or `begin_offset' in consumer config (if found) to start fetching.
%% CbState is the user's looping state for message processing.
-callback init(brod:topic(), term()) -> {ok, committed_offsets(), cb_state()}.

%% Handle a message. Return one of:
%%
%% `{ok, NewCallbackState}'
%%   The subscriber has received the message for processing async-ly.
%%   It should call brod_topic_subscriber:ack/3 to acknowledge later.
%%
%% `{ok, ack, NewCallbackState}'
%%   The subscriber has completed processing the message
%%
%% NOTE: While this callback function is being evaluated, the fetch-ahead
%%       partition-consumers are polling for more messages behind the scene
%%       unless prefetch_count and prefetch_bytes are set to 0 in consumer
%%       config.
-callback handle_message(brod:partition(),
                         brod:message() | brod:message_set(),
                         cb_state()) -> cb_ret().

%% This callback is called before stopping the subscriber
-callback terminate(_Reason, cb_state()) -> _.

%% This callback is called when the subscriber receives a message unrelated to
%% the subscription.
%% The callback must return `{noreply, NewCallbackState}'.
-callback handle_info(_Msg, cb_state()) -> {noreply, cb_state()}.

-optional_callbacks([terminate/2, handle_info/2]).

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
        , cb_module      :: module()
        , cb_state       :: cb_state()
        , message_type   :: message | message_set
        }).

-type state() :: #state{}.

%% delay 2 seconds retry the failed subscription to partition consumer process
-define(RESUBSCRIBE_DELAY, 2000).

-define(LO_CMD_START_CONSUMER(ConsumerConfig, CommittedOffsets, Partitions),
        {'$start_consumer', ConsumerConfig, CommittedOffsets, Partitions}).
-define(LO_CMD_SUBSCRIBE_PARTITIONS, '$subscribe_partitions').

-define(DOWN(Reason), {down, brod_utils:os_time_utc_str(), Reason}).

%%%_* APIs =====================================================================

%% @equiv start_link(Client, Topic, Partitions, ConsumerConfig,
%%           message, CbModule, CbInitArg)
%%
%% @deprecated Please use {@link start_link/1} instead
-spec start_link(brod:client(), brod:topic(), all | [brod:partition()],
                 brod:consumer_config(), module(), term()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, Topic, Partitions, ConsumerConfig,
           CbModule, CbInitArg) ->
  Args = #{ client            => Client
          , topic             => Topic
          , partitions        => Partitions
          , consumer_config   => ConsumerConfig
          , message_type      => message
          , cb_module         => CbModule
          , init_data         => CbInitArg
          },
  start_link(Args).

%% @doc Start (link) a topic subscriber which receives and processes the
%% messages or message sets from the given partition set. Use atom `all'
%% to subscribe to all partitions. Messages are handled by calling
%% `CbModule:handle_message'
%%
%% @deprecated Please use {@link start_link/1} instead
-spec start_link(brod:client(), brod:topic(), all | [brod:partition()],
                 brod:consumer_config(), message | message_set,
                 module(), term()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, Topic, Partitions, ConsumerConfig,
           MessageType, CbModule, CbInitArg) ->
  Args = #{ client          => Client
          , topic           => Topic
          , partitions      => Partitions
          , consumer_config => ConsumerConfig
          , message_type    => MessageType
          , cb_module       => CbModule
          , init_data       => CbInitArg
          },
  start_link(Args).

%% @doc Start (link) a topic subscriber which receives and processes the
%% messages from the given partition set. Use atom `all' to subscribe to all
%% partitions. Messages are handled by calling the callback function.
%%
%% NOTE: `CommittedOffsets' are the offsets for the messages that have
%%       been successfully processed (acknowledged), not the begin-offset
%%       to start fetching from.
%%
%% @deprecated Please use {@link start_link/1} instead
-spec start_link(brod:client(), brod:topic(), all | [brod:partition()],
                 brod:consumer_config(), committed_offsets(),
                 message | message_set, cb_fun(), cb_state()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, Topic, Partitions, ConsumerConfig,
           CommittedOffsets, MessageType, CbFun, CbInitialState) ->
  InitData = #cbm_init_data{ committed_offsets = CommittedOffsets
                           , cb_fun            = CbFun
                           , cb_data           = CbInitialState
                           },
  Args = #{ client            => Client
          , topic             => Topic
          , partitions        => Partitions
          , consumer_config   => ConsumerConfig
          , message_type      => MessageType
          , cb_module         => brod_topic_subscriber_cb_fun
          , init_data         => InitData
          },
  start_link(Args).

%% @doc Start (link) a topic subscriber which receives and processes the
%% messages from a given partition set.
%%
%% Possible `Config' keys:
%%
%% <ul><li> `client': Client ID (or pid, but not recommended) of the
%% brod client. Mandatory</li>
%%
%% <li>`topic': Topic to consume from. Mandatory</li>
%%
%% <li>`cb_module': Callback module which should have the callback
%% functions implemented for message processing. Mandatory</li>
%%
%% <li>`consumer_config': For partition consumer, {@link
%% brod_consumer:start_link/5}. Optional, defaults to `[]'
%% </li>
%%
%% <li>`message_type': The type of message that is going to be handled
%% by the callback module. Can be either message or message set.
%% Optional, defaults to `message_set'</li>
%%
%% <li>`init_data': The `term()' that is going to be passed to
%% `CbModule:init/2' when initializing the subscriber. Optional,
%% defaults to `undefined'</li>
%%
%% <li>`partitions': List of partitions to consume from, or atom
%% `all'. Optional, defaults to `all'.</li>
%% </ul>
%% @end
-spec start_link(topic_subscriber_config()) ->
        {ok, pid()} | {error, _}.
start_link(Config) ->
  gen_server:start_link(?MODULE, Config, []).

%% @doc Stop topic subscriber.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Acknowledge that message has been successfully consumed.
-spec ack(pid(), brod:partition(), brod:offset()) -> ok.
ack(Pid, Partition, Offset) ->
  gen_server:cast(Pid, {ack, Partition, Offset}).

%%%_* gen_server callbacks =====================================================

%% @private
-spec init(topic_subscriber_config()) -> {ok, state(), {continue, any()}}.
init(Config) ->
  Defaults = #{ message_type      => message_set
              , init_data         => undefined
              , consumer_config   => []
              , partitions        => all
              },
  #{ client            := Client
   , topic             := Topic
   , cb_module         := CbModule
   , init_data         := InitData
   , message_type      := MessageType
   , consumer_config   := ConsumerConfig
   , partitions        := Partitions
   } = maps:merge(Defaults, Config),
  {ok, CommittedOffsets, CbState} = CbModule:init(Topic, InitData),
  ok = brod_utils:assert_client(Client),
  ok = brod_utils:assert_topic(Topic),
  State =
    #state{ client       = Client
          , client_mref  = erlang:monitor(process, Client)
          , topic        = Topic
          , cb_module    = CbModule
          , cb_state     = CbState
          , message_type = MessageType
          },
  {ok, State, {continue, ?LO_CMD_START_CONSUMER(ConsumerConfig, CommittedOffsets, Partitions)}}.

%% @private
handle_continue(?LO_CMD_START_CONSUMER(ConsumerConfig, CommittedOffsets,
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
  {noreply, NewState}.

%% @private
handle_info({_ConsumerPid, #kafka_message_set{} = MsgSet}, State0) ->
  State = handle_consumer_delivery(MsgSet, State0),
  {noreply, State};
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
handle_info(Info, #state{cb_module = CbModule, cb_state = CbState} = State) ->
  %% Any unhandled messages are forwarded to the callback module to
  %% support arbitrary message-passing.
  %% Only the {noreply, State} return value is supported.
  case brod_utils:optional_callback(CbModule, handle_info, [Info, CbState], {noreply, CbState}) of
    {noreply, NewCbState} ->
      {noreply, State#state{cb_state = NewCbState}}
  end.

%% @private
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

%% @private
handle_cast({ack, Partition, Offset}, State) ->
  AckRef = {Partition, Offset},
  NewState = handle_ack(AckRef, State),
  {noreply, NewState};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% @private
terminate(Reason, #state{cb_module = CbModule, cb_state = CbState}) ->
  brod_utils:optional_callback(CbModule, terminate, [Reason, CbState], ok),
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
      Options = resolve_begin_offset(AckedOffset),
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

resolve_begin_offset(?undef) ->
  %% the default or configured 'begin_offset' in consumer config will be used
  [];
resolve_begin_offset(Offset) when ?IS_SPECIAL_OFFSET(Offset) ->
  %% special offsets are resolved by brod_consumer
  [{begin_offset, Offset}];
resolve_begin_offset(Offset) ->
  BeginOffset = Offset + 1,
  BeginOffset >= 0 orelse erlang:error({invalid_offset, Offset}),
  [{begin_offset, BeginOffset}].

handle_message_set(MessageSet, State) ->
  #kafka_message_set{ partition = Partition
                    , messages  = Messages
                    } = MessageSet,
  #state{cb_module = CbModule, cb_state = CbState} = State,
  {AckNow, NewCbState} =
    case CbModule:handle_message(Partition, MessageSet, CbState) of
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
  #state{cb_module = CbModule, cb_state = CbState} = State,
  AckRef = {Partition, Offset},
  {AckNow, NewCbState} =
    case CbModule:handle_message(Partition, Msg, CbState) of
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
