%%%
%%%   Copyright (c) 2015 Klarna AB
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
%%% @copyright 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).
-behaviour(gen_server).

-export([ start_link/4
        , produce/3
        , sync_produce_request/1
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

%% default number of messages in buffer before block callers
-define(DEFAULT_PARITION_BUFFER_LIMIT, 512).
%% default number of messages sent on wire before block waiting for acks
-define(DEFAULT_PARITION_ONWIRE_LIMIT, 128).
%% by default, send 1 MB message-set
-define(DEFAULT_MESSAGE_SET_BYTES_LIMIT, 1048576).
%% by default, require acks from all ISR
-define(DEFAULT_REQUIRED_ACKS, -1).
%% by default, leader should wait 1 second for replicas to ack
-define(DEFAULT_ACK_TIMEOUT, 1000).

-type config() :: producer_config().

-record(state,
        { client_id   :: client_id()
        , topic       :: topic()
        , partition   :: partition()
        , config      :: config()
        , sock_pid    :: pid()
        , buffer      :: brod_producer_buffer:buf()
        }).

%%%_* APIs =====================================================================

%% @doc Start (link) a partition producer.
%% Possible configs:
%%   required_acks (optional, default = -1):
%%     How many acknowledgements the kafka broker should receive from the
%%     clustered replicas before acking producer.
%%       0: the broker will not send any response
%%          (this is the only case where the broker will not reply to a request)
%%       1: The leader will wait the data is written to the local log before
%%          sending a response.
%%      -1: If it is -1 the broker will block until the message is committed by
%%          all in sync replicas before acking.
%%   ack_timeout (optional, default = 1000 ms):
%%     Maximum time in milliseconds the broker can await the receipt of the
%%     number of acknowledgements in RequiredAcks. The timeout is not an exact
%%     limit on the request time for a few reasons: (1) it does not include
%%     network latency, (2) the timer begins at the beginning of the processing
%%     of this request so if many requests are queued due to broker overload
%%     that wait time will not be included, (3) kafka leader will not terminate
%%     a local write so if the local write time exceeds this timeout it will
%%     not be respected.
%%   partition_buffer_limit(optional, default = 256):
%%     How many requests (per-partition) can be buffered without blocking the
%%     caller. The callers are released (by receiving the
%%     'brod_produce_req_buffered' reply) onece the request is taken into buffer
%%     or when the request has been put on wire, then the caller may expect
%%     a reply 'brod_produce_req_acked' if the request is accepted by kafka
%%   partition_onwire_limit(optional, default = 128):
%%     How many requests (per-partition) can be sent to kafka broker
%%     asynchronously before receiving ACKs from broker.
%%   message_set_bytes_limit(optional, default = 1M):
%%     In case callers are producing faster than brokers can handle (or
%%     congestion on wire), try to accumulate small requests into one kafka
%%     message set as much as possible but not exceeding message_set_bytes_limit
%% @end
-spec start_link(client_id(), topic(), partition(), producer_config()) ->
        {ok, pid()}.
start_link(ClientId, Topic, Partition, Config) when is_atom(ClientId) ->
  gen_server:start_link(?MODULE, {ClientId, Topic, Partition, Config}, []).

%% @doc Produce a message to partition asynchronizely.
%% The call is blocked until the request has been buffered in producer worker
%% The function returns a call reference of type brod_call_ref() to the
%% caller so the caller can used it to expect (match) a brod_produce_req_acked
%% message after the produce request has been acked by configured number of
%% replicas in kafka cluster.
%% @end
-spec produce(pid(), binary(), binary()) ->
        {ok, brod_call_ref()} | {error, any()}.
produce(Pid, Key, Value) ->
  CallRef = #brod_call_ref{ caller = self()
                          , callee = Pid
                          , ref    = make_ref()
                          },
  ok = gen_server:cast(Pid, {produce, CallRef, Key, Value}),
  ExpectedReply = #brod_produce_reply{ call_ref = CallRef
                                     , result   = brod_produce_req_buffered
                                     },
  %% Wait until the request is buffered
  case sync_produce_request(ExpectedReply) of
    ok              -> {ok, CallRef};
    {error, Reason} -> {error, Reason}
  end.

%% @doc Block sync the produce requests. The caller pid of this function
%% Must be the caller of produce/3 in which the call reference was created
%% @end
sync_produce_request(#brod_produce_reply{call_ref = CallRef} = Reply) ->
  #brod_call_ref{ caller = Caller
                , callee = Callee
                } = CallRef,
  Caller = self(), %% assert
  Mref = erlang:monitor(process, Callee),
  receive
    Reply ->
      erlang:demonitor(Mref, [flush]),
      ok;
    {'DOWN', Mref, process, _Pid, Reason} ->
      {error, {producer_down, Reason}}
  end.

%%%_* gen_server callbacks =====================================================

init({ClientId, Topic, Partition, Config}) ->
  self() ! init_socket,
  {ok, #state{ client_id = ClientId
             , topic     = Topic
             , partition = Partition
             , config    = Config
             }}.

handle_info(init_socket, #state{ client_id = ClientId
                               , topic     = Topic
                               , partition = Partition
                               , config    = Config0
                               } = State) ->
  %% 1. Fetch and validate metadata
  {ok, Metadata} = brod_client:get_metadata(ClientId, Topic),
  #metadata_response{ brokers = Brokers
                    , topics  = [TopicMetadata]
                    } = Metadata,
  #topic_metadata{ error_code = TopicErrorCode
                 , partitions = Partitions
                 } = TopicMetadata,
  brod_kafka:is_error(TopicErrorCode) andalso
    erlang:throw({"topic metadata error", TopicErrorCode}),
  #partition_metadata{ error_code = PartitionErrorCode
                     , leader_id  = LeaderId} =
    lists:keyfind(Partition, #partition_metadata.id, Partitions),
  brod_kafka:is_error(PartitionErrorCode) andalso
    erlang:throw({"partition metadata error", PartitionErrorCode}),
  LeaderId >= 0 orelse
    erlang:throw({"no leader for partition", {ClientId, Topic, Partition}}),
  #broker_metadata{host = Host, port = Port} =
    lists:keyfind(LeaderId, #broker_metadata.node_id, Brokers),

  %% 2. Lookup, or maybe (re-)establish a connection to partition leader
  {ok, SockPid} = brod_client:get_connection(ClientId, Host, Port),
  _ = erlang:monitor(process, SockPid),

  %% 3. Register self() to client.
  ok = brod_client:register_producer(ClientId, Topic, Partition),

  %% 4. Create the producing buffer
  {BufferLimit, Config1} = take_config(partition_buffer_limit,
                                       ?DEFAULT_PARITION_BUFFER_LIMIT,
                                       Config0),
  {OnWireLimit, Config2} = take_config(partition_onwire_limit,
                                       ?DEFAULT_PARITION_ONWIRE_LIMIT,
                                       Config1),
  {MsgSetBytes, Config}  = take_config(message_set_bytes_limit,
                                       ?DEFAULT_MESSAGE_SET_BYTES_LIMIT,
                                       Config2),
  RequiredAcks = get_required_acks(Config),
  AckTimeout = get_ack_timeout(Config),
  SendFun =
    fun(KafkaKvList) ->
      Data = [{Topic, [{Partition, KafkaKvList}]}],
      KafkaReq = #produce_request{ acks    = RequiredAcks
                                 , timeout = AckTimeout
                                 , data    = Data
                                 },
      brod_sock:send(SockPid, KafkaReq)
    end,
  Buffer = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                    MsgSetBytes, SendFun),
  %% 5. Update state.
  NewState = State#state{ sock_pid = SockPid
                        , buffer   = Buffer
                        },
  {noreply, NewState};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{sock_pid = Pid} = State) ->
  {stop, {socket_down, Reason}, State};
handle_info({msg, Pid, CorrId, #produce_response{} = R},
            #state{ sock_pid = Pid
                  , buffer   = Buffer
                  } = State) ->
  #produce_response{topics = [ProduceTopic]} = R,
  #produce_topic{topic = Topic, offsets = [ProduceOffset]} = ProduceTopic,
  #produce_offset{ partition  = Partition
                 , error_code = ErrorCode
                 , offset     = Offset
                 } = ProduceOffset,
  Topic = State#state.topic, %% assert
  Partition = State#state.partition, %% assert
  case brod_kafka:is_error(ErrorCode) of
    true ->
      error_logger:error_msg(
        "Error in produce response\n"
        "Topic: ~s\n"
        "Partition: ~B\n"
        "Offset: ~B\n"
        "Error code: ~p",
        [Topic, Partition, Offset, ErrorCode]),
      {stop, {produce_error, {Topic, Partition, Offset, ErrorCode}}, State};
    false ->
      NewBuffer = brod_producer_buffer:ack(Buffer, CorrId),
      {noreply, State#state{buffer = NewBuffer}}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
  {reply, {error, {unsupported_call, _Call}}, State}.

handle_cast({produce, CallRef, Key, Value}, #state{buffer = Buffer} = State) ->
  NewBuffer = brod_producer_buffer:maybe_send(Buffer, CallRef, Key, Value),
  {noreply, State#state{buffer = NewBuffer}};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internal Functions =======================================================

-spec take_config(atom(), any(), config()) -> {any(), config()}.
take_config(Key, Default, Config) when is_list(Config) ->
  case proplists:get_value(Key, Config) of
    ?undef -> {Default, Config};
    Value  -> {Value, proplists:delete(Key, Config)}
  end.

-spec get_required_acks(config()) -> required_acks().
get_required_acks(Config) when is_list(Config) ->
  case proplists:get_value(required_acks, Config) of
    ?undef ->
      ?DEFAULT_REQUIRED_ACKS;
    N when is_integer(N) ->
      N
  end.

-spec get_ack_timeout(config()) -> pos_integer().
get_ack_timeout(Config) when is_list(Config) ->
  case proplists:get_value(ack_timeout, Config) of
    ?undef ->
      ?DEFAULT_ACK_TIMEOUT;
    N when is_integer(N) andalso N > 0 ->
      N
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
