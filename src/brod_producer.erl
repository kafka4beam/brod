%%%
%%%   Copyright (c) 2014-2016, Klarna AB
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
%%% @copyright 2014-2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).
-behaviour(gen_server).

-export([ start_link/4
        , produce/3
        , produce_cast/2
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
%% default number of message sets sent on wire before block waiting for acks
-define(DEFAULT_PARITION_ONWIRE_LIMIT, 1).
%% by default, send max 1 MB of data in one batch (message set)
-define(DEFAULT_MAX_BATCH_SIZE, 1048576).
%% by default, require acks from all ISR
-define(DEFAULT_REQUIRED_ACKS, -1).
%% by default, leader should wait 1 second for replicas to ack
-define(DEFAULT_ACK_TIMEOUT, 1000).
%% by default, brod_producer will sleep for 0.5 second before trying to send
%% buffered messages again upon receiving a error from kafka
-define(DEFAULT_RETRY_BACKOFF_MS, 500).
%% by default, brod_producer will try to retry 3 times before crashing
-define(DEFAULT_MAX_RETRIES, 3).
%% by default, no compression
-define(DEFAULT_COMPRESSION, no_compression).
%% by default, only compress if batch size is >= 1k
-define(DEFAULT_MIN_COMPRESSION_BATCH_SIZE, 1024).
%% by default, messages never linger around in buffer
%% should be sent immediately when onwire-limit allows
-define(DEFAULT_MAX_LINGER_TIME, 0).
%% by default, messages never linger around in buffer
-define(DEFAULT_MAX_LINGER_COUNT, 0).

-define(RETRY_MSG, retry).
-define(DELAYED_SEND_MSG(REF), {delayed_send, REF}).

-define(config(Key, Default), proplists:get_value(Key, Config, Default)).

-record(state,
        { client_pid        :: pid()
        , topic             :: topic()
        , partition         :: partition()
        , sock_pid          :: pid()
        , sock_mref         :: reference()
        , buffer            :: brod_producer_buffer:buf()
        , retry_backoff_ms  :: non_neg_integer()
        , retry_tref        :: timer:tref()
        , reconnect_timeout :: non_neg_integer()
        , delay_send_ref    :: ?undef | {timer:tref(), reference()}
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
%%     'brod_produce_req_buffered' reply) once the request is taken into buffer
%%     and after the request has been put on wire, then the caller may expect
%%     a reply 'brod_produce_req_acked' when the request is accepted by kafka.
%%   partition_onwire_limit(optional, default = 1):
%%     How many message sets (per-partition) can be sent to kafka broker
%%     asynchronously before receiving ACKs from broker.
%%     NOTE: setting a number greater than 1 may cause messages being persisted
%%           in an order different from the order they were produced.
%%   max_batch_size (in bytes, optional, default = 1M):
%%     In case callers are producing faster than brokers can handle (or
%%     congestion on wire), try to accumulate small requests into batches
%%     as much as possible but not exceeding max_batch_size.
%%     OBS: If compression is enabled, care should be taken when picking
%%          the max batch size, because a compressed batch will be produced
%%          as one message and this message might be larger than
%%          'max.message.bytes' in kafka config (or topic config)
%%   max_retries (optional, default = 3):
%%     If {max_retries, N} is given, the producer retry produce request for
%%     N times before crashing in case of failures like socket being shut down
%%     or exceptions received in produce response from kafka.
%%     The special value N = -1 means 'retry indefinitely'
%%   retry_backoff_ms (optional, default = 500);
%%     Time in milli-seconds to sleep before retry the failed produce request.
%%   compression (optional, default = no_compression):
%%     'gzip' or 'snappy' to enable compression
%%   min_compression_batch_size (in bytes, optional, default = 1K):
%%     Only try to compress when batch size is greater than this value.
%%   max_linger_time(optional, default = 0):
%%     Messages are allowed to 'linger' in buffer for this amount of
%%     milli-seconds before being sent.
%%     Definition of 'linger': A message is in 'linger' state when it is allowed
%%     to be sent on-wire, but chosen not to (for better batching).
%%     The default value in 2.x release is set to 0 for backward compatibility.
%%     May change it the next major release.
%%   max_linger_count(optional, default = 0):
%%     At most this amount (count not size) of messages messages are allowed
%%     to 'linger' in buffer. Messages will be sent regardless of 'linger' age
%%     when this the threshold is hit.
%%     NOTE: It does not make sense to have this value set larger than
%%           `partition_buffer_limit`
%% @end
-spec start_link(pid(), topic(), partition(), producer_config()) ->
        {ok, pid()}.
start_link(ClientPid, Topic, Partition, Config) ->
  gen_server:start_link(?MODULE, {ClientPid, Topic, Partition, Config}, []).

%% @doc Produce a message to partition asynchronizely.
%% The call is blocked until the request has been buffered in producer worker
%% The function returns a call reference of type brod_call_ref() to the
%% caller so the caller can used it to expect (match) a brod_produce_req_acked
%% message after the produce request has been acked by configured number of
%% replicas in kafka cluster.
%% @end
-spec produce(pid(), key(), value()) ->
        {ok, brod_call_ref()} | {error, any()}.
produce(Pid, Key, Value) ->
  CallRef = #brod_call_ref{ caller = self()
                          , callee = Pid
                          , ref    = Mref = erlang:monitor(process, Pid)
                          },
  Pid ! {produce, CallRef, Key, Value},
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ ref = Mref }
                       , result   = brod_produce_req_buffered
                       } ->
      erlang:demonitor(Mref, [flush]),
      {ok, CallRef};
    {'DOWN', Mref, process, _Pid, Reason} ->
      {error, {producer_down, Reason}}
  end.

%% @doc Cast a list of messages to partition asynchronizely.
%% `{ok, brod_call_ref()}' is returned for the caller to match on received
%% acks.  `{error, producer_down}' is returned if the producer process is down.
%% This call is not blocked. The caller should stop and sync acks when there
%% are certain number of un-acked messages pending.
%% @end
-spec produce_cast(pid(), [{key(), value()}]) ->
        {ok, brod_call_ref()} | {error, {producer_down, noproc}}.
produce_cast(Pid, KvList) ->
  %% NOTE: Although the references are generated sequential,
  %%       the caller should not expect the replies to be ordered
  %%       when it produces messages to two or more partitions.
  Ref = case erlang:get(brod_producer_cast_ref) of
          undefined -> 0;
          N         -> N + 1
        end,
  erlang:put(brod_producer_cast_ref, Ref),
  CallRef = #brod_call_ref{ caller = self()
                          , callee = Pid
                          , ref    = Ref
                          },
  case is_process_alive(Pid) of
    true ->
      Pid ! {produce, CallRef, <<>>, KvList},
      {ok, CallRef};
    false ->
      {error, {producer_down, noproc}}
  end.

%% @doc Block calling process until it receives ExpectedReply.
%%      The caller pid of this function must be the caller of produce/3
%%      in which the call reference was created.
%% @end
sync_produce_request(#brod_produce_reply{call_ref = CallRef} = ExpectedReply) ->
  #brod_call_ref{ caller = Caller
                , callee = Callee
                } = CallRef,
  Caller = self(), %% assert
  Mref = erlang:monitor(process, Callee),
  receive
    ExpectedReply ->
      erlang:demonitor(Mref, [flush]),
      ok;
    {'DOWN', Mref, process, _Pid, Reason} ->
      {error, {producer_down, Reason}}
  end.

%%%_* gen_server callbacks =====================================================

init({ClientPid, Topic, Partition, Config}) ->
  BufferLimit = ?config(partition_buffer_limit, ?DEFAULT_PARITION_BUFFER_LIMIT),
  OnWireLimit = ?config(partition_onwire_limit, ?DEFAULT_PARITION_ONWIRE_LIMIT),
  MaxBatchSize = ?config(max_batch_size, ?DEFAULT_MAX_BATCH_SIZE),
  MaxRetries = ?config(max_retries, ?DEFAULT_MAX_RETRIES),
  RetryBackoffMs = ?config(retry_backoff_ms, ?DEFAULT_RETRY_BACKOFF_MS),

  RequiredAcks = ?config(required_acks, ?DEFAULT_REQUIRED_ACKS),
  AckTimeout = ?config(ack_timeout, ?DEFAULT_ACK_TIMEOUT),
  Compression = ?config(compression, ?DEFAULT_COMPRESSION),
  MinCompressBatchSize = ?config(min_compression_batch_size,
                                 ?DEFAULT_MIN_COMPRESSION_BATCH_SIZE),
  MaxLingerTime = ?config(max_linger_time, ?DEFAULT_MAX_LINGER_TIME),
  MaxLingerCount = ?config(max_linger_count, ?DEFAULT_MAX_LINGER_COUNT),
  MaybeCompress =
    fun(KafkaKvList) ->
      case Compression =/= no_compression andalso
           brod_utils:bytes(KafkaKvList) >= MinCompressBatchSize of
        true  -> Compression;
        false -> no_compression
      end
    end,
  SendFun =
    fun(SockPid, KafkaKvList) ->
        ProduceRequest = kpro:produce_request(Topic, Partition, KafkaKvList,
                                              RequiredAcks, AckTimeout,
                                              MaybeCompress(KafkaKvList)),
        sock_send(SockPid, ProduceRequest)
    end,
  Buffer = brod_producer_buffer:new(BufferLimit, OnWireLimit, MaxBatchSize,
                                    MaxRetries, MaxLingerTime, MaxLingerCount,
                                    SendFun),

  State = #state{ client_pid       = ClientPid
                , topic            = Topic
                , partition        = Partition
                , buffer           = Buffer
                , retry_backoff_ms = RetryBackoffMs
                , sock_pid         = ?undef
                },
  %% Register self() to client.
  ok = brod_client:register_producer(ClientPid, Topic, Partition),
  {ok, State}.

handle_info(?DELAYED_SEND_MSG(MsgRef),
            #state{delay_send_ref = {Tref, MsgRef}} = State0) ->
  State1 = State0#state{delay_send_ref = ?undef},
  State = maybe_produce(State1),
  {noreply, State};
handle_info(?DELAYED_SEND_MSG(_MsgRef), State) ->
  %% stale delay-send timer expiration, discard
  {noreply, State};
handle_info(?RETRY_MSG, State0) ->
  State1 = State0#state{retry_tref = ?undef},
  {ok, State} =
    case init_socket(State1) of
      {ok, State2}   -> maybe_produce(State2);
      {error, Error} -> schedule_retry(State1, Error)
    end,
  {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{sock_pid = Pid} = State) ->
  case brod_producer_buffer:is_empty(State#state.buffer) of
    true ->
      %% no socket restart in case of empty request buffer
      {noreply, State#state{sock_pid = ?undef}};
    false ->
      {ok, NewState} = schedule_retry(State, Reason),
      {noreply, NewState#state{sock_pid = ?undef}}
  end;
handle_info({produce, CallRef, Key, Value},
            #state{buffer = Buffer, sock_pid = SockPid} = State) ->
  case not brod_utils:is_pid_alive(SockPid) andalso
       brod_producer_buffer:is_empty(Buffer) of
    true ->
      %% this is the first request after fresh boot or socket death
      case init_socket(State) of
        {ok, NewState} ->
          handle_produce(CallRef, Key, Value, NewState);
        {error, _} ->
          %% failed to initialize payload socket
          %% call handle_produce, let the send fun fail
          %% retry should take care of socket re-init
          handle_produce(CallRef, Key, Value, State)
      end;
    false ->
      handle_produce(CallRef, Key, Value, State)
  end;
handle_info({msg, Pid, CorrId, #kpro_ProduceResponse{} = R},
            #state{ sock_pid = Pid
                  , buffer   = Buffer
                  } = State) ->
  #kpro_ProduceResponse{produceResponseTopic_L = [ProduceTopic]} = R,
  #kpro_ProduceResponseTopic{ topicName                  = Topic
                            , produceResponsePartition_L = [ProduceOffset]
                            } = ProduceTopic,
  #kpro_ProduceResponsePartition{ partition  = Partition
                                , errorCode = ErrorCode
                                , offset     = Offset
                                } = ProduceOffset,
  Topic = State#state.topic, %% assert
  Partition = State#state.partition, %% assert
  {ok, NewState} =
    case kpro_ErrorCode:is_error(ErrorCode) of
      true ->
        error_logger:error_msg(
          "Error in produce response\n"
          "Topic: ~s\n"
          "Partition: ~B\n"
          "Offset: ~B\n"
          "Error: ~p",
          [Topic, Partition, Offset, ErrorCode]),
        Error = {produce_response_error, Topic, Partition,
                 Offset, ErrorCode},
        is_retriable(ErrorCode) orelse exit({not_retriable, Error}),
        case brod_producer_buffer:nack(Buffer, CorrId, Error) of
          {ok, NewBuffer}  -> schedule_retry(State#state{buffer = NewBuffer});
          {error, ignored} -> maybe_produce(State)
        end;
      false ->
        case brod_producer_buffer:ack(Buffer, CorrId) of
          {ok, NewBuffer}  -> maybe_produce(State#state{buffer = NewBuffer});
          {error, ignored} -> maybe_produce(State)
        end
    end,
  {noreply, NewState};
handle_info(_Info, #state{} = State) ->
  {noreply, State}.

handle_call(stop, _From, #state{} = State) ->
  {stop, normal, ok, State};
handle_call(_Call, _From, #state{} = State) ->
  {reply, {error, {unsupported_call, _Call}}, State}.

handle_cast(_Cast, #state{} = State) ->
  {noreply, State}.

code_change(_OldVsn, #state{} = State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internal Functions =======================================================

handle_produce(CallRef, Key, Value, #state{buffer = Buffer} = State) ->
  {ok, NewBuffer} = brod_producer_buffer:add(Buffer, CallRef, Key, Value),
  State1 = State#state{buffer = NewBuffer},
  {ok, NewState} = maybe_produce(State1),
  {noreply, NewState}.

init_socket(#state{ client_pid = ClientPid
                  , sock_mref  = OldSockMref
                  , topic      = Topic
                  , partition  = Partition
                  } = State) ->
  %% 1. Lookup, or maybe (re-)establish a connection to partition leader
  case brod_client:get_leader_connection(ClientPid, Topic, Partition) of
    {ok, SockPid} ->
      ok = maybe_demonitor(OldSockMref),
      SockMref = erlang:monitor(process, SockPid),
      %% 2. Update state.
      {ok, State#state{sock_pid = SockPid, sock_mref = SockMref}};
    {error, Error} ->
      {error, Error}
  end.

%% @private
maybe_produce(#state{retry_tref = Ref} = State) when is_reference(Ref) ->
  %% pending on retry after failure
  {ok, State};
maybe_produce(#state{ buffer = Buffer0
                    , sock_pid = SockPid
                    , delay_send_ref = DelaySendRef
                    } = State) ->
  case brod_producer_buffer:maybe_send(Buffer0, SockPid) of
    {ok, Buffer} ->
      %% maybe send before delay-send timer expiration
      %% try to cancel the timer, but do NOT flush the message
      %% in case the timer is already expired by now, the stale
      %% message should be discarded in handle_info
      case DelaySendRef of
        {Tref, _MsgRef} -> _ = erlang:cancel_timer(Tref);
        ?undef          -> ok
      end,
      {ok, State#state{buffer = Buffer, delay_send_ref = ?undef}};
    {{delay, Timeout}, Buffer} when DelaySendRef =:= ?undef ->
      MsgRef = make_ref(),
      TRef = erlang:send_after(Timeout, self(), ?DELAYED_SEND_MSG(MsgRef)),
      {ok, State#state{ buffer         = Buffer
                      , delay_send_ref = {TRef, MsgRef}
                      }};
    {{delay, _Timeout}, Buffer} ->
      %% delay-send timer already started, don't start again
      {ok, State#state{buffer = Buffer}};
    {retry, Buffer} ->
      %% failed to send, e.g. due to socket down
      %% maybe retry later
      schedule_retry(State#state{buffer = Buffer})
  end.

%% @private
maybe_demonitor(?undef) ->
  ok;
maybe_demonitor(Mref) ->
  true = erlang:demonitor(Mref, [flush]),
  ok.

%% @private
schedule_retry(#state{buffer = Buffer} = State, Reason) ->
  {ok, NewBuffer} = brod_producer_buffer:nack_all(Buffer, Reason),
  schedule_retry(State#state{buffer = NewBuffer}).

%% @private
schedule_retry(#state{ retry_tref = ?undef
                     , retry_backoff_ms = Timeout
                     } = State) ->
  TRef = erlang:send_after(Timeout, self(), ?RETRY_MSG),
  {ok, State#state{retry_tref = TRef}};
schedule_retry(State) ->
  %% retry timer has been already activated
  {ok, State}.

%% @private
is_retriable(EC) when EC =:= ?EC_CORRUPT_MESSAGE;
                      EC =:= ?EC_UNKNOWN_TOPIC_OR_PARTITION;
                      EC =:= ?EC_LEADER_NOT_AVAILABLE;
                      EC =:= ?EC_NOT_LEADER_FOR_PARTITION;
                      EC =:= ?EC_REQUEST_TIMED_OUT;
                      EC =:= ?EC_NOT_ENOUGH_REPLICAS;
                      EC =:= ?EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND ->
  true;
is_retriable(_) ->
  false.

%% @private
-spec sock_send(?undef | pid(), kpro_ProduceRequest()) ->
        ok | {ok, corr_id()} | {error, any()}.
sock_send(?undef, _KafkaReq) -> {error, sock_down};
sock_send(SockPid, KafkaReq) -> brod_sock:request_async(SockPid, KafkaReq).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
