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

-define(RETRY_MSG, retry).

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
%%     or when the request has been put on wire, then the caller may expect
%%     a reply 'brod_produce_req_acked' if the request is accepted by kafka
%%   partition_onwire_limit(optional, default = 1):
%%     How many message sets (per-partition) can be sent to kafka broker
%%     asynchronously before receiving ACKs from broker.
%%     NOTE: setting a number greater than 1 may cause messages being persisted
%%           in an order different from the order they were produced.
%%   max_batch_size (in bytes, optional, default = 1M):
%%     In case callers are producing faster than brokers can handle (or
%%     congestion on wire), try to accumulate small requests into batches
%%     as much as possible but not exceeding max_batch_size
%%   max_retries (optional, default = 3):
%%     If {max_retries, N} is given, the producer retry produce request for
%%     N times before crashing in case of failures like socket being shut down
%%     or exceptions received in produce response from kafka.
%%     The special value N = -1 means 'retry indefinitely'
%%   retry_backoff_ms (optional, default = 500)
%%     Time in milli-seconds to sleep before retry the failed produce request.
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
  RequiredAcks = ?config(required_acks, ?DEFAULT_REQUIRED_ACKS),
  AckTimeout = ?config(ack_timeout, ?DEFAULT_ACK_TIMEOUT),
  MaxRetries = ?config(max_retries, ?DEFAULT_MAX_RETRIES),
  RetryBackoffMs = ?config(retry_backoff_ms, ?DEFAULT_RETRY_BACKOFF_MS),

  SendFun =
    fun(SockPid, KafkaKvList) ->
        ProduceRequest = kpro:produce_request(Topic, Partition, KafkaKvList,
                                              RequiredAcks, AckTimeout),
        case sock_send(SockPid, ProduceRequest) of
          ok              -> ok;
          {ok, CorrId}    -> {ok, CorrId};
          {error, Reason} -> {error, Reason}
        end
    end,
  Buffer = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                    MaxBatchSize, MaxRetries, SendFun),

  State0 = #state{ client_pid       = ClientPid
                 , topic            = Topic
                 , partition        = Partition
                 , buffer           = Buffer
                 , retry_backoff_ms = RetryBackoffMs
                 },

  State = case init_socket(State0) of
            {ok, NewState} ->
              NewState;
            {error, Error} ->
              {ok, NewState} = schedule_retry(State0, Error),
              NewState
          end,
  {ok, State}.

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
  {ok, NewState} = schedule_retry(State, Reason),
  {noreply, NewState#state{sock_pid = ?undef}};
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
handle_info(_Info, State) ->
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
  {reply, {error, {unsupported_call, _Call}}, State}.

handle_cast({produce, CallRef, Key, Value}, #state{buffer = Buffer} = State) ->
  {ok, NewBuffer} = brod_producer_buffer:add(Buffer, CallRef, Key, Value),
  State1 = State#state{buffer = NewBuffer},
  {ok, NewState} = maybe_produce(State1),
  {noreply, NewState};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internal Functions =======================================================

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
      %% 2. Register self() to client.
      ok = brod_client:register_producer(ClientPid, Topic, Partition),
      %% 3. Update state.
      {ok, State#state{sock_pid = SockPid, sock_mref = SockMref}};
    {error, Error} ->
      {error, Error}
  end.

maybe_produce(#state{buffer = Buffer0, sock_pid = SockPid} = State) ->
  case brod_producer_buffer:maybe_send(Buffer0, SockPid) of
    {ok, Buffer}    -> {ok, State#state{buffer = Buffer}};
    {retry, Buffer} -> schedule_retry(State#state{buffer = Buffer})
  end.

maybe_demonitor(?undef) ->
  ok;
maybe_demonitor(Mref) ->
  true = erlang:demonitor(Mref, [flush]),
  ok.

schedule_retry(#state{buffer = Buffer} = State, Reason) ->
  {ok, NewBuffer} = brod_producer_buffer:nack_all(Buffer, Reason),
  schedule_retry(State#state{buffer = NewBuffer}).

schedule_retry(#state{retry_tref = ?undef} = State) ->
  {ok, TRef} = timer:send_after(State#state.retry_backoff_ms, ?RETRY_MSG),
  {ok, State#state{retry_tref = TRef}};
schedule_retry(State) ->
  %% retry timer has been already activated
  {ok, State}.

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

sock_send(?undef, _KafkaReq) -> {error, sock_down};
sock_send(SockPid, KafkaReq) -> brod_sock:request_async(SockPid, KafkaReq).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
