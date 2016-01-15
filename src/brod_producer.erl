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
%% by default, send max 1 MB of data in one batch (message set)
-define(DEFAULT_MAX_BATCH_SIZE, 1048576).
%% by default, require acks from all ISR
-define(DEFAULT_REQUIRED_ACKS, -1).
%% by default, leader should wait 1 second for replicas to ack
-define(DEFAULT_ACK_TIMEOUT, 1000).
%% by default, brod_producer will sleep for 1 second before trying to send
%% buffered messages again upon receiving a error from kafka
-define(DEFAULT_RETRY_TIMEOUT, 1000).
%% by default, brod_producer will try to re-send buffered messages
%% max 3 times upon receiving error from kafka
%% -1 means 'retry indefinitely'
-define(DEFAULT_MAX_RETRIES, 3).
%% by default, brod_producer will wait 1 second before trying to
%% accuire a new connection to kafka broker from brod_client
%% when an old connection crashed
-define(DEFAULT_RECONNECT_TIMEOUT, 1000).

-define(INIT_SOCKET_MSG, init_socket).
-define(RETRY_MSG, retry).

-define(config(Key, Default), proplists:get_value(Key, Config, Default)).

-record(state,
        { client_pid        :: pid()
        , topic             :: topic()
        , partition         :: partition()
        , sock_pid          :: pid()
        , buffer            :: brod_producer_buffer:buf()
        , retries = 0       :: integer()
        , max_retries       :: integer()
        , retry_timeout     :: non_neg_integer()
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
%%   partition_onwire_limit(optional, default = 128):
%%     How many requests (per-partition) can be sent to kafka broker
%%     asynchronously before receiving ACKs from broker.
%%   max_batch_size (in bytes, optional, default = 1M):
%%     In case callers are producing faster than brokers can handle (or
%%     congestion on wire), try to accumulate small requests into batches
%%     as much as possible but not exceeding max_batch_size
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

  Buffer = brod_producer_buffer:new(BufferLimit, OnWireLimit, MaxBatchSize,
                                    RequiredAcks, AckTimeout),

  MaxRetries = ?config(max_retries, ?DEFAULT_MAX_RETRIES),
  RetryTimeout = ?config(retry_timeout, ?DEFAULT_RETRY_TIMEOUT),
  ReconnectTimeout = ?config(reconnect_timeout, ?DEFAULT_RECONNECT_TIMEOUT),

  self() ! ?INIT_SOCKET_MSG,

  {ok, #state{ client_pid        = ClientPid
             , topic             = Topic
             , partition         = Partition
             , buffer            = Buffer
             , max_retries       = MaxRetries
             , retry_timeout     = RetryTimeout
             , reconnect_timeout = ReconnectTimeout
             }}.

handle_info(?INIT_SOCKET_MSG, #state{ client_pid = ClientPid
                                    , topic      = Topic
                                    , partition  = Partition
                                    } = State) ->
  %% 1. Lookup, or maybe (re-)establish a connection to partition leader
  case brod_client:get_leader_connection(ClientPid, Topic, Partition) of
    {ok, SockPid} ->
      _ = erlang:monitor(process, SockPid),
      %% 2. Register self() to client.
      ok = brod_client:register_producer(ClientPid, Topic, Partition),

      %% 3. Update state.
      {noreply, State#state{sock_pid = SockPid}};
    {error, _} ->
      ReconnectTimeout = State#state.reconnect_timeout,
      erlang:send_after(ReconnectTimeout, self(), ?INIT_SOCKET_MSG),
      {noreply, State}
  end;
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            #state{sock_pid = Pid} = State) ->
  ReconnectTimeout = State#state.reconnect_timeout,
  erlang:send_after(ReconnectTimeout, self(), ?INIT_SOCKET_MSG),
  {noreply, State#state{sock_pid = undefined}};
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
      Error = {produce_response_error, Topic, Partition, Offset, ErrorCode},
      {ok, NewState} = schedule_retry(State, Error),
      NewBuffer = brod_producer_buffer:nack(Buffer, CorrId),
      {noreply, NewState#state{buffer = NewBuffer}};
    false ->
      NewBuffer = brod_producer_buffer:ack(Buffer, CorrId),
      {noreply, State#state{buffer = NewBuffer}}
  end;
handle_info({?RETRY_MSG, Msg}, State) ->
  case maybe_retry(State) of
    {ok, NewState} ->
      {noreply, NewState};
    false ->
      Error = io_lib:format("brod_producer ~p failed to recover after ~p retries: ~p",
                            [self(), State#state.retries, Msg]),
      {stop, iolist_to_binary(Error), State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
  {reply, {error, {unsupported_call, _Call}}, State}.

handle_cast({produce, CallRef, Key, Value}, #state{buffer = Buffer0} = State) ->
  {ok, Buffer1} = brod_producer_buffer:add(Buffer0, CallRef, Key, Value),
  SockPid = State#state.sock_pid,
  Topic = State#state.topic,
  Partition = State#state.partition,
  case brod_producer_buffer:maybe_send(Buffer1, SockPid, Topic, Partition) of
    {ok, Buffer} ->
      {noreply, State#state{buffer = Buffer}};
    {error, Reason} ->
      {ok, NewState} = schedule_retry(State, {error, Reason}),
      {noreply, NewState}
  end;
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internal Functions =======================================================

schedule_retry(#state{retry_tref = undefined} = State, Msg) ->
  {ok, TRef} = timer:send_after(State#state.retry_timeout, {?RETRY_MSG, Msg}),
  {ok, State#state{retry_tref = TRef}};
schedule_retry(State, _Msg) ->
  %% retry timer has been already activated
  {ok, State}.

maybe_retry(#state{retries = Retries, max_retries = MaxRetries} = State0) when
    MaxRetries =:= -1 orelse
    Retries < MaxRetries ->
  %% try to re-send buffered data
  State = State0#state{retry_tref = undefined, retries = Retries + 1},
  Buffer = State#state.buffer,
  SockPid = State#state.sock_pid,
  Topic = State#state.topic,
  Partition = State#state.partition,
  case brod_producer_buffer:maybe_send(Buffer, SockPid, Topic, Partition) of
    {ok, Buffer} ->
      {ok, State#state{buffer = Buffer}};
    {error, Reason} ->
      schedule_retry(State, {error, Reason})
  end;
maybe_retry(_State) ->
  false.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
