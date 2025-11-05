%%%
%%%   Copyright (c) 2014-2021 Klarna Bank AB (publ)
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

%% @doc A `brod_producer' is a `gen_server' that is responsible for producing
%% messages to a given partition of a given topic.
%%
%% See the <a href="https://hexdocs.pm/brod/readme.html#producers">overview</a>
%% for some more information and examples.
-module(brod_producer).
-behaviour(gen_server).

-export([ start_link/4
        , produce/3
        , produce_cb/4
        , produce_no_ack/3
        , stop/1
        , sync_produce_request/2
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-if(?OTP_RELEASE < 25).
-export([format_status/2]).
-else.
-export([format_status/1]).
-endif.

-export([ do_send_fun/4
        , do_no_ack/2
        , do_bufcb/2
        ]).

-export_type([ config/0 ]).

-include("brod_int.hrl").

%% default number of messages in buffer before block callers
-define(DEFAULT_PARTITION_BUFFER_LIMIT, 512).
%% default number of message sets sent on wire before block waiting for acks
-define(DEFAULT_PARTITION_ONWIRE_LIMIT, 1).
%% by default, send max 1 MB of data in one batch (message set)
-define(DEFAULT_MAX_BATCH_SIZE, 1048576).
%% by default, require acks from all ISR
-define(DEFAULT_REQUIRED_ACKS, -1).
%% by default, leader should wait 10 seconds for replicas to ack
-define(DEFAULT_ACK_TIMEOUT, 10000).
%% by default, brod_producer will sleep for 0.5 second before trying to send
%% buffered messages again upon receiving a error from kafka
-define(DEFAULT_RETRY_BACKOFF_MS, 500).
%% by default, brod_producer will try to retry 3 times before crashing
-define(DEFAULT_MAX_RETRIES, 3).
%% by default, no compression
-define(DEFAULT_COMPRESSION, no_compression).
%% by default, messages never linger around in buffer
%% should be sent immediately when onwire-limit allows
-define(DEFAULT_MAX_LINGER_MS, 0).
%% by default, messages never linger around in buffer
-define(DEFAULT_MAX_LINGER_COUNT, 0).

-define(RETRY_MSG, retry).
-define(DELAYED_SEND_MSG(REF), {delayed_send, REF}).

-define(config(Key, Default), proplists:get_value(Key, Config, Default)).

-type milli_sec() :: non_neg_integer().
-type delay_send_ref() :: ?undef | {reference(), reference()}.
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type config() :: proplists:proplist().
-type call_ref() :: brod:call_ref().
-type conn() :: kpro:connection().

-record(state,
        { client_pid        :: pid()
        , topic             :: topic()
        , partition         :: partition()
        , connection        :: ?undef | conn()
        , conn_mref         :: ?undef | reference()
        , buffer            :: brod_producer_buffer:buf()
        , retry_backoff_ms  :: non_neg_integer()
        , retry_tref        :: ?undef | reference()
        , delay_send_ref    :: delay_send_ref()
        , produce_req_vsn   :: {default | resolved | configured,
                                brod_kafka_apis:vsn()}
        }).

-type state() :: #state{}.

%%%_* APIs =====================================================================

%% @doc Start (link) a partition producer.
%%
%% Possible configs (passed as a proplist):
%% <ul>
%%   <li>`required_acks' (optional, default = -1):
%%
%%     How many acknowledgements the kafka broker should receive from the
%%     clustered replicas before acking producer.
%%       0: the broker will not send any response
%%          (this is the only case where the broker will not reply to a request)
%%       1: The leader will wait the data is written to the local log before
%%          sending a response.
%%      -1: If it is -1 the broker will block until the message is committed by
%%          all in sync replicas before acking.</li>
%%   <li>`ack_timeout' (optional, default = 10000 ms):
%%
%%     Maximum time in milliseconds the broker can await the receipt of the
%%     number of acknowledgements in `RequiredAcks'. The timeout is not an exact
%%     limit on the request time for a few reasons: (1) it does not include
%%     network latency, (2) the timer begins at the beginning of the processing
%%     of this request so if many requests are queued due to broker overload
%%     that wait time will not be included, (3) kafka leader will not terminate
%%     a local write so if the local write time exceeds this timeout it will
%%     not be respected.</li>
%%   <li>`partition_buffer_limit' (optional, default = 256):
%%
%%     How many requests (per-partition) can be buffered without blocking the
%%     caller. The callers are released (by receiving the
%%     'brod_produce_req_buffered' reply) once the request is taken into buffer
%%     and after the request has been put on wire, then the caller may expect
%%     a reply 'brod_produce_req_acked' when the request is accepted by kafka.
%%   </li>
%%   <li>`partition_onwire_limit' (optional, default = 1):
%%
%%     How many message sets (per-partition) can be sent to kafka broker
%%     asynchronously before receiving ACKs from broker.
%%
%%     NOTE: setting a number greater than 1 may cause messages being persisted
%%           in an order different from the order they were produced.</li>
%%   <li>`max_batch_size' (in bytes, optional, default = 1M):
%%
%%     In case callers are producing faster than brokers can handle (or
%%     congestion on wire), try to accumulate small requests into batches
%%     as much as possible but not exceeding max_batch_size.
%%
%%     OBS: If compression is enabled, care should be taken when picking
%%          the max batch size, because a compressed batch will be produced
%%          as one message and this message might be larger than
%%          'max.message.bytes' in kafka config (or topic config)</li>
%%   <li>`max_retries' (optional, default = 3):
%%
%%     If `{max_retries, N}' is given, the producer retry produce request for
%%     N times before crashing in case of failures like connection being
%%     shutdown by remote or exceptions received in produce response from kafka.
%%     The special value N = -1 means "retry indefinitely"</li>
%%   <li>`retry_backoff_ms' (optional, default = 500);
%%
%%     Time in milli-seconds to sleep before retry the failed produce request.
%%   </li>
%%   <li>`compression' (optional, default = `no_compression`):
%%
%%     `gzip', `snappy', 'lz4' or `zstd` to enable compression</li>
%%   <li>`max_linger_ms' (optional, default = 0):
%%
%%     Messages are allowed to 'linger' in buffer for this amount of
%%     milli-seconds before being sent.
%%     Definition of 'linger': A message is in "linger" state when it is allowed
%%     to be sent on-wire, but chosen not to (for better batching).
%%
%%     The default value is 0 for 2 reasons:
%%     <ol><li>Backward compatibility (for 2.x releases)</li>
%%
%%     <li>Not to surprise `brod:produce_sync' callers</li></ol></li>
%%   <li>`max_linger_count' (optional, default = 0):
%%
%%     At most this amount (count not size) of messages are allowed to "linger"
%%     in buffer. Messages will be sent regardless of "linger" age when this
%%     threshold is hit.
%%
%%     NOTE: It does not make sense to have this value set larger than
%%           `partition_buffer_limit'</li>
%%  <li>`produce_req_vsn' (optional, default = undefined):
%%
%%     User determined produce API version to use, discard the API version range
%%     received from kafka. This is to be used when a topic in newer version
%%     kafka is configured to store older version message format.
%%     e.g. When a topic in kafka 0.11 is configured to have message format
%%     0.10, sending message with headers would result in `unknown_server_error'
%%     error code.</li>
%% </ul>
-spec start_link(pid(), topic(), partition(), config()) -> {ok, pid()}.
start_link(ClientPid, Topic, Partition, Config) ->
  gen_server:start_link(?MODULE, {ClientPid, Topic, Partition, Config}, []).

%% @doc Produce a message to partition asynchronously.
%%
%% The call is blocked until the request has been buffered in producer worker
%% The function returns a call reference of type `call_ref()' to the
%% caller so the caller can used it to expect (match) a
%% `#brod_produce_reply{result = brod_produce_req_acked}'
%% message after the produce request has been acked by kafka.
-spec produce(pid(), brod:key(), brod:value()) ->
        {ok, call_ref()} | {error, any()}.
produce(Pid, Key, Value) ->
  produce_cb(Pid, Key, Value, ?undef).

%% @doc Fire-n-forget, no ack, no back-pressure.
-spec produce_no_ack(pid(), brod:key(), brod:value()) -> ok.
produce_no_ack(Pid, Key, Value) ->
  CallRef = #brod_call_ref{caller = ?undef},
  AckCb = fun ?MODULE:do_no_ack/2,
  Batch = brod_utils:make_batch_input(Key, Value),
  Pid ! {produce, CallRef, Batch, AckCb},
  ok.

%% @doc Async produce, evaluate callback if `AckCb' is a function
%% otherwise send `#brod_produce_reply{result = brod_produce_req_acked}'
%% message to caller after the produce request has been acked by kafka.
-spec produce_cb(pid(), brod:key(), brod:value(),
                 ?undef | brod:produce_ack_cb()) ->
        ok | {ok, call_ref()} | {error, any()}.
produce_cb(Pid, Key, Value, AckCb) ->
  CallRef = #brod_call_ref{ caller = self()
                          , callee = Pid
                          , ref    = Mref = erlang:monitor(process, Pid)
                          },
  Batch = brod_utils:make_batch_input(Key, Value),
  Pid ! {produce, CallRef, Batch, AckCb},
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ ref = Mref }
                       , result   = ?buffered
                       } ->
      erlang:demonitor(Mref, [flush]),
      case AckCb of
        ?undef -> {ok, CallRef};
        _ -> ok
      end;
    {'DOWN', Mref, process, _Pid, Reason} ->
      {error, {producer_down, Reason}}
  end.

%% @doc Block calling process until it receives an acked reply for the
%% `CallRef'.
%%
%% The caller pid of this function must be the caller of
%% {@link produce/3} in which the call reference was created.
-spec sync_produce_request(call_ref(), timeout()) ->
        {ok, offset()} | {error, Reason}
          when Reason :: timeout | {producer_down, any()}.
sync_produce_request(CallRef, Timeout) ->
  #brod_call_ref{ caller = Caller
                , callee = Callee
                , ref = Ref
                } = CallRef,
  Caller = self(), %% assert
  Mref = erlang:monitor(process, Callee),
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Ref}
                       , base_offset = Offset
                       , result = brod_produce_req_acked
                       } ->
      erlang:demonitor(Mref, [flush]),
      {ok, Offset};
    {'DOWN', Mref, process, _Pid, Reason} ->
      {error, {producer_down, Reason}}
  after
    Timeout ->
      erlang:demonitor(Mref, [flush]),
      {error, timeout}
  end.

%% @doc Stop the process
-spec stop(pid()) -> ok.
stop(Pid) -> ok = gen_server:call(Pid, stop).

%%%_* gen_server callbacks =====================================================

%% @private
init({ClientPid, Topic, Partition, Config}) ->
  erlang:process_flag(trap_exit, true),
  BufferLimit = ?config(partition_buffer_limit, ?DEFAULT_PARTITION_BUFFER_LIMIT),
  OnWireLimit = ?config(partition_onwire_limit, ?DEFAULT_PARTITION_ONWIRE_LIMIT),
  MaxBatchSize = ?config(max_batch_size, ?DEFAULT_MAX_BATCH_SIZE),
  MaxRetries = ?config(max_retries, ?DEFAULT_MAX_RETRIES),
  RetryBackoffMs = ?config(retry_backoff_ms, ?DEFAULT_RETRY_BACKOFF_MS),
  RequiredAcks = ?config(required_acks, ?DEFAULT_REQUIRED_ACKS),
  AckTimeout = ?config(ack_timeout, ?DEFAULT_ACK_TIMEOUT),
  Compression = ?config(compression, ?DEFAULT_COMPRESSION),
  MaxLingerMs = ?config(max_linger_ms, ?DEFAULT_MAX_LINGER_MS),
  MaxLingerCount = ?config(max_linger_count, ?DEFAULT_MAX_LINGER_COUNT),
  SendFun = make_send_fun(Topic, Partition, RequiredAcks, AckTimeout, Compression),
  Buffer = brod_producer_buffer:new(BufferLimit, OnWireLimit, MaxBatchSize,
                                    MaxRetries, MaxLingerMs, MaxLingerCount,
                                    SendFun),
  DefaultVsn = brod_kafka_apis:default_version(produce),
  ReqVersion = case ?config(produce_req_vsn, ?undef) of
                 ?undef -> {default, DefaultVsn};
                 Vsn    -> {configured, Vsn}
               end,
  State = #state{ client_pid       = ClientPid
                , topic            = Topic
                , partition        = Partition
                , buffer           = Buffer
                , retry_backoff_ms = RetryBackoffMs
                , connection       = ?undef
                , produce_req_vsn  = ReqVersion
                },
  %% Register self() to client.
  ok = brod_client:register_producer(ClientPid, Topic, Partition),
  {ok, State}.

%% @private
handle_info(?DELAYED_SEND_MSG(MsgRef),
            #state{delay_send_ref = {_Tref, MsgRef}} = State0) ->
  State1 = State0#state{delay_send_ref = ?undef},
  {ok, State} = maybe_produce(State1),
  {noreply, State};
handle_info(?DELAYED_SEND_MSG(_MsgRef), #state{} = State) ->
  %% stale delay-send timer expiration, discard
  {noreply, State};
handle_info(?RETRY_MSG, #state{} = State0) ->
  State1 = State0#state{retry_tref = ?undef},
  {ok, State2} = maybe_reinit_connection(State1),
  %% For retry-interval deterministic, produce regardless of connection state.
  %% In case it has failed to find a new connection in maybe_reinit_connection/1
  %% the produce call should fail immediately with {error, no_leader_connection}
  %% and a new retry should be scheduled (if not reached max_retries yet)
  {ok, State} = maybe_produce(State2),
  {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{connection = Pid, buffer = Buffer0} = State) ->
  case brod_producer_buffer:is_empty(Buffer0) of
    true ->
      %% no connection restart in case of empty request buffer
      {noreply, State#state{connection = ?undef, conn_mref = ?undef}};
    false ->
      %% put sent requests back to buffer immediately after connection down
      %% to fail fast if retry is not allowed (reaching max_retries).
      Buffer = brod_producer_buffer:nack_all(Buffer0, Reason),
      {ok, NewState} = schedule_retry(State#state{buffer = Buffer}),
      {noreply, NewState#state{connection = ?undef, conn_mref = ?undef}}
  end;
handle_info({produce, CallRef, Batch, AckCb}, #state{partition = Partition} = State) ->
  BufCb = make_bufcb(CallRef, AckCb, Partition),
  handle_produce(BufCb, Batch, State);
handle_info({msg, Pid, #kpro_rsp{ api = produce
                                , ref = Ref
                                , msg = Rsp
                                }},
            #state{ connection = Pid
                  , buffer   = Buffer
                  } = State) ->
  [TopicRsp] = kpro:find(responses, Rsp),
  Topic = kpro:find(topic, TopicRsp),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  Partition = kpro:find(partition, PartitionRsp),
  ErrorCode = kpro:find(error_code, PartitionRsp),
  Offset = kpro:find(base_offset, PartitionRsp),
  Topic = State#state.topic, %% assert
  Partition = State#state.partition, %% assert
  {ok, NewState} =
    case ?IS_ERROR(ErrorCode) of
      true ->
        _ = log_error_code(Topic, Partition, Offset, ErrorCode),
        Error = {produce_response_error, Topic, Partition,
                 Offset, ErrorCode},
        is_retriable(ErrorCode) orelse exit({not_retriable, Error}),
        NewBuffer = brod_producer_buffer:nack(Buffer, Ref, Error),
        schedule_retry(State#state{buffer = NewBuffer});
      false ->
        NewBuffer = brod_producer_buffer:ack(Buffer, Ref, Offset),
        maybe_produce(State#state{buffer = NewBuffer})
    end,
  {noreply, NewState};
handle_info(_Info, #state{} = State) ->
  {noreply, State}.

%% @private
handle_call(stop, _From, #state{} = State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, #state{} = State) ->
  {reply, {error, {unsupported_call, Call}}, State}.

%% @private
handle_cast(_Cast, #state{} = State) ->
  {noreply, State}.

%% @private
code_change(_OldVsn, #state{} = State, _Extra) ->
  {ok, State}.

%% @private
terminate(Reason, #state{client_pid = ClientPid
                          , topic = Topic
                          , partition = Partition
                          }) ->
  case brod_utils:is_normal_reason(Reason) of
    true ->
      brod_client:deregister_producer(ClientPid, Topic, Partition);
    false ->
      ok
  end,
  ok.

%% @private
-if(?OTP_RELEASE < 25).
format_status(normal, [_PDict, State=#state{}]) ->
  [{data, [{"State", State}]}];
format_status(terminate, [_PDict, State=#state{buffer = Buffer}]) ->
  %% Do not format the buffer attribute when process terminates abnormally and logs an error
  %% but allow it when is a sys:get_status/1.2
  State#state{buffer = brod_producer_buffer:empty_buffers(Buffer)}.
-else.
format_status(#{reason := normal} = Status) ->
  Status;
format_status(#{reason := terminate, state := #state{buffer = Buffer} = State} = Status) ->
  %% Do not format the buffer attribute when process terminates abnormally and logs an error
  %% but allow it when is a sys:get_status/1.2
  Status#{state => State#state{buffer = brod_producer_buffer:empty_buffers(Buffer)}}.
-endif.

%%%_* Internal Functions =======================================================

make_send_fun(Topic, Partition, RequiredAcks, AckTimeout, Compression) ->
  ExtraArg = {Topic, Partition, RequiredAcks, AckTimeout, Compression},
  {fun ?MODULE:do_send_fun/4, ExtraArg}.

%% @private
do_send_fun(ExtraArg, Conn, BatchInput, Vsn) ->
  {Topic, Partition, RequiredAcks, AckTimeout, Compression} = ExtraArg,
  ProduceRequest =
    brod_kafka_request:produce(Vsn, Topic, Partition, BatchInput,
                               RequiredAcks, AckTimeout, Compression),
  case send(Conn, ProduceRequest) of
    ok when ProduceRequest#kpro_req.no_ack ->
      ok;
    ok ->
      {ok, ProduceRequest#kpro_req.ref};
    {error, Reason} ->
      {error, Reason}
  end.

%% @private
do_no_ack(_Partition, _BaseOffset) -> ok.

-spec log_error_code(topic(), partition(), offset(), brod:error_code()) -> _.
log_error_code(Topic, Partition, Offset, ErrorCode) ->
  ?BROD_LOG_ERROR("Produce error ~s-~B Offset: ~B Error: ~p",
                  [Topic, Partition, Offset, ErrorCode]).

make_bufcb(CallRef, AckCb, Partition) ->
  {fun ?MODULE:do_bufcb/2, _ExtraArg = {CallRef, AckCb, Partition}}.

%% @private
do_bufcb({CallRef, AckCb, Partition}, Arg) ->
  #brod_call_ref{caller = Pid} = CallRef,
  case Arg of
    ?buffered when is_pid(Pid) ->
      Reply = #brod_produce_reply{ call_ref = CallRef
                                 , result = ?buffered
                                 },
      erlang:send(Pid, Reply);
    ?buffered ->
      %% caller requires no ack
      ok;
    {?acked, BaseOffset} when AckCb =:= ?undef ->
      Reply = #brod_produce_reply{ call_ref = CallRef
                                 , base_offset = BaseOffset
                                 , result = ?acked
                                 },
      erlang:send(Pid, Reply);
    {?acked, BaseOffset} when is_function(AckCb, 2) ->
      AckCb(Partition, BaseOffset)
  end.

handle_produce(BufCb, Batch,
               #state{retry_tref = Ref} = State) when is_reference(Ref) ->
  %% pending on retry, add to buffer regardless of connection state
  do_handle_produce(BufCb, Batch, State);
handle_produce(BufCb, Batch,
               #state{connection = Pid} = State) when is_pid(Pid) ->
  %% Connection is alive, add to buffer, and try send produce request
  do_handle_produce(BufCb, Batch, State);
handle_produce(BufCb, Batch, #state{} = State) ->
  %% this is the first request after fresh start/restart or connection death
  {ok, NewState} = maybe_reinit_connection(State),
  do_handle_produce(BufCb, Batch, NewState).

do_handle_produce(BufCb, Batch, #state{buffer = Buffer} = State) ->
  NewBuffer = brod_producer_buffer:add(Buffer, BufCb, Batch),
  State1 = State#state{buffer = NewBuffer},
  {ok, NewState} = maybe_produce(State1),
  {noreply, NewState}.

-spec maybe_reinit_connection(state()) -> {ok, state()}.
maybe_reinit_connection(#state{ client_pid      = ClientPid
                              , connection      = OldConnection
                              , conn_mref       = OldConnMref
                              , topic           = Topic
                              , partition       = Partition
                              , buffer          = Buffer0
                              , produce_req_vsn = ReqVersion
                              } = State) ->
  %% Lookup, or maybe (re-)establish a connection to partition leader
  case brod_client:get_leader_connection(ClientPid, Topic, Partition) of
    {ok, OldConnection} ->
      %% Still the old connection
      {ok, State};
    {ok, Connection} ->
      ok = maybe_demonitor(OldConnMref),
      ConnMref = erlang:monitor(process, Connection),
      %% Make sure the sent but not acked ones are put back to buffer
      Buffer = brod_producer_buffer:nack_all(Buffer0, new_leader),
      {ok, State#state{ connection      = Connection
                      , conn_mref       = ConnMref
                      , buffer          = Buffer
                      , produce_req_vsn = req_vsn(Connection, ReqVersion)
                      }};
    {error, Reason} ->
      ok = maybe_demonitor(OldConnMref),
      %% Make sure the sent but not acked ones are put back to buffer
      Buffer = brod_producer_buffer:nack_all(Buffer0, no_leader_connection),
      ?BROD_LOG_WARNING("Failed to (re)init connection, reason:\n~p",
                        [Reason]),
      {ok, State#state{ connection = ?undef
                      , conn_mref  = ?undef
                      , buffer     = Buffer
                      }}
  end.

maybe_produce(#state{retry_tref = Ref} = State) when is_reference(Ref) ->
  %% pending on retry after failure
  {ok, State};
maybe_produce(#state{ buffer = Buffer0
                    , connection = Connection
                    , delay_send_ref = DelaySendRef0
                    , produce_req_vsn = {_, Vsn}
                    } = State) ->
  _ = cancel_delay_send_timer(DelaySendRef0),
  case brod_producer_buffer:maybe_send(Buffer0, Connection, Vsn) of
    {ok, Buffer} ->
      %% One or more produce requests are sent;
      %% Or no more message left to send;
      %% Or it has hit `partition_onwire_limit', pending on reply from kafka
      {ok, State#state{buffer = Buffer}};
    {{delay, Timeout}, Buffer} ->
      %% Keep the messages in buffer for better batching
      DelaySendRef = start_delay_send_timer(Timeout),
      NewState = State#state{ buffer         = Buffer
                            , delay_send_ref = DelaySendRef
                            },
      {ok, NewState};
    {retry, Buffer} ->
      %% Failed to send, e.g. due to connection error, retry later
      schedule_retry(State#state{buffer = Buffer})
  end.

%% Resolve produce API version to use.
%% If api version is configured by user, always use configured version,
%% Otherwise if we have a connection to partition leader
%% pick the highest version supported by kafka.
%% If connection is down, keep using the version previously used.
req_vsn(_, {configured, Vsn}) ->
  {configured, Vsn};
req_vsn(Conn, {_, OldVsn} = Old) when is_pid(Conn) ->
  Default = brod_kafka_apis:default_version(produce),
  NewVsn = brod_kafka_apis:pick_version(Conn, produce),
  case NewVsn =:= Default andalso NewVsn < OldVsn of
    true ->
      %% Failed to resolve max version, e.g. during connection restart
      %% keep using the old
      Old;
    false ->
      {resolved, NewVsn}
  end.

%% Start delay send timer.
-spec start_delay_send_timer(milli_sec()) -> delay_send_ref().
start_delay_send_timer(Timeout) ->
  MsgRef = make_ref(),
  TRef = erlang:send_after(Timeout, self(), ?DELAYED_SEND_MSG(MsgRef)),
  {TRef, MsgRef}.

%% Ensure delay send timer is canceled.
%% But not flushing the possibly already sent (stale) message
%% Stale message should be discarded in handle_info
-spec cancel_delay_send_timer(delay_send_ref()) -> _.
cancel_delay_send_timer(?undef) -> ok;
cancel_delay_send_timer({Tref, _Msg}) -> _ = erlang:cancel_timer(Tref).

maybe_demonitor(?undef) ->
  ok;
maybe_demonitor(Mref) ->
  true = erlang:demonitor(Mref, [flush]),
  ok.

schedule_retry(#state{ retry_tref = ?undef
                     , retry_backoff_ms = Timeout
                     } = State) ->
  TRef = erlang:send_after(Timeout, self(), ?RETRY_MSG),
  {ok, State#state{retry_tref = TRef}};
schedule_retry(State) ->
  %% retry timer has been already activated
  {ok, State}.

is_retriable(EC) when EC =:= ?unknown_topic_or_partition;
                      EC =:= ?leader_not_available;
                      EC =:= ?not_leader_for_partition;
                      EC =:= ?request_timed_out;
                      EC =:= ?not_enough_replicas;
                      EC =:= ?not_enough_replicas_after_append ->
  true;
is_retriable(_) ->
  false.

-spec send(?undef | pid(), kpro:req()) -> ok | {error, any()}.
send(?undef, _KafkaReq) -> {error, no_leader_connection};
send(Connection, KafkaReq) -> kpro:request_async(Connection, KafkaReq).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
