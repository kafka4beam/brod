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
        , sync_produce_requests/1
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

%% default number of messages in buffer
-define(DEFAULT_PARITION_BUFFER_LIMIT, 256).
%% default number of message message-sets on wire
-define(DEFAULT_PARITION_ONWIRE_LIMIT, 128).
%% by default, send 1 MB message-set
-define(DEFAULT_MESSAGE_SET_BYTES_LIMIT, 1048576).
%% by default, require acks from all ISR
-define(DEFAULT_REQUIRED_ACKS, -1).
%% by default, leader should wait 1 second for replicas to ack
-define(DEFAULT_ACK_TIMEOUT, 1000).

-define(CALLER_PENDING_ON_BUF(Caller), {caller_pending_on_buf, Caller}).
-define(CALLER_PENDING_ON_ACK(Caller), {caller_pending_on_ack, Caller}).

-type caller_state() :: ?CALLER_PENDING_ON_BUF(brod_produce_call())
                      | ?CALLER_PENDING_ON_ACK(brod_produce_call()).

-record(req,
        { caller :: caller_state()
        , key    :: binary()
        , value  :: binary()
        }).

-record(state,
        { client_id      :: client_id()
        , topic          :: topic()
        , partition      :: partition()
        , config         :: producer_config()
        , sock_pid       :: pid()
        , topic_producer :: pid()
        %% TODO start-link a writer process to buffer
        %% to avoid huge log dumps in case of crash etc.
        , buffer = []    :: [#req{}]
        , onwire = []    :: [{corr_id(), [caller_state()]}]
        }).

%%%_* APIs ---------------------------------------------------------------------

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
%%     caller.
%%   partition_onwire_limit(optional, default = 128):
%%     How many requests (per-partition) can be sent to kafka broker
%%     asynchronously before receiving ACKs from broker.
%%   message_set_bytes_limit(optional, default = 1M):
%%     In case callers are producing faster than brokers can handle (or
%%     congestion on wire), try to accumulate small requests as much as possible
%%     but not exceeding message_set_bytes_limit.
%% @end
-spec start_link(client_id(), topic(), partition(), producer_config()) ->
        {ok, pid()}.
start_link(ClientId, Topic, Partition, Config) when is_atom(ClientId) ->
  gen_server:start_link(?MODULE, {ClientId, Topic, Partition, Config}, []).

%% @doc Produce a message to partition asynchronizely.
%% The call is blocked until the request has been buffered in producer worker
%% The function returns a call reference of type brod_produce_call() to the
%% caller so the caller can used it to expect (match) a BROD_PRODUCE_REQ_ACKED
%% message after the produce request has been acked by configured number of
%% replicas in kafka cluster.
%% @end
-spec produce(pid(), binary(), binary()) ->
        {ok, brod_produce_call()} | {error, any()}.
produce(Pid, Key, Value) ->
  MRef = erlang:monitor(process, Pid),
  Call = ?BROD_PRODUCE_CALL(self(), MRef),
  ok = gen_server:cast(Pid, {produce, Call, Key, Value}),
  ExpectedReply = #brod_produce_reply{ call   = Call
                                     , result = brod_produce_req_buffered
                                     },
  %% Wait until the request is buffered
  case sync_produce_requests([ExpectedReply]) of
    ok              -> {ok, Call};
    {error, Reason} -> {error, Reason}
  end.

%% @doc Block sync the produce requests. The caller pid of this function
%% Must be the caller of produce/3 in which the call reference was created
%% @end
-spec sync_produce_requests([brod_produce_reply()]) ->
        ok | {error, Reason::any()}.
sync_produce_requests([]) -> ok;
sync_produce_requests([#brod_produce_reply{ call   = ?BROD_PRODUCE_CALL(_, Mref)
                                          , result = Result
                                          } = Reply | Replies]) ->
  receive
    {'DOWN', Mref, process, _Pid, Reason} ->
      {error, {producer_down, Reason}};
    Reply ->
      %% demonitor if this is the last reply.
      Result =:= brod_produce_req_acked andalso erlang:demonitor(Mref, [flush]),
      sync_produce_requests(Replies)
  end.

%%%_* gen_server callbacks------------------------------------------------------

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
                               } = State) ->
  {ok, Metadata} = brod_client:get_metadata(ClientId, Topic),
  #metadata_response{ brokers = Brokers
                    , topics  = [TopicMetadata]
                    } = Metadata,
  #topic_metadata{ error_code = TopicErrorCode
                 , partitions = Partitions
                 } = TopicMetadata,
  brod_kafka:is_error(TopicErrorCode) orelse
    erlang:throw({"topic metadata error", TopicErrorCode}),
  #partition_metadata{ error_code = PartitionErrorCode
                     , leader_id  = LeaderId} =
    lists:keyfind(Partition, #partition_metadata.id, Partitions),
  brod_kafka:is_error(PartitionErrorCode) orelse
    erlang:throw({"partition metadata error", PartitionErrorCode}),
  LeaderId >= 0 orelse
    erlang:throw({"no leader for partition", {ClientId, Topic, Partition}}),
  #broker_metadata{host = Host, port = Port} =
    lists:keyfind(LeaderId, #broker_metadata.node_id, Brokers),
  {ok, SockPid} = brod_client:connect_broker(ClientId, Host, Port),
  _ = erlang:monitor(process, SockPid),
  ok = brod_client:register_producer(ClientId, Topic, Partition),
  NewState = State#state{sock_pid = SockPid},
  {noreply, NewState};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{sock_pid = Pid} = State) ->
  {stop, {socket_down, Reason}, State};
handle_info({msg, Pid, CorrId, #produce_response{} = R},
            #state{sock_pid = Pid} = State) ->
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
      {ok, NewState} = handle_produce_response(State, CorrId),
      {noreply, NewState}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(_Call, _From, State) ->
  {reply, {error, {unsupported_call, _Call}}, State}.

handle_cast({produce, Call, Key, Value}, #state{} = State) ->
  Req = #req{ caller = ?CALLER_PENDING_ON_BUF(Call)
            , key    = Key
            , value  = Value
            },
  {ok, NewState} = handle_produce_request(State, Req),
  {noreply, NewState};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  %% TODO: log
  ok.

%%%_* Internal functions -------------------------------------------------------

-spec handle_produce_request(#state{}, #req{}) -> {ok, #state{}}.
handle_produce_request(#state{buffer = Buffer} = State, Req) ->
  BufferCountLimit = get_buffer_count_limit(State),
  NewBuffer = reply_buffered(BufferCountLimit, Buffer ++ [Req]),
  NewState = State#state{buffer = NewBuffer},
  maybe_send_reqs(NewState).

-spec maybe_send_reqs(#state{}) -> {ok, #state{}}.
maybe_send_reqs(#state{buffer = []} = State) ->
  {ok, State};
maybe_send_reqs(#state{} = State) ->
  OnWireMsgSetCountLimit = get_onwire_count_limit(State),
  maybe_send_reqs(State, OnWireMsgSetCountLimit).

-spec maybe_send_reqs(#state{}, pos_integer()) -> {ok, #state{}}.
maybe_send_reqs(#state{onwire = OnWire} = State, OnWireMsgSetCountLimit)
 when length(OnWire) >= OnWireMsgSetCountLimit ->
  {ok, State};
maybe_send_reqs(#state{ buffer    = Buffer
                      , onwire    = OnWire
                      , sock_pid  = SockPid
                      , topic     = Topic
                      , partition = Partition
                      } = State, _OnWireCntLimit) ->
  MessageSetBytesLimit = get_message_set_bytes(State),
  {Callers, MessageSet, NewBuffer} = get_message_set(Buffer, MessageSetBytesLimit),
  RequiredAcks = get_required_acks(State),
  %% TODO: change to plain kv-list ?
  PartitionDict = dict:from_list([{Partition, MessageSet}]),
  Data = [{Topic, PartitionDict}],
  KafkaReq = #produce_request{ acks    = RequiredAcks
                             , timeout = get_ack_timeout(State)
                             , data    = Data
                             },
  {ok, CorrId} = brod_sock:send(SockPid, KafkaReq),
  NewState =
    State#state{ buffer = NewBuffer
               , onwire = OnWire ++ [{CorrId, Callers}]
               },
  %% in case require no ack, fake one as if received from kafka
  case RequiredAcks =:= 0 of
    true  -> handle_produce_response(NewState, CorrId);
    false -> {ok, NewState}
  end.

%% @private Get the next message-set to send from buffer.
-spec get_message_set([#req{}], pos_integer()) ->
        {[caller_state()], [kafka_kv()], [#req{}]}.
get_message_set(Buffer, MessageSetBytesLimit) ->
  get_message_set(Buffer, MessageSetBytesLimit, [], [], 0).

get_message_set([], _BytesLimit, Callers, KVList, _Bytes) ->
  %% no request left
  {lists:reverse(Callers), lists:reverse(KVList), []};
get_message_set([#req{ caller = CallerState
                     , key    = Key
                     , value  = Value
                     } | Buffer], BytesLimit, Callers, KVList, Bytes) ->
  NewBytes = Bytes + size(Key) + size(Value),
  case NewBytes > BytesLimit of
    true when Bytes > 0 ->
      %% keep accumulating would exceed limit
      {lists:reverse(Callers), lists:reverse(KVList), Buffer};
    true when Bytes =:= 0 ->
      %% the first request itself exceeded limit
      [] = KVList, %% assert
      NewCallerState = maybe_reply_buffered(CallerState),
      {[NewCallerState], [{Key, Value}], Buffer};
    false ->
      %% keep pulling in messages to message-set
      NewCallerState = maybe_reply_buffered(CallerState),
      get_message_set(Buffer, BytesLimit,
                     [NewCallerState | Callers],
                     [{Key, Value} | KVList], NewBytes)
  end.

%% @private Reply PRODUCE_REQ_BUFFERED to caller for the first N requests,
%% Keep the callers of the ones at tail blocked.
%% @end
-spec reply_buffered(non_neg_integer(), [#req{}]) -> [#req{}].
reply_buffered(FirstN, Reqs) ->
  reply_buffered(FirstN, Reqs, []).

reply_buffered(0, Reqs, Replied) ->
  lists:reverse(Replied) ++ Reqs;
reply_buffered(_N, [], Replied) ->
  lists:reverse(Replied);
reply_buffered(N, [Req | Reqs], Replied) ->
  NewCallerState = maybe_reply_buffered(Req#req.caller),
  NewReq = Req#req{caller = NewCallerState},
  reply_buffered(N-1, Reqs, [NewReq | Replied]).

%% @private Reply buffered if the caller is pending on ?PRODUCE_REQ_BUFFERED.
-spec maybe_reply_buffered(caller_state()) -> caller_state().
maybe_reply_buffered(?CALLER_PENDING_ON_BUF(Caller)) ->
  ok = do_reply_buffered(Caller),
  ?CALLER_PENDING_ON_ACK(Caller);
maybe_reply_buffered(?CALLER_PENDING_ON_ACK(Caller)) ->
  %% already replied
  ?CALLER_PENDING_ON_ACK(Caller).

-spec handle_produce_response(#state{}, corr_id()) -> {ok, #state{}}.
handle_produce_response(#state{onwire = [{CorrId, Callers} | OnWire]} = State, CorrId) ->
  lists:foreach(fun(?CALLER_PENDING_ON_ACK(Caller)) ->
                  do_reply_acked(Caller)
                end, Callers),
  {ok, State#state{onwire = OnWire}};
handle_produce_response(#state{onwire = OnWire}, CorrId) ->
  erlang:error({"unexpected response", CorrId, OnWire}).

-spec do_reply_buffered(brod_produce_call()) -> ok.
do_reply_buffered(?BROD_PRODUCE_CALL(Pid, _Ref) = Call) ->
  Reply = #brod_produce_reply{ call   = Call
                             , result = brod_produce_req_buffered
                             },
  safe_send(Pid, Reply).

-spec do_reply_acked(brod_produce_call()) -> ok.
do_reply_acked(?BROD_PRODUCE_CALL(Pid, _Ref) = Call) ->
  Reply = #brod_produce_reply{ call   = Call
                             , result = brod_produce_req_acked
                             },
  safe_send(Pid, Reply).

safe_send(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

-spec get_buffer_count_limit(#state{}) -> non_neg_integer().
get_buffer_count_limit(#state{config = Config}) ->
  case proplists:get_value(partition_buffer_limit, Config) of
    ?undef ->
      ?DEFAULT_PARITION_BUFFER_LIMIT;
    N when is_integer(N) andalso N >= 0 ->
      N
  end.

-spec get_onwire_count_limit(#state{}) -> pos_integer().
get_onwire_count_limit(#state{config = Config}) ->
  case proplists:get_value(partition_onwire_limit, Config) of
    ?undef ->
      ?DEFAULT_PARITION_ONWIRE_LIMIT;
    N when is_integer(N) andalso N > 0 ->
      N
  end.

-spec get_message_set_bytes(#state{}) -> pos_integer().
get_message_set_bytes(#state{config = Config}) ->
  case proplists:get_value(message_set_bytes_limit, Config) of
    ?undef ->
      ?DEFAULT_MESSAGE_SET_BYTES_LIMIT;
    N when is_integer(N) andalso N > 0 ->
      N
  end.

get_required_acks(#state{config = Config}) ->
  case proplists:get_value(required_acks, Config) of
    ?undef ->
      ?DEFAULT_REQUIRED_ACKS;
    N when is_integer(N) ->
      N
  end.

get_ack_timeout(#state{config = Config}) ->
  case proplists:get_value(ack_timeout, Config) of
    ?undef ->
      ?DEFAULT_ACK_TIMEOUT;
    N when is_integer(N) andalso N > 0 ->
      N
  end.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
