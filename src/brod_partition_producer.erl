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

-module(brod_partition_producer).
-behaviour(gen_server).

-export([ start_link/1
        , produce/4
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-define(undef, undefined).

%% by default, keep 32 message in buffer
-define(DEFAULT_PARITION_BUFFER_LIMIT, 32).
%% by default, allow 16 message-sets on wire
-define(DEFAULT_PARITION_ONWIRE_LIMIT, 16).
%% by default, send 1 MB message-set
-define(DEFAULT_MESSAGE_SET_BYTES, 1048576).
%% by default, require acks from all ISR
-define(DEFAULT_REQUIRED_ACKS, -1).
%% by default, leader should wait 1 second for replicas to ack
-define(DEFAULT_ACK_TIMEOUT, 1000).

-define(CALLER_PENDING_ON_BUF(Caller), {caller_pending_on_buf, Caller}).
-define(CALLER_PENDING_ON_ACK(Caller), {caller_pending_on_ack, Caller}).

-type caller_state() :: ?CALLER_PENDING_ON_BUF(produce_caller())
                      | ?CALLER_PENDING_ON_ACK(produce_caller()).

-record(req,
        { caller :: caller_state()
        , key    :: binary()
        , value  :: binary()
        }).

-record(state,
        { client_id :: client_id()
        , topic     :: topic()
        , partition :: partition()
        , config    :: producer_config()
        , sock_pid  :: pid()
        %% TODO start-link a writer process to buffer
        %% to avoid huge log dumps in case of crash etc.
        , buffer = [] :: [#req{}]
        , onwire = [] :: [{corr_id(), [caller_state()]}]
        }).

%%%_* APIs ---------------------------------------------------------------------

-spec start_link({client_id(), cluster_id(), kafka_hosts(),
                 topic(), partition(), producer_config()}) -> {ok, pid()}.
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

-spec produce(pid(), produce_caller(), binary(), binary()) -> ok.
produce(Pid, Caller, Key, Value) ->
  Req = #req{ caller = ?CALLER_PENDING_ON_BUF(Caller)
            , key    = Key
            , value  = Value
            },
  gen_server:cast(Pid, {produce, Req}).

%%%_* gen_server callbacks------------------------------------------------------

init({ClientId, Topic, Partition, Config}) ->
  {ok, Metadata} = brod_client:get_metadata(ClientId, Topic),
  #metadata_response{ brokers = Brokers
                    , topics  = [TopicMetadata]
                    } = Metadata,
  #topic_metadata{ error_code = TopicErrorCode
                 , partitions = Partitions
                 } = TopicMetadata,
  brod_kakfa:is_error(TopicErrorCode) orelse
    erlang:throw({"topic metadata error", TopicErrorCode}),
  #partition_metadata{leader_id = LeaderId} =
    lists:keyfind(Partition, #partition_metadata.id, Partitions),
  LeaderId >= 0 orelse
    erlang:throw({"no leader for partition", {ClientId, Topic, Partition}}),
  #broker_metadata{host = Host, port = Port} =
    lists:keyfind(LeaderId, #broker_metadata.node_id, Brokers),
  {ok, SockPid} = brod_client:connect_broker(ClientId, Host, Port),
  _ = erlang:monitor(process, SockPid),
  {ok, #state{ client_id  = ClientId
             , topic      = Topic
             , partition  = Partition
             , config     = Config
             , sock_pid   = SockPid
             }}.

handle_call(_Call, _From, State) ->
  {reply, {error, {unknown_call, _Call}}, State}.

handle_cast({produce, Req}, State) ->
  {ok, NewState} = handle_produce_request(State, Req),
  {noreply, NewState};
handle_cast(_Cast, State) ->
  {noreply, State}.

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
                      } = State, _OnWireCntLimit) ->
  MessageSetBytesLimit = get_message_set_bytes(State),
  {Callers, MessageSet, NewBuffer} = get_message_set(Buffer, MessageSetBytesLimit),
  KafkaReq = #produce_request{ acks    = get_required_acks(State)
                             , timeout = get_ack_timeout(State)
                             , data    = MessageSet
                             },
  {ok, CorrId} = brod_sock:send(SockPid, KafkaReq),
  {ok, State#state{ buffer = NewBuffer
                  , onwire = OnWire ++ [{CorrId, Callers}]
                  }}.

get_message_set(Buffer, MessageSetBytesLimit) ->
  get_message_set(Buffer, MessageSetBytesLimit, [], [], 0).

get_message_set([], _BytesLimit, Callers, KVList, _Bytes) ->
  {lists:reverse(Callers), lists:reverse(KVList), []};
get_message_set(Buffer, BytesLimit, Callers, KVList, Bytes) when Bytes >= BytesLimit ->
  {lists:reverse(Callers), lists:reverse(KVList), Buffer};
get_message_set([#req{ caller = CallerState
                     , key    = Key
                     , value  = Value
                     } | Buffer], BytesLimit, Callers, KVList, Bytes) ->
  NewCallerState = maybe_reply_buffered(CallerState),
  NewBytes = Bytes + size(Key) + size(Value),
  get_message_set(Buffer, BytesLimit,
                  [NewCallerState | Callers],
                  [{Key, Value} | KVList], NewBytes).

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
  case proplists:get_value(message_set_size_limit, Config) of
    ?undef ->
      ?DEFAULT_MESSAGE_SET_BYTES;
    N when is_integer(N) andalso N > 0 ->
      N
  end.

-spec handle_produce_response(#state{}, corr_id()) -> {ok, #state{}}.
handle_produce_response(#state{onwire = [{CorrId, Callers} | OnWire]} = State, CorrId) ->
  lists:foreach(fun(?CALLER_PENDING_ON_ACK(Caller)) ->
                  do_reply_acked(Caller)
                end, Callers),
  {ok, State#state{onwire = OnWire}};
handle_produce_response(#state{onwire = OnWire}, CorrId) ->
  erlang:error({"unexpected response", CorrId, OnWire}).

-spec do_reply_buffered({reference(), pid()}) -> ok.
do_reply_buffered({Ref, Pid}) ->
  safe_send(Pid, ?BROD_PRODUCE_REQ_BUFFERED(Ref)).

-spec do_reply_acked({reference(), pid()}) -> ok.
do_reply_acked({Ref, Pid}) ->
  safe_send(Pid, ?BROD_PRODUCE_REQ_ACKED(Ref)).

safe_send(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
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
