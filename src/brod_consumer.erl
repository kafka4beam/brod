
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

-module(brod_consumer).

-behaviour(gen_server).

%% Server API
-export([ ack/2
        , start_link/4
        , start_link/5
        , stop/1
        , subscribe/3
        , unsubscribe/1
        ]).

%% Debug API
-export([ debug/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-include("brod.hrl").
-include("brod_int.hrl").

-type options() :: consumer_options().

-record(state, { client_pid        :: pid()
               , socket_pid        :: pid()
               , topic             :: binary()
               , partition         :: integer()
               , begin_offset      :: integer()
               , max_wait_time     :: integer()
               , min_bytes         :: integer()
               , max_bytes         :: integer()
               , sleep_timeout     :: integer()
               , prefetch_count    :: integer()
               , last_corr_id      :: corr_id()
               , subscriber        :: ?undef | pid()
               , subscriber_mref   :: ?undef | reference()
               , pending_acks = [] :: [offset()]
               , is_suspended      :: boolean()
               }).

-define(DEFAULT_BEGIN_OFFSET, -1).
-define(DEFAULT_MIN_BYTES, 0).
-define(DEFAULT_MAX_BYTES, 1048576).  % 1 MB
-define(DEFAULT_MAX_WAIT_TIME, 10000). % 10 sec
-define(DEFAULT_SLEEP_TIMEOUT, 1000). % 1 sec
-define(DEFAULT_PREFETCH_COUNT, 1).
-define(ERROR_COOLDOWN, 1000).
-define(SOCKET_RETRY_DELAY_MS, 1000).

-define(SEND_FETCH_REQUEST, send_fetch_request).
-define(INIT_SOCKET, init_socket).

%%%_* APIs =====================================================================
%% @equiv start_link(ClientPid, Topic, Partition, Config, [])
-spec start_link(pid(), topic(), partition(),
                 consumer_config()) -> {ok, pid()} | {error, any()}.
start_link(ClientPid, Topic, Partition, Config) ->
  start_link(ClientPid, Topic, Partition, Config, []).

%% @doc Start (link) a partition consumer.
%% Possible configs:
%%   min_bytes (optional default = 0):
%%     Minimal bytes to fetch in a batch of messages
%%   max_bytes (optional default = 1MB):
%%     Maximum bytes to fetch in a batch of messages
%%     NOTE: this value might be doubled in each retry when it is not enough
%%           to fetch even one single message.
%%     NOTE: in current implementation, the value is not shrinked back to
%%           original after it has been expanded.
%%  max_wait_time (optional, default = 10000 ms):
%%     Max number of seconds allowd for the broker to collect min_bytes of
%%     messages in fetch response
%%  sleep_timeout (optional, default = 1000 ms):
%%     Allow consumer process to sleep this amout of ms if kafka replied
%%     'empty' message-set.
%%  prefetch_count (optional, default = 1):
%%     The window size (number of messages) allowed to fetch-ahead.
%%  begin_offset (optional, default = -1):
%%     The offset from which to begin fetch requests.
%% @end
-spec start_link(pid(), topic(), partition(),
                 consumer_config(), [any()]) -> {ok, pid()} | {error, any()}.
start_link(ClientPid, Topic, Partition, Config, Debug) ->
  Args = {ClientPid, Topic, Partition, Config},
  gen_server:start_link(?MODULE, Args, [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) -> safe_gen_call(Pid, stop, infinity).

%% @doc Subscribe or resubscribe on messages from a partition.
%% Caller may pass in a set of options which is an extention of consumer config
%% to update the parameters such as max_bytes and max_wait_time etc.
%% also to update the start point (begin_offset) of the data stream.
%% Possible options:
%%   all consumer configs as documented for start_link/5
%%   begin_offset (optional, default = -1)
%%     A subscriber may consume and process messages then persist the associated
%%     offset to a persistent storage, then start (or restart) with
%%     last_processed_offset + 1 as the begin_offset to proceed.
%%     By default, it fetches from the latest available offset (-1)
%% @end
-spec subscribe(pid(), pid(), options()) -> ok | {error, any()}.
subscribe(Pid, SubscriberPid, ConsumerOptions) ->
  safe_gen_call(Pid, {subscribe, SubscriberPid, ConsumerOptions}, infinity).

%% @doc Unsubscribe the current subscriber.
-spec unsubscribe(pid()) -> ok.
unsubscribe(Pid) -> safe_gen_call(Pid, unsubscribe, infinity).

%% @doc Subscriber confirms that a message (identified by offset) has been
%% consumed, consumer process now may continue to fetch more messages.
%% @end
-spec ack(pid(), offset()) -> ok.
ack(Pid, Offset) -> safe_gen_call(Pid, {ack, Offset}, infinity).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable/disable debugging on the consumer process.
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
debug(Pid, none) ->
  do_debug(Pid, no_debug);
debug(Pid, print) ->
  do_debug(Pid, {trace, true});
debug(Pid, File) when is_list(File) ->
  do_debug(Pid, {log_to_file, File}).

%%%_* gen_server callbacks =====================================================

init({ClientPid, Topic, Partition, Config}) ->
  self() ! ?INIT_SOCKET,
  MinBytes = proplists:get_value(min_bytes, Config, ?DEFAULT_MIN_BYTES),
  MaxBytes = proplists:get_value(max_bytes, Config, ?DEFAULT_MAX_BYTES),
  MaxWaitTime =
    proplists:get_value(max_wait_time, Config, ?DEFAULT_MAX_WAIT_TIME),
  SleepTimeout =
    proplists:get_value(sleep_timeout, Config, ?DEFAULT_SLEEP_TIMEOUT),
  PrefetchCount =
    proplists:get_value(prefetch_count, Config, ?DEFAULT_PREFETCH_COUNT),
  Offset = proplists:get_value(begin_offset, Config, ?undef),
  {ok, #state{ client_pid     = ClientPid
             , topic          = Topic
             , partition      = Partition
             , begin_offset   = Offset
             , max_wait_time  = MaxWaitTime
             , min_bytes      = MinBytes
             , max_bytes      = MaxBytes
             , sleep_timeout  = SleepTimeout
             , prefetch_count = PrefetchCount
             , socket_pid     = ?undef
             , is_suspended   = false
             }}.

handle_info(?INIT_SOCKET,
            #state{ client_pid = ClientPid
                  , topic      = Topic
                  , partition  = Partition
                  , socket_pid = ?undef
                  } = State0) ->
  %% 1. Lookup, or maybe (re-)establish a connection to partition leader
  case brod_client:get_leader_connection(ClientPid, Topic, Partition) of
    {ok, SocketPid} ->
      _ = erlang:monitor(process, SocketPid),
      State1 = State0#state{socket_pid = SocketPid},
      ok = brod_client:register_consumer(ClientPid, Topic, Partition),
      State = maybe_send_fetch_request(State1),
      {noreply, State};
    {error, _Reason} ->
      Timeout = ?SOCKET_RETRY_DELAY_MS,
      erlang:send_after(Timeout, self(), ?INIT_SOCKET),
      {noreply, State0}
  end;
handle_info({msg, _Pid, CorrId, R}, State) ->
  handle_fetch_response(R, CorrId, State);
handle_info(?SEND_FETCH_REQUEST, State0) ->
  State = maybe_send_fetch_request(State0),
  {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{subscriber = Pid} = State) ->
  error_logger:info_msg("~p ~p subscriber ~p is down\nreason:~p",
                        [?MODULE, self(), Pid, Reason]),
  NewState = reset_buffer(State#state{ subscriber      = ?undef
                                     , subscriber_mref = ?undef
                                     }),
  {noreply, NewState};
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            #state{socket_pid = Pid} = State) ->
  Timeout = ?SOCKET_RETRY_DELAY_MS,
  erlang:send_after(Timeout, self(), ?INIT_SOCKET),
  State1 = State#state{socket_pid = ?undef},
  NewState = reset_buffer(State1),
  {noreply, NewState};
handle_info(Info, State) ->
  error_logger:warning_msg("~p ~p got unexpected info: ~p",
                          [?MODULE, self(), Info]),
  {noreply, State}.

handle_call({subscribe, _Pid, _Options}, _From,
            #state{ socket_pid = ?undef} = State) ->
  {reply, {error, no_connection}, State};
handle_call({subscribe, Pid, Options}, _From,
            #state{ subscriber = Subscriber
                  , subscriber_mref = OldMref} = State0) ->
  case Subscriber =:= ?undef orelse
      not is_process_alive(Subscriber) orelse
      Subscriber =:= Pid of
    true ->
      case update_options(Options, State0) of
        {ok, State1} ->
          %% demonitor in case the same process tries to subscribe again
          is_reference(OldMref) andalso erlang:demonitor(OldMref, [flush]),
          Mref = erlang:monitor(process, Pid),
          State2 = State1#state{ subscriber      = Pid
                               , subscriber_mref = Mref
                               },
          %% always reset buffer to fetch again
          State3 = reset_buffer(State2),
          State4 = State3#state{is_suspended = false},
          State  = maybe_send_fetch_request(State4),
          {reply, ok, State};
        {error, Reason} ->
          {reply, {error, Reason}, State0}
      end;
    false ->
      {reply, {error, {already_subscribed_by, Subscriber}}, State0}
  end;
handle_call(unsubscribe, _From, #state{subscriber_mref = Mref} = State) ->
  is_reference(Mref) andalso erlang:demonitor(Mref, [flush]),
  NewState = State#state{ subscriber      = ?undef
                        , subscriber_mref = ?undef
                        },
  {reply, ok, reset_buffer(NewState)};
handle_call({ack, Offset}, _From,
            #state{pending_acks = PendingAcks} = State0) ->
  NewPendingAcks = handle_ack(PendingAcks, Offset),
  State1 = State0#state{pending_acks = NewPendingAcks},
  State = maybe_send_fetch_request(State1),
  {reply, ok, State};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast(Cast, State) ->
  error_logger:warning_msg("~p ~p got unexpected cast: ~p",
                          [?MODULE, self(), Cast]),
  {noreply, State}.

terminate(Reason, _State) ->
  error_logger:warning_msg("~p ~p terminating, reason:\n~p",
                           [?MODULE, self(), Reason]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal Functions =======================================================

do_debug(Pid, Debug) ->
  {ok, _} = gen:call(Pid, system, {debug, Debug}, infinity),
  ok.

handle_fetch_response(_Response, _CorrId,
                      #state{subscriber = ?undef} = State) ->
  %% discard fetch response when there is no (dead?) subscriber
  {noreply, State};
handle_fetch_response(_Response, CorrId1,
                      #state{ last_corr_id = CorrId2
                            } = State) when CorrId1 < CorrId2 ->
  {noreply, State};
handle_fetch_response(#kpro_FetchResponse{ fetchResponseTopic_L = [TopicData]
                                         }, CorrId, State) ->
  CorrId = State#state.last_corr_id, %% assert
  #kpro_FetchResponseTopic{ topicName = Topic
                          , fetchResponsePartition_L = [PM]
                          } = TopicData,
  #kpro_FetchResponsePartition{ partition           = Partition
                              , errorCode           = ErrorCode
                              , highWatermarkOffset = HighWmOffset
                              , message_L           = Messages0} = PM,
  Messages = map_messages(Messages0),
  case kpro_ErrorCode:is_error(ErrorCode) of
    true ->
      Error = #kafka_fetch_error{ topic      = Topic
                                , partition  = Partition
                                , error_code = ErrorCode
                                , error_desc = kpro_ErrorCode:desc(ErrorCode)
                                },
      handle_fetch_error(Error, State);
    false ->
      MsgSet = #kafka_message_set{ topic          = Topic
                                 , partition      = Partition
                                 , high_wm_offset = HighWmOffset
                                 , messages       = Messages
                                 },
      handle_message_set(MsgSet, State)
  end.

handle_message_set(#kafka_message_set{messages = []}, State0) ->
  State = maybe_delay_fetch_request(State0),
  {noreply, State};
handle_message_set(#kafka_message_set{messages = [?incomplete_message]},
                   #state{max_bytes = MaxBytes} = State0) ->
  NewMaxBytes = MaxBytes * 2,
  NewMaxBytes >= (1 bsl 31) andalso
      erlang:error({max_bytes_overflow,
                   <<"perhaps message corruption in broker?">>}),
  error_logger:warning_msg("~p ~p max_bytes ~p is too small, trying with ~p",
                           [?MODULE, self(), MaxBytes, NewMaxBytes]),
  State1 = State0#state{max_bytes = NewMaxBytes},
  State = maybe_send_fetch_request(State1),
  {noreply, State};
handle_message_set(#kafka_message_set{messages = Messages} = MsgSet,
                   #state{ subscriber    = Subscriber
                         , pending_acks  = PendingAcks
                         } = State0) ->
  ok = cast_to_subscriber(Subscriber, MsgSet),
  MapFun = fun(#kafka_message{offset = Offset}) -> Offset end,
  Offsets = lists:map(MapFun, Messages),
  LastOffset = lists:last(Offsets),
  State = State0#state{ pending_acks = PendingAcks ++ Offsets
                      , begin_offset = LastOffset + 1
                      },
  NewState = maybe_send_fetch_request(State),
  {noreply, NewState}.

err_op(?EC_REQUEST_TIMED_OUT)          -> retry;
err_op(?EC_UNKNOWN_TOPIC_OR_PARTITION) -> stop;
err_op(?EC_INVALID_TOPIC_EXCEPTION)    -> stop;
err_op(?EC_OFFSET_OUT_OF_RANGE)        -> suspend;
err_op(_)                              -> restart.

%% @private Map message to brod's format.
%% incomplete message indicator is kept when the only one message is incomplete.
%% @end
map_messages([?incomplete_message]) ->
  [?incomplete_message];
map_messages(Messages) ->
  F = fun(#kpro_Message{} = M) ->
            #kafka_message{ offset     = M#kpro_Message.offset
                          , magic_byte = M#kpro_Message.magicByte
                          , attributes = M#kpro_Message.attributes
                          , key        = M#kpro_Message.key
                          , value      = M#kpro_Message.value
                          }
      end,
  [F(M) || M <- Messages, M =/= ?incomplete_message].

handle_fetch_error(#kafka_fetch_error{error_code = ErrorCode} = Error,
                   #state{ topic      = Topic
                         , partition  = Partition
                         , subscriber = Subscriber
                         } = State) ->
  case err_op(ErrorCode) of
    retry ->
      {noreply, maybe_send_fetch_request(State)};
    stop ->
      ok = cast_to_subscriber(Subscriber, Error),
      error_logger:error_msg("consumer of topic ~p partition ~p shutdown, "
                             "reason: ~p", [Topic, Partition, ErrorCode]),
      {stop, normal, State};
    suspend ->
      ok = cast_to_subscriber(Subscriber, Error),
      %% Suspend, no more fetch request until the subscriber re-subscribes
      {noreply, State#state{is_suspended = true}};
    restart ->
      ok = cast_to_subscriber(Subscriber, Error),
      {stop, {restart, ErrorCode}, State}
  end.

handle_ack([], _Offset) -> [];
handle_ack([H | Offsets], Offset) ->
  case H =< Offset of
    true  -> handle_ack(Offsets, Offset);
    false -> [H | Offsets]
  end.

cast_to_subscriber(Pid, Msg) ->
  try
    Pid ! {self(), Msg},
    ok
  catch _ : _ ->
    ok
  end.

-spec maybe_delay_fetch_request(#state{}) -> ok.
maybe_delay_fetch_request(#state{sleep_timeout = T} = State) when T > 0 ->
  _ = erlang:send_after(T, self(), ?SEND_FETCH_REQUEST),
  State;
maybe_delay_fetch_request(State) ->
  maybe_send_fetch_request(State).

%% @private Send new fetch request if no pending error.
maybe_send_fetch_request(#state{ subscriber     = Subscriber
                               , pending_acks   = PendingAcks
                               , prefetch_count = PrefetchCount
                               , socket_pid     = SocketPid
                               , is_suspended   = IsSuspended
                               } = State) ->
  case is_pid(SocketPid)  andalso
       is_pid(Subscriber) andalso
       not IsSuspended    andalso
       length(PendingAcks) =< PrefetchCount of
    true ->
      case send_fetch_request(State) of
        {ok, CorrId} ->
          State#state{last_corr_id = CorrId};
        {error, {sock_down, _Reason}} ->
          %% ignore error here, the socket pid 'DOWN' message
          %% should trigger the socket re-init loop
          State
      end;
    false ->
      State
  end.

send_fetch_request(#state{ begin_offset = BeginOffset
                         , socket_pid   = SocketPid
                         } = State) ->
  (is_integer(BeginOffset) andalso BeginOffset >= 0) orelse
    erlang:error({bad_begin_offset, BeginOffset}),
  Request =
    kpro:fetch_request(State#state.topic,
                       State#state.partition,
                       State#state.begin_offset,
                       State#state.max_wait_time,
                       State#state.min_bytes,
                       State#state.max_bytes),
  brod_sock:request_async(SocketPid, Request).

-spec update_options(options(), #state{}) -> {ok, #state{}} | {error, any()}.
update_options(Options, #state{begin_offset = OldBeginOffset} = State) ->
  F = fun(Name, Default) -> proplists:get_value(Name, Options, Default) end,
  DefaultBeginOffset = case OldBeginOffset =:= ?undef of
                         true  -> ?DEFAULT_BEGIN_OFFSET;
                         false -> OldBeginOffset
                       end,
  NewBeginOffset = F(begin_offset, DefaultBeginOffset),
  State1 =
    State#state{ begin_offset   = NewBeginOffset
               , min_bytes      = F(min_bytes, State#state.min_bytes)
               , max_bytes      = F(max_bytes, State#state.max_bytes)
               , max_wait_time  = F(max_wait_time, State#state.max_wait_time)
               , sleep_timeout  = F(sleep_timeout, State#state.sleep_timeout)
               , prefetch_count = F(prefetch_count, State#state.prefetch_count)
               },
  NewState =
    case NewBeginOffset =/= OldBeginOffset of
      true ->
        %% reset buffer in case subscriber wants to fetch from a new offset
        State1#state{pending_acks = []};
      false ->
        State1
    end,
  resolve_begin_offset(NewState).

-spec resolve_begin_offset(#state{}) -> {ok, #state{}} | {error, any()}.
resolve_begin_offset(#state{ begin_offset = BeginOffset
                           , socket_pid   = SocketPid
                           , topic        = Topic
                           , partition    = Partition
                           } = State) when BeginOffset < 0 ->
  case fetch_valid_offset(SocketPid, BeginOffset, Topic, Partition) of
    {ok, NewBeginOffset} ->
      {ok, State#state{begin_offset = NewBeginOffset}};
    {error, Reason} ->
      {error, Reason}
  end;
resolve_begin_offset(State) ->
  {ok, State}.

fetch_valid_offset(_S, Offset, _T, _P) when Offset >= 0 ->
  {ok, Offset};
fetch_valid_offset(SocketPid, Time, Topic, Partition) ->
  Request = kpro:offset_request(Topic, Partition, Time,
                                      _MaxNoOffsets = 1),
  {ok, Response} = brod_sock:request_sync(SocketPid, Request, 5000),
  #kpro_OffsetResponse{topicOffsets_L = [TopicOffsets]} = Response,
  #kpro_TopicOffsets{partitionOffsets_L = [PartitionOffsets]} = TopicOffsets,
  #kpro_PartitionOffsets{offset_L = Offsets} = PartitionOffsets,
  case Offsets of
    [Offset] -> {ok, Offset};
    []       -> {error, no_available_offsets}
  end.

%% @private Reset fetch buffer, use the last unacked offset as the next begin
%% offset to fetch data from.
%% @end
-spec reset_buffer(#state{}) -> #state{}.
reset_buffer(#state{pending_acks = []} = State) ->
  State;
reset_buffer(#state{pending_acks = [Offset | _]} = State) ->
  State#state{ begin_offset = Offset
             , pending_acks = []
             }.

%% @private Catch noproc exit exception when making gen_server:call.
-spec safe_gen_call(pid() | atom(), Call, Timeout) -> Return
        when Call    :: term(),
             Timeout :: infinity | integer(),
             Return  :: {ok, term()} | {error, consumer_down | term()}.
safe_gen_call(Server, Call, Timeout) ->
  try
    gen_server:call(Server, Call, Timeout)
  catch exit : {noproc, _} ->
    {error, consumer_down}
  end.

%%%_* Tests ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
