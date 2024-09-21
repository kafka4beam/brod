%%%   Copyright (c) 2014-2021 Klarna Bank AB (publ)
%%%   Copyright (c) 2022-2024 kafka4beam contributors
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

%% @doc Kafka consumers work in poll mode. In brod, `brod_consumer' is the poller,
%% which is constantly asking for more data from the kafka node which is a leader
%% for the given partition.
%%
%% By subscribing to `brod_consumer' a process should receive the polled message
%% sets (not individual messages) into its mailbox. Shape of the message is
%% documented at {@link brod:subscribe/5}.
%%
%% Messages processed by the subscriber has to be acked by calling
%% {@link ack/2} (or {@link brod:consume_ack/4}) to notify the consumer
%% that all messages before the acknowledged offsets are processed,
%% hence more messages can be fetched and sent to the subscriber and the
%% subscriber won't be overwhelmed by it.
%%
%% Each consumer can have only one subscriber.
%%
%% See the <a href="https://hexdocs.pm/brod/readme.html#consumers">overview</a> for
%% some more information and examples.
-module(brod_consumer).

-behaviour(gen_server).

%% Server API
-export([ ack/2
        , start_link/4
        , start_link/5
        , stop/1
        , stop_maybe_kill/2
        , subscribe/3
        , unsubscribe/2
        ]).

%% Debug API
-export([ debug/2
        , get_connection/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export_type([ config/0
             , offset_reset_policy/0
             , isolation_level/0
             ]).

-include("brod_int.hrl").
-include_lib("kafka_protocol/include/kpro_error_codes.hrl").

-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type offset_time() :: brod:offset_time().

%% Consumer options.
%%
%% Documented at {@link start_link/5}.
-type offset_reset_policy() :: reset_by_subscriber
                             | reset_to_earliest
                             | reset_to_latest.

-type bytes() :: non_neg_integer().
-define(PENDING(Offset, Bytes), {Offset, Bytes}).
-type pending() :: ?PENDING(offset(), bytes()).
-type pending_queue() :: queue:queue(pending()).
-type config() :: brod:consumer_config().

-record(pending_acks, { count = 0 :: non_neg_integer()
                      , bytes = 0 :: bytes()
                      , queue = queue:new() :: pending_queue()
                      }).

-type pending_acks() :: #pending_acks{}.
-type isolation_level() :: kpro:isolation_level().

-define(GET_FROM_CLIENT, get).
-define(IGNORE, ignore).
-record(state, { client_pid          :: ?IGNORE | pid()
               , bootstrap           :: ?IGNORE | ?GET_FROM_CLIENT | brod:bootstrap()
               , connection          :: ?undef | pid()
               , topic               :: binary()
               , partition           :: integer()
               , begin_offset        :: offset_time()
               , max_wait_time       :: integer()
               , min_bytes           :: bytes()
               , max_bytes_orig      :: bytes()
               , sleep_timeout       :: integer()
               , prefetch_count      :: integer()
               , last_req_ref        :: ?undef | reference()
               , subscriber          :: ?undef | pid()
               , subscriber_mref     :: ?undef | reference()
               , pending_acks        :: pending_acks()
               , is_suspended        :: boolean()
               , offset_reset_policy :: offset_reset_policy()
               , avg_bytes           :: number()
               , max_bytes           :: bytes()
               , size_stat_window    :: non_neg_integer()
               , prefetch_bytes      :: non_neg_integer()
               , connection_mref     :: ?undef | reference()
               , isolation_level     :: isolation_level()
               }).

-type state() :: #state{}.

-define(DEFAULT_BEGIN_OFFSET, ?OFFSET_LATEST).
-define(DEFAULT_MIN_BYTES, 0).
-define(DEFAULT_MAX_BYTES, 1048576).  % 1 MB
-define(DEFAULT_MAX_WAIT_TIME, 10000). % 10 sec
-define(DEFAULT_SLEEP_TIMEOUT, 1000). % 1 sec
-define(DEFAULT_PREFETCH_COUNT, 10).
%% For backward-compatibility,
%% keep default prefetch-bytes small
%% so prefetch-count can dominate fetch-ahead limit
-define(DEFAULT_PREFETCH_BYTES, 102400). % 100 KB
-define(DEFAULT_OFFSET_RESET_POLICY, reset_by_subscriber).
-define(CONNECTION_RETRY_DELAY_MS, 1000).

-define(SEND_FETCH_REQUEST, send_fetch_request).
-define(INIT_CONNECTION, init_connection).
-define(DEFAULT_AVG_WINDOW, 5).
-define(DEFAULT_ISOLATION_LEVEL, ?kpro_read_committed).
-define(DEFAULT_SHARE_LEADER_CONN, false).

%%%_* APIs =====================================================================
%% @equiv start_link(ClientPid, Topic, Partition, Config, [])
-spec start_link(pid() | brod:bootstrap(), topic(), partition(), config()) ->
        {ok, pid()} | {error, any()}.
start_link(Bootstrap, Topic, Partition, Config) ->
  start_link(Bootstrap, Topic, Partition, Config, []).

%% @doc Start (link) a partition consumer.
%%
%% Possible configs:
%% <ul>
%%   <li>`min_bytes' (optional, default = 0)
%%
%%     Minimal bytes to fetch in a batch of messages</li>
%%
%%   <li>`max_bytes' (optional, default = 1MB)
%%
%%     Maximum bytes to fetch in a batch of messages.
%%
%%     NOTE: this value might be expanded to retry when it is not
%%           enough to fetch even a single message, then slowly
%%           shrunk back to the given value.</li>
%%
%%  <li>`max_wait_time' (optional, default = 10000 ms)
%%
%%     Max number of seconds allowed for the broker to collect
%%     `min_bytes' of messages in fetch response</li>
%%
%%  <li>`sleep_timeout' (optional, default = 1000 ms)
%%
%%     Allow consumer process to sleep this amount of ms if kafka replied
%%     'empty' message set.</li>
%%
%%  <li>`prefetch_count' (optional, default = 10)
%%
%%     The window size (number of messages) allowed to fetch-ahead.</li>
%%
%%  <li>`prefetch_bytes' (optional, default = 100KB)
%%
%%     The total number of bytes allowed to fetch-ahead.
%%     `brod_consumer' is greed, it only stops fetching more messages in
%%     when number of unacked messages has exceeded `prefetch_count' AND
%%     the unacked total volume has exceeded `prefetch_bytes'</li>
%%
%%  <li>`begin_offset' (optional, default = latest)
%%
%%     The offset from which to begin fetch requests.
%%     A subscriber may consume and process messages, then persist the
%%     associated offset to a persistent storage, then start (or
%%     restart) from `last_processed_offset + 1' as the `begin_offset'
%%     to proceed. The offset has to already exist at the time of calling.</li>
%%
%%  <li>`offset_reset_policy' (optional, default = reset_by_subscriber)
%%
%%     How to reset `begin_offset' if `OffsetOutOfRange' exception is received.
%%
%%     `reset_by_subscriber': consumer is suspended
%%                           (`is_suspended=true' in state) and wait
%%                           for subscriber to re-subscribe with a new
%%                           `begin_offset' option.
%%
%%     `reset_to_earliest': consume from the earliest offset.
%%
%%     `reset_to_latest': consume from the last available offset.</li>
%%
%%  <li>`size_stat_window': (optional, default = 5)
%%
%%     The moving-average window size to calculate average message
%%     size.  Average message size is used to shrink `max_bytes' in
%%     fetch requests after it has been expanded to fetch a large
%%     message. Use 0 to immediately shrink back to original
%%     `max_bytes' from config.  A size estimation allows users to set
%%     a relatively small `max_bytes', then let it dynamically adjust
%%     to a number around `PrefetchCount * AverageSize'</li>
%%
%%  <li>`isolation_level': (optional, default = `read_committed')
%%
%%     Level to control what transaction records are exposed to the
%%     consumer. Two values are allowed, `read_uncommitted' to retrieve
%%     all records, independently on the transaction outcome (if any),
%%     and `read_committed' to get only the records from committed
%%     transactions</li>
%%
%%  <li>`share_leader_conn': (optional, default = `false')
%%
%%     Whether or not share the partition leader connection with
%%     other producers or consumers.
%%     Set to `true' to consume less TCP connections towards Kafka,
%%     but may lead to higher fetch latency. This is because Kafka can
%%     ony accumulate messages for the oldest fetch request, later
%%     requests behind it may get blocked until `max_wait_time' expires
%%     for the oldest one</li>
%%
%% </ul>
%% @end
-spec start_link(pid() | brod:bootstrap(),
                 topic(), partition(), config(), [any()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Bootstrap, Topic, Partition, Config, Debug) ->
  Args = {Bootstrap, Topic, Partition, Config},
  gen_server:start_link(?MODULE, Args, [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) -> safe_gen_call(Pid, stop, infinity).

-spec stop_maybe_kill(pid(), timeout()) -> ok.
stop_maybe_kill(Pid, Timeout) ->
  try
    gen_server:call(Pid, stop, Timeout)
  catch
    exit : {noproc, _} ->
      ok;
    exit : {timeout, _} ->
      exit(Pid, kill),
      ok
  end.

%% @doc Subscribe or resubscribe on messages from a partition.
%%
%% Caller may specify a set of options extending consumer config.
%% It is possible to update parameters such as `max_bytes' and
%% `max_wait_time', or the starting point (`begin_offset') of the data
%% stream. Note that you currently cannot update `isolation_level'.
%%
%% Possible options are documented at {@link start_link/5}.
-spec subscribe(pid(), pid(), config()) -> ok | {error, any()}.
subscribe(Pid, SubscriberPid, ConsumerOptions) ->
  safe_gen_call(Pid, {subscribe, SubscriberPid, ConsumerOptions}, infinity).

%% @doc Unsubscribe the current subscriber.
-spec unsubscribe(pid(), pid()) -> ok | {error, any()}.
unsubscribe(Pid, SubscriberPid) ->
  safe_gen_call(Pid, {unsubscribe, SubscriberPid}, infinity).

%% @doc Subscriber confirms that a message (identified by offset) has been
%% consumed, consumer process now may continue to fetch more messages.
-spec ack(pid(), brod:offset()) -> ok.
ack(Pid, Offset) ->
  gen_server:cast(Pid, {ack, Offset}).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable/disable debugging on the consumer process.
%%
%% `debug(Pid, print)' prints debug info to stdout.
%%
%% `debug(Pid, File)' prints debug info to a file `File'.
debug(Pid, none) ->
  do_debug(Pid, no_debug);
debug(Pid, print) ->
  do_debug(Pid, {trace, true});
debug(Pid, File) when is_list(File) ->
  do_debug(Pid, {log_to_file, File}).

%% @doc Get connection pid. Test/debug only.
get_connection(Pid) ->
  gen_server:call(Pid, get_connection).

%%%_* gen_server callbacks =====================================================

init({Bootstrap0, Topic, Partition, Config}) ->
  erlang:process_flag(trap_exit, true),
  Cfg = fun(Name, Default) ->
          proplists:get_value(Name, Config, Default)
        end,
  MinBytes = Cfg(min_bytes, ?DEFAULT_MIN_BYTES),
  MaxBytes = Cfg(max_bytes, ?DEFAULT_MAX_BYTES),
  MaxWaitTime = Cfg(max_wait_time, ?DEFAULT_MAX_WAIT_TIME),
  SleepTimeout = Cfg(sleep_timeout, ?DEFAULT_SLEEP_TIMEOUT),
  PrefetchCount = erlang:max(Cfg(prefetch_count, ?DEFAULT_PREFETCH_COUNT), 0),
  PrefetchBytes = erlang:max(Cfg(prefetch_bytes, ?DEFAULT_PREFETCH_BYTES), 0),
  BeginOffset = Cfg(begin_offset, ?DEFAULT_BEGIN_OFFSET),
  OffsetResetPolicy = Cfg(offset_reset_policy, ?DEFAULT_OFFSET_RESET_POLICY),
  IsolationLevel = Cfg(isolation_level, ?DEFAULT_ISOLATION_LEVEL),
  IsShareConn = Cfg(share_leader_conn, ?DEFAULT_SHARE_LEADER_CONN),

  %% resolve connection bootstrap args
  {ClientPid, Bootstrap} =
    case is_pid(Bootstrap0) of
      true when IsShareConn ->
        %% share leader connection with other producers/consumers
        %% the connection is to be managed by brod_client
        {Bootstrap0, ?IGNORE};
      true ->
        %% not sharing leader connection with other producers/consumers
        %% the bootstrap args will be resolved later when it's
        %% time to establish a connection to partition leader
        {Bootstrap0, ?GET_FROM_CLIENT};
      false ->
        %% this consumer process is not started from `brod' APIs
        %% maybe managed by other supervisors.
        {?IGNORE, Bootstrap0}
    end,
  case is_pid(ClientPid) of
    true ->
      ok = brod_client:register_consumer(Bootstrap, Topic, Partition);
    false ->
      ok
  end,
  {ok, #state{ client_pid          = ClientPid
             , bootstrap           = Bootstrap
             , topic               = Topic
             , partition           = Partition
             , begin_offset        = BeginOffset
             , max_wait_time       = MaxWaitTime
             , min_bytes           = MinBytes
             , max_bytes_orig      = MaxBytes
             , sleep_timeout       = SleepTimeout
             , prefetch_count      = PrefetchCount
             , prefetch_bytes      = PrefetchBytes
             , connection          = ?undef
             , pending_acks        = #pending_acks{}
             , is_suspended        = false
             , offset_reset_policy = OffsetResetPolicy
             , avg_bytes           = 0
             , max_bytes           = MaxBytes
             , size_stat_window    = Cfg(size_stat_window, ?DEFAULT_AVG_WINDOW)
             , connection_mref     = ?undef
             , isolation_level     = IsolationLevel
             }}.

%% @private
handle_info(?INIT_CONNECTION, #state{subscriber = Subscriber} = State0) ->
  case brod_utils:is_pid_alive(Subscriber) andalso
       maybe_init_connection(State0) of
    false ->
      %% subscriber not alive
      {noreply, State0};
    {ok, State1} ->
      State = maybe_send_fetch_request(State1),
      {noreply, State};
    {{error, _Reason}, State} ->
      %% failed when connecting to partition leader
      %% retry after a delay
      ok = maybe_send_init_connection(State),
      {noreply, State}
  end;
handle_info({msg, _Pid, Rsp}, State) ->
  handle_fetch_response(Rsp, State);
handle_info(?SEND_FETCH_REQUEST, State0) ->
  State = maybe_send_fetch_request(State0),
  {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            #state{subscriber = Pid} = State) ->
  NewState = reset_buffer(State#state{ subscriber      = ?undef
                                     , subscriber_mref = ?undef
                                     }),
  {noreply, NewState};
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            #state{connection = Pid} = State) ->
  %% monitored connection managed by brod_client
  {noreply, handle_conn_down(State)};
handle_info({'EXIT', Pid, _Reason}, #state{connection = Pid} = State) ->
  %% standalone connection spawn-linked to self()
  {noreply, handle_conn_down(State)};
handle_info(Info, State) ->
  ?BROD_LOG_WARNING("~p ~p got unexpected info: ~p",
                    [?MODULE, self(), Info]),
  {noreply, State}.

%% @private
handle_call(get_connection, _From, #state{connection = C} = State) ->
  {reply, C, State};
handle_call({subscribe, Pid, Options}, _From,
            #state{subscriber = Subscriber} = State0) ->
  case (not brod_utils:is_pid_alive(Subscriber)) %% old subscriber died
    orelse Subscriber =:= Pid of                 %% re-subscribe
    true ->
      %% Ensure connection is established before replying this call
      %% because we may need the connection
      %% to resolve begin offset (latest/earliest)
      case maybe_init_connection(State0) of
        {ok, State} ->
          handle_subscribe_call(Pid, Options, State);
        {{error, Reason}, State} ->
          {reply, {error, Reason}, State}
      end;
    false ->
      {reply, {error, {already_subscribed_by, Subscriber}}, State0}
  end;
handle_call({unsubscribe, SubscriberPid}, _From,
            #state{ subscriber = CurrentSubscriber
                  , subscriber_mref = Mref} = State) ->
  case SubscriberPid =:= CurrentSubscriber of
    true ->
      is_reference(Mref) andalso erlang:demonitor(Mref, [flush]),
      NewState = State#state{ subscriber      = ?undef
                            , subscriber_mref = ?undef
                            },
      {reply, ok, reset_buffer(NewState)};
    false ->
      {reply, {error, ignored}, State}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

%% @private
handle_cast({ack, Offset}, #state{pending_acks = PendingAcks} = State0) ->
  NewPendingAcks = handle_ack(PendingAcks, Offset),
  State1 = State0#state{pending_acks = NewPendingAcks},
  State = maybe_send_fetch_request(State1),
  {noreply, State};
handle_cast(Cast, State) ->
  ?BROD_LOG_WARNING("~p ~p got unexpected cast: ~p",
                    [?MODULE, self(), Cast]),
  {noreply, State}.

%% @private
terminate(Reason, #state{ client_pid = ClientPid
                        , topic = Topic
                        , partition = Partition
                        , connection = Connection
                        , connection_mref = Mref
                        }) ->
  IsNormal = brod_utils:is_normal_reason(Reason),
  %% deregister consumer if it's shared connection and normal shutdown
  case is_pid(ClientPid) andalso IsNormal of
    true ->
      brod_client:deregister_consumer(ClientPid, Topic, Partition);
    false ->
      ok
  end,
  %% close connection if it's owned by this consumer
  case Mref =:= ?undef andalso is_pid(Connection) andalso is_process_alive(Connection) of
    true ->
      kpro:close_connection(Connection);
    false ->
      ok
  end,
  %% write a log if it's not a normal reason
  IsNormal orelse ?BROD_LOG_ERROR("Consumer ~s-~w terminate reason: ~p",
                                  [Topic, Partition, Reason]),
  ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal Functions =======================================================

handle_conn_down(State0) ->
  State = State0#state{connection = ?undef, connection_mref = ?undef},
  ok = maybe_send_init_connection(State),
  State.

do_debug(Pid, Debug) ->
  {ok, _} = gen:call(Pid, system, {debug, Debug}, infinity),
  ok.

handle_fetch_response(#kpro_rsp{}, #state{subscriber = ?undef} = State0) ->
  %% discard fetch response when there is no (dead?) subscriber
  State = State0#state{last_req_ref = ?undef},
  {noreply, State};
handle_fetch_response(#kpro_rsp{ref = Ref1},
                      #state{ last_req_ref = Ref2
                            } = State) when Ref1 =/= Ref2 ->
  %% Not expected response, discard
  {noreply, State};
handle_fetch_response(#kpro_rsp{ref = Ref, vsn = Vsn} = Rsp,
                      #state{ topic = Topic
                            , partition = Partition
                            , last_req_ref = Ref
                            } = State0) ->
  State = State0#state{last_req_ref = ?undef},
  case brod_utils:parse_rsp(Rsp) of
    {ok, #{ header := Header
          , batches := Batches
          }} ->
      handle_batches(Header, Batches, State, Vsn);
    {error, ErrorCode} ->
      Error = #kafka_fetch_error{ topic      = Topic
                                , partition  = Partition
                                , error_code = ErrorCode
                                },
      handle_fetch_error(Error, State)
  end.

handle_batches(?undef, [], #state{} = State0, _Vsn) ->
  %% It is only possible to end up here in a incremental
  %% fetch session, empty fetch response implies no
  %% new messages to fetch, and no changes in partition
  %% metadata (e.g. high watermark offset, or last stable offset) either.
  %% Do not advance offset, try again (maybe after a delay) with
  %% the last begin_offset in use.
  State = maybe_delay_fetch_request(State0),
  {noreply, State};
handle_batches(_Header, ?incomplete_batch(Size),
               #state{max_bytes = MaxBytes} = State0, _Vsn) ->
  %% max_bytes is too small to fetch ONE complete batch
  true = Size > MaxBytes, %% assert
  State1 = State0#state{max_bytes = Size},
  State = maybe_send_fetch_request(State1),
  {noreply, State};
handle_batches(Header, [], #state{begin_offset = BeginOffset} = State0, Vsn) ->
  StableOffset = brod_utils:get_stable_offset(Header),
  State =
    case BeginOffset < StableOffset of
      true when Vsn > 0 ->
        %% There are chances that Kafka may return empty message set
        %% when messages are deleted from a compacted topic.
        %% Since there is no way to know how big the 'hole' is
        %% we can only bump begin_offset with +1 and try again.
        ?BROD_LOG_WARNING("~s-~p empty_batch_detected_at_offset=~p, "
                          "fetch_api_vsn=~p, skip_to_offset=~p",
                          [State0#state.topic,
                           State0#state.partition,
                           BeginOffset,
                           BeginOffset + 1
                          ]),
        State1 = State0#state{begin_offset = BeginOffset + 1},
        maybe_send_fetch_request(State1);
      true ->
        %% Fetch API v0 (Kafka 0.9 and 0.10) seems to have a race condition:
        %% Kafka returns empty batch even if BeginOffset is lower than high-watermark
        %% if fetch request is sent in a tight loop
        %% Retry seems to resolve the issue
        maybe_delay_fetch_request(State0);
      false ->
        %% We have either reached the end of a partition
        %% or trying to read uncommitted messages
        %% try to poll again (maybe after a delay)
        maybe_delay_fetch_request(State0)
    end,
  {noreply, State};
handle_batches(Header, Batches,
               #state{ subscriber   = Subscriber
                     , pending_acks = PendingAcks
                     , begin_offset = BeginOffset
                     , topic        = Topic
                     , partition    = Partition
                     } = State0, _Vsn) ->
  StableOffset = brod_utils:get_stable_offset(Header),
  {NewBeginOffset, Messages} =
    brod_utils:flatten_batches(BeginOffset, Header, Batches),
  State1 = State0#state{begin_offset = NewBeginOffset},
  State =
    case Messages =:= [] of
      true ->
        %% All messages are before requested offset, hence dropped
        State1;
      false ->
        MsgSet = #kafka_message_set{ topic          = Topic
                                   , partition      = Partition
                                   , high_wm_offset = StableOffset
                                   , messages       = Messages
                                   },
        ok = cast_to_subscriber(Subscriber, MsgSet),
        NewPendingAcks = add_pending_acks(PendingAcks, Messages),
        State2 = State1#state{pending_acks = NewPendingAcks},
        maybe_shrink_max_bytes(State2, MsgSet#kafka_message_set.messages)
    end,
  {noreply, maybe_send_fetch_request(State)}.

%% Add received offsets to pending queue.
add_pending_acks(PendingAcks, Messages) ->
  lists:foldl(fun add_pending_ack/2, PendingAcks, Messages).

add_pending_ack(#kafka_message{offset = Offset, key = Key, value = Value},
                #pending_acks{ queue = Queue
                             , count = Count
                             , bytes = Bytes
                             } = PendingAcks) ->
  Size = size(Key) + size(Value),
  NewQueue = queue:in(?PENDING(Offset, Size), Queue),
  PendingAcks#pending_acks{ queue = NewQueue
                          , count = Count + 1
                          , bytes = Bytes + Size
                          }.

%% In case max_bytes has been expanded to fetch a large message
%% try to shrink back to the original max_bytes from consumer config
maybe_shrink_max_bytes(#state{ size_stat_window = W
                             , max_bytes_orig = MaxBytesOrig
                             } = State, _) when W < 1 ->
  %% Configured to not collect average message size,
  %% Shrink back to original max_bytes immediately
  State#state{max_bytes = MaxBytesOrig};
maybe_shrink_max_bytes(State0, Messages) ->
  #state{ prefetch_count = PrefetchCount
        , max_bytes_orig = MaxBytesOrig
        , max_bytes      = MaxBytes
        , avg_bytes      = AvgBytes
        } = State = update_avg_size(State0, Messages),
  %% This is the estimated size of a message set based on the
  %% average size of the last X messages.
  EstimatedSetSize = erlang:round(PrefetchCount * AvgBytes),
  %% respect the original max_bytes config
  NewMaxBytes = erlang:max(EstimatedSetSize, MaxBytesOrig),
  %% maybe shrink the max_bytes to send in fetch request to NewMaxBytes
  State#state{max_bytes = erlang:min(NewMaxBytes, MaxBytes)}.

update_avg_size(#state{} = State, []) -> State;
update_avg_size(#state{ avg_bytes        = AvgBytes
                      , size_stat_window = WindowSize
                      } = State,
                [#kafka_message{key = Key, value = Value} | Rest]) ->
  %% kafka adds 34 bytes of overhead (metadata) for each message
  %% use 40 to give some room for future kafka protocol versions
  MsgBytes = size(Key) + size(Value) + 40,
  %% See https://en.wikipedia.org/wiki/Moving_average
  NewAvgBytes = ((WindowSize - 1) * AvgBytes + MsgBytes) / WindowSize,
  update_avg_size(State#state{avg_bytes = NewAvgBytes}, Rest).

err_op(?request_timed_out)          -> retry;
err_op(?invalid_topic_exception)    -> stop;
err_op(?offset_out_of_range)        -> reset_offset;
err_op(?leader_not_available)       -> reset_connection;
err_op(?not_leader_for_partition)   -> reset_connection;
err_op(?unknown_topic_or_partition) -> reset_connection;
err_op(_)                           -> restart.

handle_fetch_error(#kafka_fetch_error{error_code = ErrorCode} = Error,
                   #state{ topic           = Topic
                         , partition       = Partition
                         , subscriber      = Subscriber
                         , connection_mref = MRef
                         } = State) ->
  case err_op(ErrorCode) of
    reset_connection ->
      ?BROD_LOG_INFO("Fetch error ~s-~p: ~p",
                     [Topic, Partition, ErrorCode]),
      %% The current connection in use is not connected to the partition leader,
      %% so we dereference and demonitor the connection pid, but leave it alive,
      %% Can not kill it because it might be shared with other partition workers
      %% Worst case scenario, kafka will close the connection after it
      %% idles for a few minutes.
      is_reference(MRef) andalso erlang:demonitor(MRef),
      NewState = State#state{ connection = ?undef
                            , connection_mref = ?undef
                            },
      ok = maybe_send_init_connection(NewState),
      {noreply, NewState};
    retry ->
      {noreply, maybe_send_fetch_request(State)};
    stop ->
      ok = cast_to_subscriber(Subscriber, Error),
      ?BROD_LOG_ERROR("Consumer ~s-~p shutdown\nReason: ~p",
                      [Topic, Partition, ErrorCode]),
      {stop, normal, State};
    reset_offset ->
      handle_reset_offset(State, Error);
    restart ->
      ok = cast_to_subscriber(Subscriber, Error),
      {stop, {restart, ErrorCode}, State}
  end.

handle_reset_offset(#state{ subscriber          = Subscriber
                          , offset_reset_policy = reset_by_subscriber
                          } = State, Error) ->
  ok = cast_to_subscriber(Subscriber, Error),
  %% Suspend, no more fetch request until the subscriber re-subscribes
  ?BROD_LOG_INFO("~p ~p consumer is suspended, "
                 "waiting for subscriber ~p to resubscribe with "
                 "new begin_offset", [?MODULE, self(), Subscriber]),
  {noreply, State#state{is_suspended = true}};
handle_reset_offset(#state{offset_reset_policy = Policy} = State, _Error) ->
  ?BROD_LOG_INFO("~p ~p offset out of range, applying reset policy ~p",
                 [?MODULE, self(), Policy]),
  BeginOffset = case Policy of
                  reset_to_earliest -> ?OFFSET_EARLIEST;
                  reset_to_latest   -> ?OFFSET_LATEST
                end,
  State1 = State#state{ begin_offset = BeginOffset
                      , pending_acks = #pending_acks{}
                      },
  {ok, State2} = resolve_begin_offset(State1),
  NewState = maybe_send_fetch_request(State2),
  {noreply, NewState}.

handle_ack(#pending_acks{ queue = Queue
                        , bytes = Bytes
                        , count = Count
                        } = PendingAcks, Offset) ->
  case queue:out(Queue) of
    {{value, ?PENDING(O, Size)}, Queue1} when O =< Offset ->
      handle_ack(PendingAcks#pending_acks{ queue = Queue1
                                         , count = Count - 1
                                         , bytes = Bytes - Size
                                         }, Offset);
    _ ->
      PendingAcks
  end.

cast_to_subscriber(Pid, Msg) ->
  try
    Pid ! {self(), Msg},
    ok
  catch _ : _ ->
    ok
  end.

-spec maybe_delay_fetch_request(state()) -> state().
maybe_delay_fetch_request(#state{sleep_timeout = T} = State) when T > 0 ->
  _ = erlang:send_after(T, self(), ?SEND_FETCH_REQUEST),
  State;
maybe_delay_fetch_request(State) ->
  maybe_send_fetch_request(State).

%% Send new fetch request if no pending error.
maybe_send_fetch_request(#state{subscriber = ?undef} = State) ->
  %% no subscriber
  State;
maybe_send_fetch_request(#state{connection = ?undef} = State) ->
  %% no connection
  State;
maybe_send_fetch_request(#state{is_suspended = true} = State) ->
  %% waiting for subscriber to re-subscribe
  State;
maybe_send_fetch_request(#state{last_req_ref = R} = State)
  when is_reference(R) ->
  %% Waiting for the last request
  State;
maybe_send_fetch_request(#state{ pending_acks   = #pending_acks{ count = Count
                                                               , bytes = Bytes
                                                               }
                               , prefetch_count = PrefetchCount
                               , prefetch_bytes = PrefetchBytes
                               } = State) ->
  %% Do not send fetch request if exceeded limits on both count and size
  case Count > PrefetchCount andalso Bytes > PrefetchBytes of
    true  -> State;
    false -> send_fetch_request(State)
  end.

-spec send_fetch_request(state()) -> state().
send_fetch_request(#state{ begin_offset = BeginOffset
                         , connection = Connection
                         } = State) ->
  (is_integer(BeginOffset) andalso BeginOffset >= 0) orelse
    erlang:error({bad_begin_offset, BeginOffset}),
  %% MaxBytes=0 will make no progress when it's Kafka 0.9
  MaxBytes = max(12, State#state.max_bytes),
  Request =
    brod_kafka_request:fetch(Connection,
                             State#state.topic,
                             State#state.partition,
                             State#state.begin_offset,
                             State#state.max_wait_time,
                             State#state.min_bytes,
                             MaxBytes,
                             State#state.isolation_level),
  case kpro:request_async(Connection, Request) of
    ok ->
      State#state{last_req_ref = Request#kpro_req.ref};
    {error, {connection_down, _Reason}} ->
      %% ignore error here, the connection pid 'DOWN' message
      %% should trigger the re-init loop
      State
  end.

handle_subscribe_call(Pid, Options,
                      #state{subscriber_mref = OldMref} = State0) ->
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
  end.

-spec update_options(config(), state()) -> {ok, state()} | {error, any()}.
update_options(Options, #state{begin_offset = OldBeginOffset} = State) ->
  F = fun(Name, Default) -> proplists:get_value(Name, Options, Default) end,
  NewBeginOffset = F(begin_offset, OldBeginOffset),
  OffsetResetPolicy = F(offset_reset_policy, State#state.offset_reset_policy),
  State1 = State#state
    { begin_offset        = NewBeginOffset
    , min_bytes           = F(min_bytes, State#state.min_bytes)
    , max_bytes_orig      = F(max_bytes, State#state.max_bytes_orig)
    , max_wait_time       = F(max_wait_time, State#state.max_wait_time)
    , sleep_timeout       = F(sleep_timeout, State#state.sleep_timeout)
    , prefetch_count      = F(prefetch_count, State#state.prefetch_count)
    , prefetch_bytes      = F(prefetch_bytes, State#state.prefetch_bytes)
    , offset_reset_policy = OffsetResetPolicy
    , max_bytes           = F(max_bytes, State#state.max_bytes)
    , size_stat_window    = F(size_stat_window, State#state.size_stat_window)
    },
  NewState =
    case NewBeginOffset =/= OldBeginOffset of
      true ->
        %% reset buffer in case subscriber wants to fetch from a new offset
        State1#state{pending_acks = #pending_acks{}};
      false ->
        State1
    end,
  resolve_begin_offset(NewState).

-spec resolve_begin_offset(state()) -> {ok, state()} | {error, any()}.
resolve_begin_offset(#state{ begin_offset = BeginOffset
                           , connection   = Connection
                           , topic        = Topic
                           , partition    = Partition
                           } = State) when ?IS_SPECIAL_OFFSET(BeginOffset) ->
  case resolve_offset(Connection, Topic, Partition, BeginOffset) of
    {ok, NewBeginOffset} ->
      {ok, State#state{begin_offset = NewBeginOffset}};
    {error, Reason} ->
      {error, Reason}
  end;
resolve_begin_offset(State) ->
  {ok, State}.

-spec resolve_offset(pid(), topic(), partition(), offset_time()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Connection, Topic, Partition, BeginOffset) ->
  try
    brod_utils:resolve_offset(Connection, Topic, Partition, BeginOffset)
  catch
    throw : Reason ->
      {error, Reason}
  end.

%% Reset fetch buffer, use the last unacked offset as the next begin
%% offset to fetch data from.
%% Discard onwire fetch responses by setting last_req_ref to undefined.
-spec reset_buffer(state()) -> state().
reset_buffer(#state{ pending_acks = #pending_acks{queue = Queue}
                   , begin_offset = BeginOffset0
                   } = State) ->
  BeginOffset = case queue:peek(Queue) of
                  {value, ?PENDING(Offset, _)} -> Offset;
                  empty                        -> BeginOffset0
                end,
  State#state{ begin_offset = BeginOffset
             , pending_acks = #pending_acks{}
             , last_req_ref = ?undef
             }.

%% Catch exit exceptions when making gen_server:call.
-spec safe_gen_call(pid() | atom(), Call, Timeout) -> Return
        when Call    :: term(),
             Timeout :: infinity | integer(),
             Return  :: ok | {ok, term()} | {error, any()}.
safe_gen_call(Server, Call, Timeout) ->
  try
    gen_server:call(Server, Call, Timeout)
  catch
    exit : {Reason, _} ->
      {error, Reason}
  end.

%% Init payload connection regardless of subscriber state.
-spec maybe_init_connection(state()) ->
        {ok, state()} | {{error, any()}, state()}.
maybe_init_connection(
  #state{ client_pid = ClientPid
        , bootstrap  = Bootstrap
        , topic      = Topic
        , partition  = Partition
        , connection = ?undef
        } = State0) ->
  %% Lookup, or maybe (re-)establish a connection to partition leader
  {MonitorOrLink, Result} = connect_leader(ClientPid, Bootstrap, Topic, Partition),
  case Result of
    {ok, Connection} ->
      Mref = case MonitorOrLink of
               monitor -> erlang:monitor(process, Connection);
               linked -> ?undef
             end,
      %% Switching to a new connection
      %% the response for last_req_ref will be lost forever
      State = State0#state{ last_req_ref = ?undef
                          , connection = Connection
                          , connection_mref = Mref
                          },
      {ok, State};
    {error, Reason} ->
      {{error, {connect_leader, Reason}}, State0}
  end;
maybe_init_connection(State) ->
  {ok, State}.

connect_leader(ClientPid, ?IGNORE, Topic, Partition) when is_pid(ClientPid) ->
  {monitor, brod_client:get_leader_connection(ClientPid, Topic, Partition)};
connect_leader(ClientPid, ?GET_FROM_CLIENT, Topic, Partition) when is_pid(ClientPid) ->
  case brod_client:get_bootstrap(ClientPid) of
    {ok, Bootstrap} ->
      link_connect_leader(Bootstrap, Topic, Partition);
    {error, Reason} ->
      {linked, {error, Reason}}
  end;
connect_leader(?IGNORE, Bootstrap, Topic, Partition) ->
  link_connect_leader(Bootstrap, Topic, Partition).

link_connect_leader(Endpoints, Topic, Partition) when is_list(Endpoints) ->
  link_connect_leader({Endpoints, []}, Topic, Partition);
link_connect_leader({Endpoints, ConnCfg}, Topic, Partition) ->
  %% connection pid is linked to self()
  {linked, kpro:connect_partition_leader(Endpoints, ConnCfg, Topic, Partition)}.

%% Send a ?INIT_CONNECTION delayed loopback message to re-init.
-spec maybe_send_init_connection(state()) -> ok.
maybe_send_init_connection(#state{subscriber = Subscriber}) ->
  Timeout = ?CONNECTION_RETRY_DELAY_MS,
  %% re-init payload connection only when subscriber is alive
  brod_utils:is_pid_alive(Subscriber) andalso
    erlang:send_after(Timeout, self(), ?INIT_CONNECTION),
  ok.

%%%_* Tests ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

pending_acks_to_offset_list(#pending_acks{count = C, bytes = B, queue = Q}) ->
  All = queue:to_list(Q),
  Size = lists:foldl(fun(?PENDING(_Offset, Bytes), Acc) -> Bytes + Acc end,
                     0, All),
  ?assertEqual(C, length(All)),
  ?assertEqual(B, Size),
  [O || {O, _} <- All].

pending_acks_test() ->
  Offsets = [1, 2, 3, 5, 6, 7, 9, 100],
  Messages = [#kafka_message{ offset = O
                            , key = <<>>
                            , value = crypto:strong_rand_bytes(10)
                            } || O <- Offsets],
  Pending0 = add_pending_acks(#pending_acks{}, Messages),
  ?assertMatch(#pending_acks{count = 8}, Pending0),
  Message = #kafka_message{ offset = 101
                          , key = <<>>
                          , value = crypto:strong_rand_bytes(10)
                          },
  Pending1 = add_pending_acks(Pending0, [Message]),
  ?assertEqual(Offsets ++ [101], pending_acks_to_offset_list(Pending1)),
  Pending2 = handle_ack(Pending1, 2),
  ?assertEqual([3, 5, 6, 7, 9, 100, 101],
               pending_acks_to_offset_list(Pending2)),
  Pending3 = handle_ack(Pending2, 99),
  ?assertEqual([100, 101], pending_acks_to_offset_list(Pending3)),
  Pending4 = handle_ack(Pending3, 101),
  ?assertEqual([], pending_acks_to_offset_list(Pending4)).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
