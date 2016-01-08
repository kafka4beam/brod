%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_consumer).

-behaviour(gen_server).

%% Server API
-export([ start_link/5
        , start_link/6
        , stop/1
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

-type consumer_option() :: offset
                         | min_bytes
                         | max_bytes
                         | max_wait_time
                         | sleep_timeout
                         | prefetch_count.

%% behaviour definition
-callback init_consumer( Topic     :: topic()
                       , Partition :: partition()
                       , CbArgs    :: any()) ->
  {ok, CbState :: any(), CbOptions :: [{consumer_option(), any()}]}.

-callback handle_messages( Topic        :: topic()
                         , Partition    :: partition()
                         , HighWmOffset :: integer()
                         , Messages     :: [#message{}]
                         , CbState      :: any()) ->
  {ok, NewCbState :: any()} |
  {ok, NewCbState :: any(), NewCbOptions :: [{consumer_option(), any()}]}.

-callback handle_error( Topic        :: topic()
                      , Partition    :: partition()
                      , Error        :: {atom(), any()}
                      , CbState      :: any()) ->
  {ok, NewCbState :: any()} |
  {ok, NewCbState :: any(), NewCbOptions :: [{consumer_option(), any()}]} |
  stop.

-record(state, { cb_mod         :: atom()
               , cb_state       :: any()
               , cb_pending     :: [reference()]
               , cb_pending_cnt :: integer()
               , client_id      :: client_id()
               , config         :: consumer_config()
               , socket_pid     :: pid()
               , topic          :: binary()
               , partition      :: integer()
               , offset         :: integer()
               , max_wait_time  :: integer()
               , min_bytes      :: integer()
               , max_bytes      :: integer()
               , sleep_timeout  :: integer()
               , prefetch_count :: integer()
               , corr_id        :: corr_id()
               }).

-define(DEFAULT_OFFSET, -1).
-define(DEFAULT_MIN_BYTES, 0).
-define(DEFAULT_MAX_BYTES, 1048576).  % 1 MB
-define(DEFAULT_MAX_WAIT_TIME, 1000). % 1 sec
-define(DEFAULT_SLEEP_TIMEOUT, 1000). % 1 sec
-define(DEFAULT_PREFETCH_COUNT, 1).

-define(SEND_FETCH_REQUEST, send_fetch_request).
-define(ERROR_COOLDOWN, 1000).

%%%_* APIs =====================================================================
%% @equiv start_link(ClientId, Topic, Partition, Config, [])
-spec start_link(atom(), client_id(), topic(), partition(),
                 consumer_config()) -> {ok, pid()} | {error, any()}.
start_link(CbMod, ClientId, Topic, Partition, Config) ->
  start_link(CbMod, ClientId, Topic, Partition, Config, []).

-spec start_link(atom(), client_id(), topic(), partition(),
                 consumer_config(), [any()]) -> {ok, pid()} | {error, any()}.
start_link(CbMod, ClientId, Topic, Partition, Config, Debug) ->
  Args = {CbMod, ClientId, Topic, Partition, Config},
  gen_server:start_link(?MODULE, Args, [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

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

init({CbMod, ClientId, Topic, Partition, Config}) ->
  self() ! init_socket,
  {ok, #state{ cb_mod         = CbMod
             , cb_pending     = []
             , cb_pending_cnt = 0
             , client_id      = ClientId
             , topic          = Topic
             , partition      = Partition
             , config         = Config
             }}.

handle_info(init_socket, #state{ cb_mod    = CbMod
                               , client_id = ClientId
                               , topic     = Topic
                               , partition = Partition
                               , config    = Config
                               } = State0) ->
  %% 1. Lookup, or maybe (re-)establish a connection to partition leader
  {ok, SocketPid} =
    brod_client:get_leader_connection(ClientId, Topic, Partition),
  _ = erlang:monitor(process, SocketPid),

  %% 2. Get options from callback module and merge with Config
  CbArgs = proplists:get_value(cb_args, Config, []),
  {ok, CbState, CbOptions} = CbMod:init_consumer(Topic, Partition, CbArgs),
  MinBytes0 = proplists:get_value(min_bytes, Config, ?DEFAULT_MIN_BYTES),
  MinBytes = proplists:get_value(min_bytes, CbOptions, MinBytes0),
  MaxBytes0 = proplists:get_value(max_bytes, Config, ?DEFAULT_MAX_BYTES),
  MaxBytes = proplists:get_value(max_bytes, CbOptions, MaxBytes0),
  MaxWaitTime0 =
    proplists:get_value(max_wait_time, Config, ?DEFAULT_MAX_WAIT_TIME),
  MaxWaitTime =
    proplists:get_value(max_wait_time, CbOptions, MaxWaitTime0),
  SleepTimeout0 =
    proplists:get_value(sleep_timeout, Config, ?DEFAULT_SLEEP_TIMEOUT),
  SleepTimeout =
    proplists:get_value(sleep_timeout, CbOptions, SleepTimeout0),
  PrefetchCount0 =
    proplists:get_value(prefetch_count, Config, ?DEFAULT_PREFETCH_COUNT),
  PrefetchCount =
    proplists:get_value(prefetch_count, CbOptions, PrefetchCount0),
  Offset0 = proplists:get_value(offset, Config, ?DEFAULT_OFFSET),
  Offset1 = proplists:get_value(offset, CbOptions, Offset0),

  %% 3. Retrieve valid starting offset
  case fetch_valid_offset(SocketPid, Offset1, Topic, Partition) of
    {error, Error} ->
      {stop, {error, Error}, State0};
    {ok, Offset} ->
      State = State0#state{ cb_state       = CbState
                          , socket_pid     = SocketPid
                          , offset         = Offset
                          , max_wait_time  = MaxWaitTime
                          , min_bytes      = MinBytes
                          , max_bytes      = MaxBytes
                          , sleep_timeout  = SleepTimeout
                          , prefetch_count = PrefetchCount
                          },
      %% 4. Start consuming
      {ok, CorrId} = send_fetch_request(State),
      {noreply, State#state{corr_id = CorrId}}
  end;
handle_info({msg, _Pid, CorrId, R}, #state{corr_id = CorrId} = State0) ->
  #fetch_response{topics = [TopicFetchData]} = R,
  #topic_fetch_data{ topic = Topic
                   , partitions = [PM]} = TopicFetchData,
  #partition_messages{ partition = Partition
                     , error_code = ErrorCode
                     , high_wm_offset = HighWmOffset
                     , last_offset = LastOffset
                     , messages = Messages} = PM,
  CbMod = State0#state.cb_mod,
  CbState = State0#state.cb_state,
  case brod_kafka:is_error(ErrorCode) of
    true ->
      Error = {ErrorCode, brod_kafka_errors:desc(ErrorCode)},
      case CbMod:handle_error(Topic, Partition, Error, CbState) of
        {ok, NewCbState} ->
          erlang:send_after(?ERROR_COOLDOWN, self(), ?SEND_FETCH_REQUEST),
          {noreply, State0#state{cb_state = NewCbState}};
        {ok, NewCbState, NewCbOptions} ->
          {ok, State} = update_cb_options(NewCbOptions, State0),
          OldOffset = State0#state.offset,
          NewOffset0 = State#state.offset,
          SocketPid = State#state.socket_pid,
          {ok, NewOffset} = maybe_fetch_valid_offset(SocketPid, OldOffset,
                                                     NewOffset0, Topic, Partition),
          erlang:send_after(?ERROR_COOLDOWN, self(), ?SEND_FETCH_REQUEST),
          {noreply, State#state{cb_state = NewCbState, offset = NewOffset}};
        stop ->
          {stop, Error, State0};
        Other ->
          {stop, {callback_error, Other, Error}, State0}
      end;
    false ->
      SleepTimeout = State0#state.sleep_timeout,
      case Messages of
        [] when SleepTimeout =:= 0 ->
          {ok, NewCorrId} = send_fetch_request(State0),
          {noreply, State0#state{corr_id = NewCorrId}};
        [] when SleepTimeout > 0 ->
          erlang:send_after(SleepTimeout, self(), ?SEND_FETCH_REQUEST),
          {noreply, State0};
        [_|_] ->
          F = fun() ->
                  Res = CbMod:handle_messages(Topic, Partition,HighWmOffset,
                                              Messages, CbState),
                  exit({shutdown, Res})
              end,
          {_, Ref} = erlang:spawn_monitor(F),
          CbPending = [Ref | State0#state.cb_pending],
          CbPendingCnt = State0#state.cb_pending_cnt + 1,
          State1 = State0#state{ offset = LastOffset + 1
                               , cb_pending = CbPending
                               , cb_pending_cnt = CbPendingCnt},
          {ok, State} = maybe_send_fetch_request(State1),
          {noreply, State}
        end
  end;
%% handle obsolete fetch responses in case we got new offset from callback
handle_info({msg, Pid, CorrId1, _R},
            #state{corr_id = CorrId2, socket_pid = Pid} = State)
  when CorrId1 < CorrId2 ->
  error_logger:info_msg("~p ~p Dropping obsolete fetch response with corr_id = ~p",
                        [?MODULE, self(), CorrId1]),
  {noreply, State};
handle_info(?SEND_FETCH_REQUEST, State) ->
  {ok, CorrId} = send_fetch_request(State),
  {noreply, State#state{corr_id = CorrId}};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{socket_pid = Pid} = State) ->
  {stop, {socket_down, Reason}, State};
%% callback completed
handle_info({'DOWN', MRef, process, Pid, Reason}, State0) ->
  CbPending0 = State0#state.cb_pending,
  CbPendingCnt0 = State0#state.cb_pending_cnt,
  IsBlocked = CbPendingCnt0 > State0#state.prefetch_count,
  case lists:member(MRef, CbPending0) of
    true ->
      CbPending = lists:delete(MRef, CbPending0),
      CbPendingCnt = CbPendingCnt0 - 1,
      case Reason of
        {shutdown, {ok, CbState}} ->
          State1 = State0#state{ cb_pending = CbPending
                               , cb_pending_cnt = CbPendingCnt
                               , cb_state = CbState},
          {ok, State} = maybe_unblock(IsBlocked, State1),
          {noreply, State};
        {shutdown, {ok, CbState, NewCbOptions}} ->
          {ok, State1} = update_cb_options(NewCbOptions, State0),
          OldOffset = State0#state.offset,
          NewOffset0 = State1#state.offset,
          SocketPid = State1#state.socket_pid,
          Topic = State1#state.topic,
          Partition = State1#state.partition,
          {ok, NewOffset} = maybe_fetch_valid_offset(SocketPid, OldOffset,
                                                     NewOffset0, Topic,
                                                     Partition),
          State1 = State1#state{ cb_pending = CbPending
                               , cb_pending_cnt = CbPendingCnt
                               , cb_state = CbState
                               , offset = NewOffset},
          {ok, State} = maybe_unblock(IsBlocked, State1),
          {noreply, State};
        Other ->
          {stop, {callback_error, Other}, State0}
      end;
    false ->
      error_logger:warning_msg("Unexpected 'DOWN' in ~p ~p from ~p, reason: ~p",
                               [?MODULE, self(), Pid, Reason]),
      {noreply, State0}
  end;
handle_info(Info, State) ->
  error_logger:warning_msg("~p [~p] ~p got unexpected info: ~p",
                          [?MODULE, self(), State#state.client_id, Info]),
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast(Cast, State) ->
  error_logger:warning_msg("~p [~p] ~p got unexpected cast: ~p",
                          [?MODULE, self(), State#state.client_id, Cast]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal Functions =======================================================
maybe_fetch_valid_offset(_SocketPid, Offset, Offset, _Topic, _Partition) ->
  {ok, Offset};
maybe_fetch_valid_offset(SocketPid, _OldOffset, NewOffset, Topic, Partition) ->
  case fetch_valid_offset(SocketPid, NewOffset, Topic, Partition) of
    {ok, Offset} ->
      {ok, Offset};
    {error, no_available_offsets} ->
      %% use new offset in this case and let the client handle the issue later
      {ok, NewOffset}
  end.

fetch_valid_offset(SocketPid, InitialOffset, Topic, Partition) ->
  Request = #offset_request{ topic = Topic
                           , partition = Partition
                           , time = InitialOffset
                           , max_n_offsets = 1},
  {ok, Response} = brod_sock:send_sync(SocketPid, Request, 5000),
  #offset_response{topics = [#offset_topic{} = OT]} = Response,
  #offset_topic{partitions =
                 [#partition_offsets{offsets = Offsets}]} = OT,
  case Offsets of
    [Offset] -> {ok, Offset};
    []       -> {error, no_available_offsets}
  end.

maybe_unblock(false, State) ->
  {ok, State};
maybe_unblock(true, State) ->
  maybe_send_fetch_request(State).

maybe_send_fetch_request(State) ->
  case State#state.cb_pending_cnt > State#state.prefetch_count of
    true ->
      {ok, State};
    false ->
      {ok, CorrId} = send_fetch_request(State),
      {ok, State#state{corr_id = CorrId}}
  end.

send_fetch_request(#state{socket_pid = SocketPid} = State) ->
  Request = #fetch_request{ topic = State#state.topic
                          , partition = State#state.partition
                          , offset = State#state.offset
                          , max_wait_time = State#state.max_wait_time
                          , min_bytes = State#state.min_bytes
                          , max_bytes = State#state.max_bytes},
  brod_sock:send(SocketPid, Request).

do_debug(Pid, Debug) ->
  {ok, _} = gen:call(Pid, system, {debug, Debug}, infinity),
  ok.

update_cb_options(NewCbOptions, State) ->
  MinBytes = proplists:get_value(min_bytes,NewCbOptions,
                                 State#state.min_bytes),
  MaxBytes = proplists:get_value(max_bytes, NewCbOptions,
                                 State#state.max_bytes),
  MaxWaitTime = proplists:get_value(max_wait_time, NewCbOptions,
                                    State#state.max_wait_time),
  SleepTimeout = proplists:get_value(sleep_timeout, NewCbOptions,
                                     State#state.sleep_timeout),
  PrefetchCount = proplists:get_value(prefetch_count, NewCbOptions,
                                      State#state.prefetch_count),
  Offset = proplists:get_value(offset, NewCbOptions,
                               State#state.offset),
  {ok, State#state{ offset = Offset
                  , min_bytes = MinBytes
                  , max_bytes = MaxBytes
                  , max_wait_time = MaxWaitTime
                  , sleep_timeout = SleepTimeout
                  , prefetch_count = PrefetchCount}}.

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
