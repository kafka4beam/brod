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
%%% @copyright 2014-2016 Klarna AB
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

-define(KAFKA_ERROR(Timestamp, ErrorCode, HandleErrorFun),
        {Timestamp, ErrorCode, HandleErrorFun}).

-type last_error() :: undefined
                    | ?KAFKA_ERROR(erlang:timestamp(), error_code(), function()).

-record(state, { cb_mod         :: atom()
               , cb_state       :: any()
               , cb_pending     :: [reference()]
               , cb_pending_cnt :: integer()
               , client_pid     :: pid()
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
               , last_corr_id   :: corr_id()
               , last_error     :: last_error()
               }).

-define(DEFAULT_OFFSET, -1).
-define(DEFAULT_MIN_BYTES, 0).
-define(DEFAULT_MAX_BYTES, 1048576).  % 1 MB
-define(DEFAULT_MAX_WAIT_TIME, 1000). % 1 sec
-define(DEFAULT_SLEEP_TIMEOUT, 1000). % 1 sec
-define(DEFAULT_PREFETCH_COUNT, 1).
-define(ERROR_COOLDOWN, 1000).

-define(SEND_FETCH_REQUEST, send_fetch_request).

%%%_* APIs =====================================================================
%% @equiv start_link(ClientPid, Topic, Partition, Config, [])
-spec start_link(atom(), pid(), topic(), partition(),
                 consumer_config()) -> {ok, pid()} | {error, any()}.
start_link(CbMod, ClientPid, Topic, Partition, Config) ->
  start_link(CbMod, ClientPid, Topic, Partition, Config, []).

-spec start_link(atom(), pid(), topic(), partition(),
                 consumer_config(), [any()]) -> {ok, pid()} | {error, any()}.
start_link(CbMod, ClientPid, Topic, Partition, Config, Debug) ->
  Args = {CbMod, ClientPid, Topic, Partition, Config},
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

init({CbMod, ClientPid, Topic, Partition, Config}) ->
  self() ! init_socket,
  {ok, #state{ cb_mod         = CbMod
             , cb_pending     = []
             , cb_pending_cnt = 0
             , client_pid     = ClientPid
             , topic          = Topic
             , partition      = Partition
             , config         = Config
             }}.

handle_info(init_socket, #state{ cb_mod     = CbMod
                               , client_pid = ClientPid
                               , topic      = Topic
                               , partition  = Partition
                               , config     = Config
                               } = State0) ->
  %% 1. Lookup, or maybe (re-)establish a connection to partition leader
  {ok, SocketPid} =
    brod_client:get_leader_connection(ClientPid, Topic, Partition),
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
      {noreply, State#state{last_corr_id = CorrId}}
  end;
handle_info({msg, _Pid, CorrId, R}, #state{last_corr_id = CorrId} = State0) ->
  #fetch_response{topics = [TopicFetchData]} = R,
  #topic_fetch_data{ topic = Topic
                   , partitions = [PM]} = TopicFetchData,
  #partition_messages{ partition = Partition
                     , error_code = ErrorCode
                     , high_wm_offset = HighWmOffset
                     , last_offset = LastOffset
                     , messages = Messages} = PM,
  CbMod = State0#state.cb_mod,
  case brod_kafka:is_error(ErrorCode) of
    true ->
      Now = os:timestamp(),
      Error = {ErrorCode, brod_kafka_errors:desc(ErrorCode)},
      F = fun(CbStateIn) ->
            CbMod:handle_error(Topic, Partition, Error, CbStateIn)
          end,
      State = State0#state{last_error = ?KAFKA_ERROR(Now, ErrorCode, F)},
      maybe_handle_last_error(State);
    false ->
      case Messages of
        [] ->
          maybe_delay_fetch_request(State0#state.sleep_timeout),
          {noreply, State0};
        [_|_] ->
          F = fun(CbStateIn) ->
                  CbMod:handle_messages(Topic, Partition, HighWmOffset,
                                        Messages, CbStateIn)
              end,
          CbPending = State0#state.cb_pending ++ [F],
          CbPendingCnt = State0#state.cb_pending_cnt + 1,
          State1 = State0#state{ offset = LastOffset + 1
                               , cb_pending = CbPending
                               , cb_pending_cnt = CbPendingCnt},
          State2 = maybe_spawn_handle_messages(State1),
          {ok, State} = maybe_send_fetch_request(State2),
          {noreply, State}
        end
  end;
%% handle obsolete fetch responses in case we got new offset from callback
handle_info({msg, Pid, CorrId1, _R},
            #state{last_corr_id = CorrId2, socket_pid = Pid} = State)
  when CorrId1 < CorrId2 ->
  error_logger:info_msg("~p ~p Dropping obsolete fetch response "
                       "with corr_id = ~p",
                       [?MODULE, self(), CorrId1]),
  {noreply, State};
handle_info(?SEND_FETCH_REQUEST, State) ->
  {ok, CorrId} = send_fetch_request(State),
  {noreply, State#state{last_corr_id = CorrId}};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{socket_pid = Pid} = State) ->
  {stop, {socket_down, Reason}, State};
%% callback completed
handle_info({handle_messages_result, Ref, Result}, State0) ->
  [Ref | Rest] = State0#state.cb_pending, %% assert
  CbPendingCnt = State0#state.cb_pending_cnt - 1,
  State1 = State0#state{ cb_pending     = Rest
                       , cb_pending_cnt = CbPendingCnt
                       },
  case do_handle_messages_result(Result, State1) of
    {ok, State} ->
      maybe_handle_last_error(State);
    {error, Reason} ->
      {stop, Reason, State0}
  end;
handle_info(Info, State) ->
  error_logger:warning_msg("~p [~p] got unexpected info: ~p",
                          [?MODULE, self(), Info]),
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast(Cast, State) ->
  error_logger:warning_msg("~p [~p] got unexpected cast: ~p",
                          [?MODULE, self(), Cast]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal Functions =======================================================

maybe_delay_fetch_request(SleepTime) when SleepTime > 0 ->
  erlang:send_after(SleepTime, self(), ?SEND_FETCH_REQUEST);
maybe_delay_fetch_request(_SleepTime) ->
  self() ! ?SEND_FETCH_REQUEST.

%% @private Handle error only when there is no pending handle_messages
maybe_handle_last_error(#state{ cb_pending = []
                              , cb_state   = CbState
                              , last_error = ?KAFKA_ERROR(Ts, ErrorCode, F)
                              , topic      = Topic
                              , partition  = Partition
                              } = State0) ->
  ErrorAge = timer:now_diff(os:timestamp(), Ts) div 1000,
  NextFetchTime = max(?ERROR_COOLDOWN - ErrorAge, 0),
  case F(CbState) of
    {ok, NewCbState} ->
      maybe_delay_fetch_request(NextFetchTime),
      {noreply, State0#state{ cb_state   = NewCbState
                            , last_error = undefined
                            }};
    {ok, NewCbState, NewCbOptions} ->
      {ok, State} = update_cb_options(NewCbOptions, State0),
      OldOffset = State0#state.offset,
      NewOffset0 = State#state.offset,
      SocketPid = State#state.socket_pid,
      {ok, NewOffset} = maybe_fetch_valid_offset(SocketPid, OldOffset,
                                                 NewOffset0, Topic, Partition),
      maybe_delay_fetch_request(NextFetchTime),
      {noreply, State#state{ cb_state   = NewCbState
                           , offset     = NewOffset
                           , last_error = undefined
                           }};
    stop ->
      {stop, ErrorCode, State0};
    Other ->
      {stop, {callback_error, Other}, State0}
  end;
maybe_handle_last_error(#state{} = State) ->
  {noreply, State}.

do_handle_messages_result({ok, CbState}, State0) ->
  State1 = State0#state{cb_state = CbState},
  State2 = maybe_spawn_handle_messages(State1),
  maybe_send_fetch_request(State2);
do_handle_messages_result({ok, CbState, CbOptions}, State0) ->
  {ok, State1} = update_cb_options(CbOptions, State0),
  OldOffset = State0#state.offset,
  NewOffset0 = State1#state.offset,
  SocketPid = State1#state.socket_pid,
  Topic = State1#state.topic,
  Partition = State1#state.partition,
  {ok, NewOffset} = maybe_fetch_valid_offset(SocketPid, OldOffset,
                                             NewOffset0, Topic,
                                             Partition),
  State2 = State1#state{ cb_state = CbState
                       , offset   = NewOffset
                       },
  State3 = maybe_spawn_handle_messages(State2),
  maybe_send_fetch_request(State3);
do_handle_messages_result({error, Reason}, _State) ->
  {error, {callback_error, Reason}}.

maybe_spawn_handle_messages(#state{ cb_pending = [F | Rest]
                                  , cb_state = CbState
                                  } = State) when is_function(F) ->
  Ref = make_ref(),
  Parent = self(),
  spawn_link(
    fun() ->
      Result = F(CbState),
      Parent ! {handle_messages_result, Ref, Result}
    end),
  State#state{cb_pending = [Ref | Rest]};
maybe_spawn_handle_messages(State) ->
  State.

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

%% @private Send new fetch request if no pending error.
maybe_send_fetch_request(#state{last_error = undefined} = State) ->
  case State#state.cb_pending_cnt < State#state.prefetch_count of
    true ->
      {ok, CorrId} = send_fetch_request(State),
      {ok, State#state{last_corr_id = CorrId}};
    false ->
      {ok, State}
  end;
maybe_send_fetch_request(#state{} = State) ->
  {ok, State}.

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

update_cb_options(NewCbOptions, #state{offset = OldOffset} = State) ->
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
  State1 = State#state{ offset = Offset
                      , min_bytes = MinBytes
                      , max_bytes = MaxBytes
                      , max_wait_time = MaxWaitTime
                      , sleep_timeout = SleepTimeout
                      , prefetch_count = PrefetchCount
                      },
  %% in case callback wants to fetch from a new offset, rest fetch buffer
  NewState =
    case OldOffset =/= Offset of
      true  ->
        State1;
      false ->
        State1#state{ cb_pending = []
                    , cb_pending_cnt = 0
                    , last_corr_id = undefined
                    }
    end,
  {ok, NewState}.

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
