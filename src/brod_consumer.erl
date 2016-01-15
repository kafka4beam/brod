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
-export([ start_link/4
        , start_link/5
        , stop/1
        , subscribe/3
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

-type consumer_option() :: begin_offset
                         | min_bytes
                         | max_bytes
                         | max_wait_time
                         | sleep_timeout
                         | prefetch_count.

-type options() :: [{consumer_option(), integer()}].

-type last_error() :: undefined | error_code().

-record(state, { client_pid        :: pid()
               , config            :: consumer_config()
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
               , last_error        :: last_error()
               , subscriber        :: undefined | pid()
               , pending_acks = [] :: [offset()]
               }).

-define(DEFAULT_BEGIN_OFFSET, -1).
-define(DEFAULT_MIN_BYTES, 0).
-define(DEFAULT_MAX_BYTES, 1048576).  % 1 MB
-define(DEFAULT_MAX_WAIT_TIME, 1000). % 1 sec
-define(DEFAULT_SLEEP_TIMEOUT, 1000). % 1 sec
-define(DEFAULT_PREFETCH_COUNT, 1).
-define(ERROR_COOLDOWN, 1000).

-define(SEND_FETCH_REQUEST, send_fetch_request).

%%%_* APIs =====================================================================
%% @equiv start_link(ClientPid, Topic, Partition, Config, [])
-spec start_link(pid(), topic(), partition(),
                 consumer_config()) -> {ok, pid()} | {error, any()}.
start_link(ClientPid, Topic, Partition, Config) ->
  start_link(ClientPid, Topic, Partition, Config, []).

-spec start_link(pid(), topic(), partition(),
                 consumer_config(), [any()]) -> {ok, pid()} | {error, any()}.
start_link(ClientPid, Topic, Partition, Config, Debug) ->
  Args = {ClientPid, Topic, Partition, Config},
  gen_server:start_link(?MODULE, Args, [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

-spec subscribe(pid(), pid(), options()) -> ok | {error, any()}.
subscribe(Pid, SubscriberPid, ConsumerOptions) ->
  gen_server:call(Pid, {subscribe, SubscriberPid, ConsumerOptions}).

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
  self() ! init_socket,
  {ok, #state{ client_pid     = ClientPid
             , topic          = Topic
             , partition      = Partition
             , config         = Config
             }}.

handle_info(init_socket, #state{ client_pid = ClientPid
                               , topic      = Topic
                               , partition  = Partition
                               , config     = Config
                               } = State0) ->
  %% 1. Lookup, or maybe (re-)establish a connection to partition leader
  {ok, SocketPid} =
    brod_client:get_leader_connection(ClientPid, Topic, Partition),
  _ = erlang:monitor(process, SocketPid),

  %% 2. Get options from consumer config
  MinBytes = proplists:get_value(min_bytes, Config, ?DEFAULT_MIN_BYTES),
  MaxBytes = proplists:get_value(max_bytes, Config, ?DEFAULT_MAX_BYTES),
  MaxWaitTime =
    proplists:get_value(max_wait_time, Config, ?DEFAULT_MAX_WAIT_TIME),
  SleepTimeout =
    proplists:get_value(sleep_timeout, Config, ?DEFAULT_SLEEP_TIMEOUT),
  PrefetchCount =
    proplists:get_value(prefetch_count, Config, ?DEFAULT_PREFETCH_COUNT),
  Offset = proplists:get_value(offset, Config, ?DEFAULT_BEGIN_OFFSET),

  %% 3. Retrieve valid starting offset
  State = State0#state{ socket_pid     = SocketPid
                      , begin_offset   = Offset
                      , max_wait_time  = MaxWaitTime
                      , min_bytes      = MinBytes
                      , max_bytes      = MaxBytes
                      , sleep_timeout  = SleepTimeout
                      , prefetch_count = PrefetchCount
                      },
  case resolve_begin_offset(State) of
    {ok, NewState} ->
      {noreply, NewState};
    {error, Reason} ->
      {stop, {error, Reason}, State}
  end;
%% discard fetch response when there is no (dead?) subscriber
handle_info({msg, _Pid, _CorrId, _R},
            #state{subscriber = undefined} = State) ->
  {noreply, State};
handle_info({msg, _Pid, CorrId, R},
            #state{ last_corr_id  = CorrId
                  , subscriber    = Subscriber
                  , pending_acks  = PendingAcks
                  , sleep_timeout = SleepTimeout
                  } = State0) when is_pid(Subscriber) ->
  #fetch_response{topics = [TopicFetchData]} = R,
  #topic_fetch_data{ topic = Topic
                   , partitions = [PM]} = TopicFetchData,
  #partition_messages{ partition = Partition
                     , error_code = ErrorCode
                     , high_wm_offset = HighWmOffset
                     , last_offset = LastOffset
                     , messages = Messages} = PM,
  case brod_kafka:is_error(ErrorCode) of
    true ->
      Error = #kafka_fetch_error{ topic      = Topic
                                , partition  = Partition
                                , error_code = ErrorCode
                                , error_desc = brod_kafka_errors:desc(ErrorCode)
                                },
      %% TODO, resolve some of the errors here locally
      ok = cast(Subscriber, Error),
      State = State0#state{last_error = ErrorCode},
      {noreply, State};
    false ->
      MsgSet = #kafka_message_set{ topic          = Topic
                                 , partition      = Partition
                                 , high_wm_offset = HighWmOffset
                                 , messages       = Messages
                                 },
      ok = cast(Subscriber, MsgSet),
      Offsets = lists:map(fun(#kafka_message{offset = Offset}) -> Offset end,
                          Messages),
      State = State0#state{ pending_acks = PendingAcks ++ Offsets
                          , begin_offset = LastOffset
                          },
      _ = maybe_delay_fetch_request(SleepTimeout),
      {noreply, State}
  end;
%% handle obsolete fetch responses in case we got new offset from callback
handle_info({msg, Pid, CorrId1, _R},
            #state{last_corr_id = CorrId2, socket_pid = Pid} = State)
  when CorrId1 < CorrId2 ->
  error_logger:info_msg("~p ~p Dropping obsolete fetch response "
                        "with corr_id = ~p",
                        [?MODULE, self(), CorrId1]),
  {noreply, State};
handle_info(?SEND_FETCH_REQUEST, State0) ->
  {ok, State} = maybe_send_fetch_request(State0),
  {noreply, State};
handle_info({'DOWN', _MonitorRef, process, Pid, _Reason},
            #state{subscriber = Pid} = State) ->
  error_logger:info_msg("~p ~p subscriber ~p is down",
                        [?MODULE, self(), Pid]),
  {noreply, State#state{ subscriber   = undefined
                       , last_error   = undefined
                       , pending_acks = []
                       }};
handle_info({'DOWN', _MonitorRef, process, Pid, Reason},
            #state{socket_pid = Pid} = State) ->
  {stop, {socket_down, Reason}, State};
handle_info(Info, State) ->
  error_logger:warning_msg("~p ~p got unexpected info: ~p",
                          [?MODULE, self(), Info]),
  {noreply, State}.

handle_call({subscribe, Pid, Options}, _From,
            #state{subscriber = Subscriber} = State) ->
  case Subscriber =:= undefined orelse Subscriber =:= Pid of
    true ->
      case update_options(Options, State) of
        {ok, NewState} ->
          {reply, ok, NewState#state{subscriber = Pid}};
        {error, Reason} ->
          {reply, {error, Reason}, State}
      end;
    false ->
      {reply, {error, {already_subscribed_by, Subscriber}}, State}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast(Cast, State) ->
  error_logger:warning_msg("~p ~p got unexpected cast: ~p",
                          [?MODULE, self(), Cast]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal Functions =======================================================

cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

maybe_delay_fetch_request(SleepTime) when SleepTime > 0 ->
  erlang:send_after(SleepTime, self(), ?SEND_FETCH_REQUEST);
maybe_delay_fetch_request(_SleepTime) ->
  self() ! ?SEND_FETCH_REQUEST.

%% @private Send new fetch request if no pending error.
maybe_send_fetch_request(#state{last_error = undefined} = State) ->
  case length(State#state.pending_acks) < State#state.prefetch_count of
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
                          , offset = State#state.begin_offset
                          , max_wait_time = State#state.max_wait_time
                          , min_bytes = State#state.min_bytes
                          , max_bytes = State#state.max_bytes},
  brod_sock:send(SocketPid, Request).

do_debug(Pid, Debug) ->
  {ok, _} = gen:call(Pid, system, {debug, Debug}, infinity),
  ok.

-spec update_options(options(), #state{}) -> {ok, #state{}} | {error, any()}.
update_options(Options, #state{begin_offset = OldBeginOffset} = State) ->
  F = fun(Name, Default) -> proplists:get_value(Name, Options, Default) end,
  NewState =
    State#state{ begin_offset   = F(begin_offset, State#state.begin_offset)
               , min_bytes      = F(min_bytes, State#state.min_bytes)
               , max_bytes      = F(max_bytes, State#state.max_bytes)
               , max_wait_time  = F(max_wait_time, State#state.max_wait_time)
               , sleep_timeout  = F(sleep_timeout, State#state.sleep_timeout)
               , prefetch_count = F(prefetch_count, State#state.prefetch_count)
               },
  case NewState#state.begin_offset =:= OldBeginOffset of
    true  ->
      {ok, NewState};
    false ->
      %% reset buffer in case subscriber wants to fetch from a new offset
      NewState1 = NewState#state{ last_error   = undefined
                                , pending_acks = []
                                },
      resolve_begin_offset(NewState1)
  end.

-spec resolve_begin_offset(#state{}) -> {ok, #state{}} | {error, any()}.
resolve_begin_offset(#state{ begin_offset = BeginOffset
                           , socket_pid   = SocketPid
                           , topic        = Topic
                           , partition    = Partition
                           } = State) ->
  case fetch_valid_offset(SocketPid, BeginOffset, Topic, Partition) of
    {ok, NewBeginOffset} ->
      {ok, State#state{begin_offset = NewBeginOffset}};
    {error, Reason} ->
      {error, Reason}
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

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
