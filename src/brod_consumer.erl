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
-export([ start_link/4
        , start_link/5
        , stop/1
        ]).

%% Kafka API
-export([ consume/6
        ]).

%% Debug API
-export([ debug/2
        , no_debug/1
        , get_socket/1
        ]).


%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------
-record(state, { hosts         :: [{string(), integer()}]
               , socket        :: #socket{}
               , topic         :: binary()
               , partition     :: integer()
               , callback      :: callback_fun()
               , offset        :: integer()
               , max_wait_time :: integer()
               , min_bytes     :: integer()
               , max_bytes     :: integer()
               , sleep_timeout :: integer()
               }).

%%%_* Macros -------------------------------------------------------------------
-define(SEND_FETCH_REQUEST, send_fetch_request).

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], binary(), integer(), integer()) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Topic, Partition, SleepTimeout) ->
  start_link(Hosts, Topic, Partition, SleepTimeout, []).

-spec start_link([{string(), integer()}], binary(), integer(),
                 integer(), [term()]) -> {ok, pid()} | {error, any()}.
start_link(Hosts, Topic, Partition, SleepTimeout, Debug) ->
  Args = [Hosts, Topic, Partition, SleepTimeout, Debug],
  Options = [{debug, Debug}],
  gen_server:start_link(?MODULE, Args, Options).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop, infinity).

-spec consume(pid(), callback_fun(), integer(), integer(),
              integer(), integer()) -> ok | {error, any()}.
consume(Pid, Callback, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  gen_server:call(Pid, {consume, Callback, Offset,
                        MaxWaitTime, MinBytes, MaxBytes},
                  infinity).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable debugging on consumer and its connection to a broker
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
debug(Pid, print) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log, print});
debug(Pid, File) when is_list(File) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log_to_file, File}).

-spec no_debug(pid()) -> ok.
%% @doc Disable debugging
no_debug(Pid) ->
  do_debug(Pid, no_debug).

-spec get_socket(pid()) -> {ok, #socket{}}.
get_socket(Pid) ->
  gen_server:call(Pid, get_socket, infinity).

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, Topic, Partition, SleepTimeout, Debug]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Metadata} = brod_utils:get_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  try
    %% detect a leader for the given partition and connect it
    Partitions =
      case lists:keyfind(Topic, #topic_metadata.name, Topics) of
        #topic_metadata{} = TM ->
          TM#topic_metadata.partitions;
        false ->
          throw({unknown_topic, Topic})
      end,
    NodeId =
      case lists:keyfind(Partition, #partition_metadata.id, Partitions) of
        #partition_metadata{leader_id = Id} ->
          Id;
        false ->
          throw({unknown_partition, Topic, Partition})
      end,
    #broker_metadata{host = Host, port = Port} =
      lists:keyfind(NodeId, #broker_metadata.node_id, Brokers),
    %% client id matters only for producer clients
    {ok, Pid} = brod_sock:start_link(self(), Host, Port,
                                     ?DEFAULT_CLIENT_ID, Debug),
    Socket = #socket{ pid = Pid
                    , host = Host
                    , port = Port
                    , node_id = NodeId},
    {ok, #state{ hosts         = Hosts
               , socket        = Socket
               , topic         = Topic
               , partition     = Partition
               , sleep_timeout = SleepTimeout}}
  catch
    throw:What ->
      {stop, What}
  end.

handle_call({consume, Callback, Offset0,
             MaxWaitTime, MinBytes, MaxBytes}, _From, State0) ->
  case get_valid_offset(Offset0, State0) of
    {error, Error} ->
      {reply, {error, Error}, State0};
    {ok, Offset} ->
      State = State0#state{ callback      = Callback
                          , offset        = Offset
                          , max_wait_time = MaxWaitTime
                          , min_bytes     = MinBytes
                          , max_bytes     = MaxBytes},
      ok = send_fetch_request(State),
      {reply, ok, State}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_socket, _From, State) ->
  {reply, {ok, State#state.socket}, State};
handle_call(Request, _From, State) ->
  {stop, {unsupported_call, Request}, State}.

handle_cast(Msg, State) ->
  {stop, {unsupported_cast, Msg}, State}.

handle_info({msg, _Pid, _CorrId, #fetch_response{} = R}, State0) ->
  case handle_fetch_response(R, State0) of
    {error, Error} ->
      {stop, {broker_error, Error}, State0};
    {ok, State} ->
      MessageSet = brod_utils:fetch_response_to_message_set(R),
      exec_callback(State#state.callback, MessageSet),
      ok = send_fetch_request(State),
      {noreply, State};
    {empty, State} ->
      erlang:send_after(State#state.sleep_timeout, self(), ?SEND_FETCH_REQUEST),
      {noreply, State}
  end;
handle_info(?SEND_FETCH_REQUEST, State) ->
  ok = send_fetch_request(State),
  {noreply, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
  {stop, {socket_down, Reason}, State};
handle_info(Info, State) ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, #state{socket = Socket}) ->
  brod_sock:stop(Socket#socket.pid).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

format_status(_Opt, [_PDict, State0]) ->
  State = lists:zip(record_info(fields, state), tl(tuple_to_list(State0))),
  [{data, [{"State", State}]}].

%%%_* Internal functions -------------------------------------------------------
get_valid_offset(Offset, _State) when Offset > 0 ->
  {ok, Offset};
get_valid_offset(Time, #state{socket = Socket} = State) ->
  Request = #offset_request{ topic = State#state.topic
                           , partition = State#state.partition
                           , time = Time},
  {ok, Response} = brod_sock:send_sync(Socket#socket.pid, Request, 5000),
  #offset_response{topics = [#offset_topic{} = Topic]} = Response,
  #offset_topic{partitions =
                 [#partition_offsets{offsets = Offsets}]} = Topic,
  case Offsets of
    [Offset] -> {ok, Offset};
    []       -> {error, no_available_offsets}
  end.

send_fetch_request(#state{socket = Socket} = State) ->
  Request = #fetch_request{ max_wait_time = State#state.max_wait_time
                          , min_bytes = State#state.min_bytes
                          , topic = State#state.topic
                          , partition = State#state.partition
                          , offset = State#state.offset
                          , max_bytes = State#state.max_bytes},
  {ok, _} = brod_sock:send(Socket#socket.pid, Request),
  ok.

handle_fetch_response(#fetch_response{topics = [TopicFetchData]}, State) ->
  #topic_fetch_data{partitions = [PM]} = TopicFetchData,
  case has_error(PM) of
    {true, Error} ->
      {error, Error};
    false ->
      case PM#partition_messages.messages of
        [] ->
          {empty, State};
        X when is_list(X) ->
          LastOffset = PM#partition_messages.last_offset,
          Offset = erlang:max(State#state.offset, LastOffset + 1),
          {ok, State#state{offset = Offset}}
      end
  end.

has_error(#partition_messages{error_code = ErrorCode}) ->
  case brod_kafka:is_error(ErrorCode) of
    true  -> {true, ErrorCode};
    false -> false
  end.

exec_callback(Callback, MessageSet) ->
  case erlang:fun_info(Callback, arity) of
    {arity, 1} ->
      try
        Callback(MessageSet)
      catch C:E ->
          erlang:C({E, erlang:get_stacktrace()})
      end;
    {arity, 3} ->
      F = fun(#message{offset = Offset, key = K, value = V}) ->
              try
                Callback(Offset, K, V)
              catch C:E ->
                  erlang:C({E, erlang:get_stacktrace()})
              end
          end,
      lists:foreach(F, MessageSet#message_set.messages)
  end.

do_debug(Pid, Debug) ->
  {ok, #socket{pid = Sock}} = get_socket(Pid),
  {ok, _} = gen:call(Sock, system, {debug, Debug}, infinity),
  {ok, _} = gen:call(Pid, system, {debug, Debug}, infinity),
  ok.

%% Tests -----------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

handle_fetch_response_test() ->
  State0 = #state{},
  PM0 = #partition_messages{error_code = ?EC_OFFSET_OUT_OF_RANGE},
  R0 = #fetch_response{topics = [#topic_fetch_data{partitions = [PM0]}]},
  ?assertEqual({error, ?EC_OFFSET_OUT_OF_RANGE},
               handle_fetch_response(R0, State0)),
  PM1 = #partition_messages{error_code = ?EC_NONE, messages = []},
  R1 = #fetch_response{topics = [#topic_fetch_data{partitions = [PM1]}]},
  ?assertEqual({empty, State0}, handle_fetch_response(R1, State0)),
  State1 = State0#state{offset = 0},
  PM2 = #partition_messages{error_code = ?EC_NONE,
                            messages = [foo],
                            last_offset = 1},
  R2 = #fetch_response{topics = [#topic_fetch_data{partitions = [PM2]}]},
  State2 = State1#state{offset = 2},
  ?assertEqual({ok, State2}, handle_fetch_response(R2, State1)),
  ok.

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
