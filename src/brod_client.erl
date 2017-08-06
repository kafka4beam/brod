%%%
%%%   Copyright (c) 2015-2017 Klarna AB
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

-module(brod_client).
-behaviour(gen_server).

-export([ get_connection/3
        , get_consumer/3
        , get_group_coordinator/2
        , get_leader_connection/3
        , get_metadata/2
        , get_partitions_count/2
        , get_producer/3
        , register_consumer/3
        , register_producer/3
        , start_link/3
        , start_producer/3
        , start_consumer/3
        , stop/1
        , stop_producer/2
        , stop_consumer/2
        ]).

%% Private export
-export([ find_producer/3
        , find_consumer/3
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-export_type([config/0]).

-include("brod_int.hrl").

-define(DEFAULT_RECONNECT_COOL_DOWN_SECONDS, 1).
-define(DEFAULT_GET_METADATA_TIMEOUT_SECONDS, 5).
-define(DEFAULT_MAX_METADATA_SOCK_RETRY, 1).

-define(dead_since(TS, REASON), {dead_since, TS, REASON}).
-type dead_socket() :: ?dead_since(erlang:timestamp(), any()).

%% ClientId as ets table name.
-define(ETS(ClientId), ClientId).
-define(PRODUCER_KEY(Topic, Partition),
        {producer, Topic, Partition}).
-define(CONSUMER_KEY(Topic, Partition),
        {consumer, Topic, Partition}).
-define(TOPIC_METADATA_KEY(Topic),
        {topic_metadata, Topic}).
-define(PRODUCER(Topic, Partition, Pid),
        {?PRODUCER_KEY(Topic, Partition), Pid}).
-define(CONSUMER(Topic, Partition, Pid),
        {?CONSUMER_KEY(Topic, Partition), Pid}).

-define(UNKNOWN_TOPIC_CACHE_EXPIRE_SECONDS, 120).

-type endpoint() :: brod:endpoint().
-type client() :: brod:client().
-type client_id() :: brod:client_id().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type config() :: proplists:proplist().
-type group_id() :: brod:group_id().
-type hostname() :: brod:hostname().
-type portnum() :: brod:portnum().

-type partition_worker_key() :: ?PRODUCER_KEY(topic(), partition())
                              | ?CONSUMER_KEY(topic(), partition()).

-type get_producer_error() :: client_down
                            | {producer_down, noproc}
                            | {producer_not_found, topic()}
                            | { producer_not_found
                              , topic()
                              , partition()}.

-type get_consumer_error() :: client_down
                            | {consumer_down, noproc}
                            | {consumer_not_found, topic()}
                            | {consumer_not_found, topic(), partition()}.

-type get_worker_error() :: get_producer_error()
                          | get_consumer_error().

-record(sock,
        { endpoint :: endpoint()
        , sock_pid :: ?undef | pid() | dead_socket()
        }).

-record(state,
        { client_id            :: client_id()
        , bootstrap_endpoints  :: [endpoint()]
        , meta_sock_pid        :: ?undef | pid() | dead_socket()
        , payload_sockets = [] :: [#sock{}]
        , producers_sup        :: ?undef | pid()
        , consumers_sup        :: ?undef | pid()
        , config               :: ?undef | config()
        , workers_tab          :: ?undef | ets:tab()
        }).

-type state() :: #state{}.

%%%_* APIs =====================================================================

-spec start_link([endpoint()], client_id(), config()) ->
        {ok, pid()} | {error, any()}.
start_link(BootstrapEndpoints, ClientId, Config) when is_atom(ClientId) ->
  Args = {BootstrapEndpoints, ClientId, Config},
  gen_server:start_link({local, ClientId}, ?MODULE, Args, []).

-spec stop(client()) -> ok.
stop(Client) ->
  Mref = erlang:monitor(process, Client),
  _ = safe_gen_call(Client, stop, infinity),
  receive
    {'DOWN', Mref, process, _, _Reason} ->
      ok
  end.

%% @doc Get producer of the given topic-partition.
%% The producer is started if auto_start_producers is
%% enabled in client config.
%% @end
-spec get_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_producer_error()}.
get_producer(Client, Topic, Partition) ->
  case get_partition_worker(Client, ?PRODUCER_KEY(Topic, Partition)) of
    {ok, Pid} ->
      {ok, Pid};
    {error, {producer_not_found, Topic}} = Error ->
      %% try to start a producer for the given topic if
      %% auto_start_producers option is enabled for the client
      maybe_start_producer(Client, Topic, Partition, Error);
    Error ->
      Error
  end.

%% @doc Get consumer of the given topic-parition.
-spec get_consumer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_consumer_error()}.
get_consumer(Client, Topic, Partition) ->
  get_partition_worker(Client, ?CONSUMER_KEY(Topic, Partition)).

%% @doc Dynamically start a per-topic producer.
%% Return ok if the producer is already started.
%% @end
-spec start_producer(client(), topic(), brod:producer_config()) ->
        ok | {error, any()}.
start_producer(Client, TopicName, ProducerConfig) ->
  case get_producer(Client, TopicName, _Partition = 0) of
    {ok, _Pid} ->
      ok; %% already started
    {error, {producer_not_found, TopicName}} ->
      Call = {start_producer, TopicName, ProducerConfig},
      safe_gen_call(Client, Call, infinity);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Stop all partition producers of the given topic.
-spec stop_producer(client(), topic()) -> ok | {error, any()}.
stop_producer(Client, TopicName) ->
  safe_gen_call(Client, {stop_producer, TopicName}, infinity).

%% @doc Dynamically start a topic consumer.
%% Returns ok if the consumer is already started.
%% @end.
-spec start_consumer(client(), topic(), brod:consumer_config()) ->
                        ok | {error, any()}.
start_consumer(Client, TopicName, ConsumerConfig) ->
  case get_consumer(Client, TopicName, _Partition = 0) of
    {ok, _Pid} ->
      ok; %% already started
    {error, {consumer_not_found, TopicName}} ->
      Call = {start_consumer, TopicName, ConsumerConfig},
      safe_gen_call(Client, Call, infinity);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Stop all partition consumers of the given topic.
-spec stop_consumer(client(), topic()) -> ok | {error, any()}.
stop_consumer(Client, TopicName) ->
  safe_gen_call(Client, {stop_consumer, TopicName}, infinity).

%% @doc Get the connection to kafka broker which is a leader
%% for given Topic/Partition.
%% If there is no such connection established yet, try to establish a new one.
%% If the connection is already established per request from another
%% producer/consumer the same socket is returned.
%% If the old connection was dead less than a configurable N seconds ago,
%% {error, LastReason} is returned.
%% @end
-spec get_leader_connection(client(), topic(), partition()) ->
        {ok, pid()} | {error, any()}.
get_leader_connection(Client, Topic, Partition) ->
  case get_metadata(Client, Topic) of
    {ok, Metadata} ->
      case brod_utils:find_leader_in_metadata(Metadata, Topic, Partition) of
        {ok, {Host, Port}} -> get_connection(Client, Host, Port);
        {error, Reason}    -> {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Get topic metadata, if topic is 'undefined', will fetch ALL metadata.
-spec get_metadata(client(), ?undef | topic()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Client, Topic) ->
  safe_gen_call(Client, {get_metadata, Topic}, infinity).

%% @doc Get number of partitions for a given topic.
-spec get_partitions_count(client(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Topic) when is_atom(Client) ->
  %% Ets =:= ClientId
  get_partitions_count(Client, Client, Topic);
get_partitions_count(Client, Topic) when is_pid(Client) ->
  %% TODO: remove this clause when brod:start_link_client/_ is removed.
  case safe_gen_call(Client, get_workers_table, infinity) of
    {ok, Ets}       -> get_partitions_count(Client, Ets, Topic);
    {error, Reason} -> {error, Reason}
  end.

%% @doc Get the endpoint of the group coordinator broker.
-spec get_group_coordinator(client(), group_id()) ->
        {ok, {endpoint(), config()}} | {error, any()}.
get_group_coordinator(Client, GroupId) ->
  safe_gen_call(Client, {get_group_coordinator, GroupId}, infinity).

%% @doc Register self() as a partition producer. The pid is registered in an ETS
%% table, then the callers may lookup a producer pid from the table and make
%% produce requests to the producer process directly.
%% @end
-spec register_producer(client(), topic(), partition()) -> ok.
register_producer(Client, Topic, Partition) ->
  Producer = self(),
  Key = ?PRODUCER_KEY(Topic, Partition),
  gen_server:cast(Client, {register, Key, Producer}).

%% @doc Register self() as a partition consumer. The pid is registered in an ETS
%% table, then the callers may lookup a consumer pid from the table ane make
%% subscribe calls to the process directly.
%% @end
register_consumer(Client, Topic, Partition) ->
  Consumer = self(),
  Key = ?CONSUMER_KEY(Topic, Partition),
  gen_server:cast(Client, {register, Key, Consumer}).

%% @doc Get the connection to kafka broker at Host:Port.
%% If there is no such connection established yet, try to establish a new one.
%% If the connection is already established per request from another producer
%% the same socket is returned.
%% If the old connection was dead less than a configurable N seconds ago,
%% {error, LastReason} is returned.
%% @end
-spec get_connection(client(), hostname(), portnum()) ->
                        {ok, pid()} | {error, any()}.
get_connection(Client, Host, Port) ->
  safe_gen_call(Client, {get_connection, Host, Port}, infinity).

%%%_* gen_server callbacks =====================================================

init({BootstrapEndpoints, ClientId, Config}) ->
  erlang:process_flag(trap_exit, true),
  Tab = ets:new(?ETS(ClientId),
                [named_table, protected, {read_concurrency, true}]),
  self() ! init,
  {ok, #state{ client_id           = ClientId
             , bootstrap_endpoints = BootstrapEndpoints
             , config              = Config
             , workers_tab         = Tab
             }}.

handle_info(init, State) ->
  ClientId = State#state.client_id,
  Endpoints = State#state.bootstrap_endpoints,
  {ok, MetaSock, ReorderedEndpoints} =
    start_metadata_socket(ClientId, Endpoints, State#state.config),
  {ok, ProducersSupPid} = brod_producers_sup:start_link(),
  {ok, ConsumersSupPid} = brod_consumers_sup:start_link(),
  {noreply, State#state{ bootstrap_endpoints = ReorderedEndpoints
                       , meta_sock_pid       = MetaSock
                       , producers_sup       = ProducersSupPid
                       , consumers_sup       = ConsumersSupPid
                       }};

handle_info({'EXIT', Pid, Reason}, #state{ client_id     = ClientId
                                         , producers_sup = Pid
                                         } = State) ->
  error_logger:error_msg("client ~p producers supervisor down~nReason: ~p",
                         [ClientId, Pid, Reason]),
  {stop, {producers_sup_down, Reason}, State};
handle_info({'EXIT', Pid, Reason}, #state{ client_id     = ClientId
                                         , consumers_sup = Pid
                                         } = State) ->
  error_logger:error_msg("client ~p consumers supervisor down~nReason: ~p",
                         [ClientId, Pid, Reason]),
  {stop, {consumers_sup_down, Reason}, State};
handle_info({'EXIT', Pid, Reason},
            #state{ client_id           = ClientId
                  , meta_sock_pid       = Pid
                  , bootstrap_endpoints = BootstrapEndpoints0
                  } = State) ->
  [{Host, Port} | Rest] = BootstrapEndpoints0,
  error_logger:info_msg("client ~p metadata socket down ~s:~p~nReason:~p",
                        [ClientId, Host, Port, Reason]),
  %% move the newly failed endpoint to the last in the list
  BootstrapEndpoints = Rest ++ [{Host, Port}],
  NewState =
    State#state{ meta_sock_pid       = ?dead_since(os:timestamp(), Reason)
               , bootstrap_endpoints = BootstrapEndpoints
               },
  {noreply, NewState};
handle_info({'EXIT', Pid, Reason}, State) ->
  {ok, NewState} = handle_socket_down(State, Pid, Reason),
  {noreply, NewState};
handle_info(Info, State) ->
  error_logger:warning_msg("~p [~p] ~p got unexpected info: ~p",
                          [?MODULE, self(), State#state.client_id, Info]),
  {noreply, State}.

handle_call({stop_producer, Topic}, _From, State) ->
  ok = brod_producers_sup:stop_producer(State#state.producers_sup, Topic),
  {reply, ok, State};
handle_call({stop_consumer, Topic}, _From, State) ->
  ok = brod_consumers_sup:stop_consumer(State#state.consumers_sup, Topic),
  {reply, ok, State};
handle_call({get_group_coordinator, GroupId}, _From, State) ->
  #state{config = Config} = State,
  Timeout = proplists:get_value(get_metadata_timout_seconds, Config,
                                ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  {Result, NewState} =
    do_get_group_coordinator(State, GroupId, timer:seconds(Timeout)),
  {reply, Result, NewState};
handle_call({start_producer, TopicName, ProducerConfig}, _From, State) ->
  {Reply, NewState} = do_start_producer(TopicName, ProducerConfig, State),
  {reply, Reply, NewState};
handle_call({start_consumer, TopicName, ConsumerConfig}, _From, State) ->
  {Reply, NewState} = do_start_consumer(TopicName, ConsumerConfig, State),
  {reply, Reply, NewState};
handle_call({auto_start_producer, Topic}, _From, State) ->
  Config = State#state.config,
  case proplists:get_value(auto_start_producers, Config, false) of
    true ->
      ProducerConfig =
        proplists:get_value(default_producer_config, Config, []),
      {Reply, NewState} = do_start_producer(Topic, ProducerConfig, State),
      {reply, Reply, NewState};
    false ->
      {reply, {error, disabled}, State}
  end;
handle_call(get_workers_table, _From, State) ->
  %% TODO: remove this clause when brod:start_link_client/_ is removed
  {reply, {ok, State#state.workers_tab}, State};
handle_call(get_producers_sup_pid, _From, State) ->
  {reply, {ok, State#state.producers_sup}, State};
handle_call(get_consumers_sup_pid, _From, State) ->
  {reply, {ok, State#state.consumers_sup}, State};
handle_call({get_metadata, Topic}, _From, State) ->
  {Result, NewState} = do_get_metadata(Topic, State),
  {reply, Result, NewState};
handle_call({get_connection, Host, Port}, _From, State) ->
  {NewState, Result} = do_get_connection(State, Host, Port),
  {reply, Result, NewState};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({register, Key, Pid}, #state{workers_tab = Tab} = State) ->
  ets:insert(Tab, {Key, Pid}),
  {noreply, State};
handle_cast(Cast, State) ->
  error_logger:warning_msg("~p [~p] ~p got unexpected cast: ~p",
                          [?MODULE, self(), State#state.client_id, Cast]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(Reason, #state{ client_id       = ClientId
                        , meta_sock_pid   = MetaSock
                        , payload_sockets = Sockets
                        , producers_sup   = ProducersSup
                        , consumers_sup   = ConsumersSup
                        }) ->
  case brod_utils:is_normal_reason(Reason) of
    true ->
      ok;
    false ->
      error_logger:warning_msg("~p [~p] ~p is terminating\nreason: ~p~n",
                               [?MODULE, self(), ClientId, Reason])
  end,
  %% stop producers and consumers first because they are monitoring socket pids
  brod_utils:shutdown_pid(ProducersSup),
  brod_utils:shutdown_pid(ConsumersSup),
  brod_utils:shutdown_pid(MetaSock),
  lists:foreach(
    fun(#sock{sock_pid = Pid}) -> brod_utils:shutdown_pid(Pid) end, Sockets).

%%%_* Internal Functions =======================================================

-spec get_partition_worker(client(), partition_worker_key()) ->
        {ok, pid()} | {error, get_worker_error()}.
get_partition_worker(ClientPid, Key) when is_pid(ClientPid) ->
  case erlang:process_info(ClientPid, registered_name) of
    {registered_name, ClientId} when is_atom(ClientId) ->
      get_partition_worker(ClientId, Key);
    _ ->
      %% This is a client process started without registered name
      %% have to call the process to get the producer/consumer worker
      %% process registration table.
      case safe_gen_call(ClientPid, get_workers_table, infinity) of
        {ok, Ets}       -> lookup_partition_worker(ClientPid, Ets, Key);
        {error, Reason} -> {error, Reason}
      end
  end;
get_partition_worker(ClientId, Key) when is_atom(ClientId) ->
  lookup_partition_worker(ClientId, ?ETS(ClientId), Key).

-spec lookup_partition_worker(client(), ets:tab(), partition_worker_key()) ->
        {ok, pid()} | { error, get_worker_error()}.
lookup_partition_worker(Client, Ets, Key) ->
  try ets:lookup(Ets, Key) of
    [] ->
      %% not yet registered, 2 possible reasons:
      %% 1. caller is too fast, producers/consumers are starting up
      %%    make a synced call all the way down the supervision tree
      %%    to the partition producer sup should resolve the race
      %% 2. bad argument, no such worker started, supervisors should know
      find_partition_worker(Client, Key);
    [?PRODUCER(_Topic, _Partition, Pid)] ->
      {ok, Pid};
    [?CONSUMER(_Topic, _Partition, Pid)] ->
      {ok, Pid}
  catch
    error : badarg ->
      {error, client_down}
  end.

-spec find_partition_worker(client(), partition_worker_key()) ->
        {ok, pid()} | {error, get_worker_error()}.
find_partition_worker(Client, ?PRODUCER_KEY(Topic, Partition)) ->
  find_producer(Client, Topic, Partition);
find_partition_worker(Client, ?CONSUMER_KEY(Topic, Partition)) ->
  find_consumer(Client, Topic, Partition).

-spec find_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_producer_error()}.
find_producer(Client, Topic, Partition) ->
  case safe_gen_call(Client, get_producers_sup_pid, infinity) of
    {ok, SupPid} ->
      brod_producers_sup:find_producer(SupPid, Topic, Partition);
    {error, Reason} ->
      {error, Reason}
  end.

-spec find_consumer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_consumer_error()}.
find_consumer(Client, Topic, Partition) ->
  case safe_gen_call(Client, get_consumers_sup_pid, infinity) of
    {ok, SupPid} ->
      brod_consumers_sup:find_consumer(SupPid, Topic, Partition);
    {error, Reason} ->
      {error, Reason}
  end.

%% @private
-spec validate_topic_existence(topic(), state(), boolean()) ->
        {Result, state()} when Result :: ok | {error, any()}.
validate_topic_existence(Topic, #state{workers_tab = Ets} = State, IsRetry) ->
  case lookup_partitions_count_cache(Ets, Topic) of
    {ok, _Count} ->
      {ok, State};
    {error, Reason} ->
      {{error, Reason}, State};
    false when IsRetry ->
      {{error, ?EC_UNKNOWN_TOPIC_OR_PARTITION}, State};
    false ->
      %% Try fetch metadata (and populate partition count cache)
      %% Then validate topic existence again.
      with_ok(get_metadata_safe(Topic, State),
              fun(_, S) -> validate_topic_existence(Topic, S, true) end)
  end.

%% @private Continue with {{ok, Result}, NewState}
%% return whatever error immediately.
%% @end
-spec with_ok(Result, fun((_, state()) -> Result)) ->
        Result when Result :: {ok | {ok, term()} | {error, any()}, state()}.
with_ok({ok, State}, Continue) -> Continue(ok, State);
with_ok({{ok, OK}, State}, Continue) -> Continue(OK, State);
with_ok({{error, _}, #state{}} = Return, _Continue) -> Return.

%% @private If allow_topic_auto_creation is set 'false',
%% do not try to fetch metadata per topic name, fetch all topics instead.
%% As sending metadata request with topic name will cause an auto creation
%% of the topic if auto.create.topics.enable is enabled in broker config.
%% @end
-spec get_metadata_safe(topic(), state()) -> {Result, state()}
        when Result :: {ok, kpro:struct()} | {error, any()}.
get_metadata_safe(Topic0, #state{config = Config} = State) ->
  Topic =
    case proplists:get_value(allow_topic_auto_creation, Config, true) of
      true  -> Topic0;
      false -> ?undef
    end,
  do_get_metadata(Topic, State).

%% @private
-spec do_get_metadata(?undef | topic(), state()) -> {Result, state()}
        when Result :: {ok, kpro:struct()} | {error, any()}.
do_get_metadata(Topic, #state{ client_id   = ClientId
                             , config      = Config
                             , workers_tab = Ets
                             } = State) ->
  Topics = case Topic of
             ?undef -> []; %% in case no topic is given, get all
             _      -> [Topic]
           end,
  Vsn = 0, %% TODO: pick version
  Request = kpro:req(metadata_request, Vsn, [{topics, Topics}]),
  Timeout = proplists:get_value(get_metadata_timout_seconds, Config,
                                ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  {Result, NewState} = request_sync(State, Request, timer:seconds(Timeout)),
  case Result of
    {ok, #kpro_rsp{tag = metadata_response, vsn = Vsn, msg = Metadata}} ->
      TopicMetadataArray = kf(topic_metadata, Metadata),
      ok = update_partitions_count_cache(Ets, TopicMetadataArray),
      {{ok, Metadata}, NewState};
    {error, Reason} ->
      error_logger:error_msg("~p failed to fetch metadata for topics: ~p\n"
                             "reason=~p",
                             [ClientId, Topics, Reason]),
      {Result, NewState}
  end.

%% @private
-spec do_get_group_coordinator(state(), group_id(), timeout()) ->
        {Result, state()} when Result :: {ok, endpoint()} | {error, any()}.
do_get_group_coordinator(#state{config = Config} = State, GroupId, Timeout) ->
  Req = kpro:req(group_coordinator_request, _Vsn = 0, [{group_id, GroupId}]),
  with_ok(
    request_sync(State, Req, Timeout),
    fun(#kpro_rsp{msg = Msg}, NewState) ->
        EC = kf(error_code, Msg),
        Result =
          case ?IS_ERROR(EC) of
            true ->
              {error, EC}; %% OBS: {error, EC} is used by group coordinator
            false ->
              Coordinator = kf(coordinator, Msg),
              Host = kf(host, Coordinator),
              Port = kf(port, Coordinator),
              {ok, {{binary_to_list(Host), Port}, Config}}
          end,
        {Result, NewState}
    end).

%% @private
-spec do_get_connection(state(), hostname(), portnum()) ->
        {state(), Result} when Result :: {ok, pid()} | {error, any()}.
do_get_connection(#state{} = State, Host, Port) ->
  case find_socket(State, Host, Port) of
    {ok, Pid} ->
      {State, {ok, Pid}};
    {error, Reason} ->
      maybe_connect(State, Host, Port, Reason)
  end.

%% @private
-spec maybe_connect(state(), hostname(), portnum(), Reason) ->
        {state(), Result} when
          Reason :: not_found | dead_socket(),
          Result :: {ok, pid()} | {error, any()}.
maybe_connect(State, Host, Port, not_found) ->
  %% connect for the first time
  connect(State, Host, Port);
maybe_connect(#state{client_id = ClientId} = State,
                Host, Port, ?dead_since(Ts, Reason)) ->
  case is_cooled_down(Ts, State) of
    true ->
      connect(State, Host, Port);
    false ->
      error_logger:error_msg("~p (re)connect to ~s:~p aborted, "
                             "last failure reason:~p",
                             [ClientId, Host, Port, Reason]),
     {State, {error, Reason}}
  end.

%% @private
-spec connect(state(), hostname(), portnum()) -> {state(), Result}
        when Result :: {ok, pid()} | {error, any()}.
connect(#state{ client_id       = ClientId
              , payload_sockets = Sockets
              , config          = Config
              } = State, Host, Port) ->
  Endpoint = {Host, Port},
  case brod_sock:start_link(self(), Host, Port, ClientId, Config) of
    {ok, Pid} ->
      S = #sock{ endpoint = Endpoint
               , sock_pid = Pid
               },
      error_logger:info_msg("client ~p connected to ~s:~p~n",
                            [ClientId, Host, Port]),
      NewSockets = lists:keystore(Endpoint, #sock.endpoint, Sockets, S),
      {State#state{payload_sockets = NewSockets}, {ok, Pid}};
    {error, Reason} ->
      error_logger:error_msg("client ~p failed to connect to ~s:~p~n"
                             "reason:~p",
                             [ClientId, Host, Port, Reason]),
      {ok, Sock} = mark_socket_dead(#sock{endpoint = Endpoint}, Reason),
      NewSockets = lists:keystore(Endpoint, #sock.endpoint, Sockets, Sock),
      {State#state{payload_sockets = NewSockets}, {error, Reason}}
  end.

%% @private Handle socket pid EXIT event, keep the timestamp.
%% But do not restart yet. Connection will be re-established when a
%% per-partition producer restarts and requests for a connection after
%% it is cooled down.
%% @end
-spec handle_socket_down(state(), pid(), any()) -> {ok, state()}.
handle_socket_down(#state{ client_id       = ClientId
                         , payload_sockets = Sockets
                         } = State, Pid, Reason) ->
  case lists:keyfind(Pid, #sock.sock_pid, Sockets) of
    #sock{endpoint = {Host, Port} = Endpoint} = Socket ->
      error_logger:info_msg("client ~p: payload socket down ~s:~p~n"
                            "reason:~p",
                            [ClientId, Host, Port, Reason]),
      {ok, NewSocket} = mark_socket_dead(Socket, Reason),
      NewSockets = lists:keystore(Endpoint, #sock.endpoint, Sockets, NewSocket),
      {ok, State#state{payload_sockets = NewSockets}};
    false ->
      %% is_pid_alive is checked and reconnect is done for metadata
      %% socket in maybe_restart_metadata_socket, hence the 'EXIT' message
      %% of old metadata socket pid may end up in this clause, simply ignore
      {ok, State}
  end.

%% @private
-spec mark_socket_dead(#sock{}, any()) -> {ok, #sock{}}.
mark_socket_dead(Socket, Reason) ->
  {ok, Socket#sock{sock_pid = ?dead_since(os:timestamp(), Reason)}}.

%% @private
-spec find_socket(state(), hostname(), portnum()) ->
        {ok, pid()} %% normal case
      | {error, not_found} %% first call
      | {error, dead_socket()}.
find_socket(#state{payload_sockets = Sockets}, Host, Port) ->
  case lists:keyfind({Host, Port}, #sock.endpoint, Sockets) of
    #sock{sock_pid = Pid} when is_pid(Pid)         -> {ok, Pid};
    #sock{sock_pid = ?dead_since(_, _) = NotAlive} -> {error, NotAlive};
    false                                          -> {error, not_found}
  end.

%% @private Check if the socket is down for long enough to retry.
is_cooled_down(Ts, #state{config = Config}) ->
  Threshold = proplists:get_value(reconnect_cool_down_seconds, Config,
                                  ?DEFAULT_RECONNECT_COOL_DOWN_SECONDS),
  Now = os:timestamp(),
  Diff = timer:now_diff(Now, Ts) div 1000000,
  Diff >= Threshold.

%% @private Establish a dedicated socket to kafka cluster bootstrap endpoint(s)
%% for metadata retrievals. Failed endpoints are moved to the end of the list
%% so that future retries will start from one that has not tried yet, or the
%% one failed longest ago.
%%
%% NOTE: This socket is not intended for kafka payload. This is to avoid
%%       burst of connection usage when many partition producers (re)start
%%       at same time, if we always start a new socket to fetch metadata.
%% NOTE: crash in case failed to connect to all of the endpoints.
%%       should be restarted by supervisor.
%% @end
-spec start_metadata_socket(client_id(), [endpoint()], config()) ->
        {ok, pid(),  [endpoint()]}.
start_metadata_socket(ClientId, [_|_] = Endpoints, Config) ->
  start_metadata_socket(ClientId, Endpoints, Config,
                        _FailedEndpoints = [], _Reason = ?undef).

%% @private
start_metadata_socket(_ClientId, [], _Config, FailedEndpoints, Reason) ->
  erlang:error({Reason, FailedEndpoints});
start_metadata_socket(ClientId, [Endpoint | Rest] = Endpoints, Config,
                      FailedEndpoints, _Reason) ->
  {Host, Port} = Endpoint,
  case brod_sock:start_link(self(), Host, Port, ClientId, Config) of
    {ok, Pid} ->
      ReorderedEndpoints = Endpoints ++ lists:reverse(FailedEndpoints),
      {ok, Pid, ReorderedEndpoints};
    {error, Reason} ->
      start_metadata_socket(ClientId, Rest, Config,
                            [Endpoint | FailedEndpoints], Reason)
  end.

%% @private
-spec update_partitions_count_cache(ets:tab(), [kpro:struct()]) -> ok.
update_partitions_count_cache(_Ets, []) -> ok;
update_partitions_count_cache(Ets, [TopicMetadata | Rest]) ->
  Topic = kf(topic, TopicMetadata),
  case do_get_partitions_count(TopicMetadata) of
    {ok, Cnt} ->
      ets:insert(Ets, {?TOPIC_METADATA_KEY(Topic), Cnt, os:timestamp()});
    {error, ?EC_UNKNOWN_TOPIC_OR_PARTITION} = Err ->
      ets:insert(Ets, {?TOPIC_METADATA_KEY(Topic), Err, os:timestamp()});
    {error, _Reason} ->
      ok
  end,
  update_partitions_count_cache(Ets, Rest).

%% @private Get partition counter from cache.
%% If cache is not hit, send meta data request to retrieve.
%% @end
-spec get_partitions_count(client(), ets:tab(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Ets, Topic) ->
  case lookup_partitions_count_cache(Ets, Topic) of
    {ok, Result} ->
      {ok, Result};
    {error, Reason} ->
      {error, Reason};
    false ->
      %% This call should populate the cache
      case get_metadata(Client, Topic) of
        {ok, Meta} ->
          [TopicMetadata] = kf(topic_metadata, Meta),
          do_get_partitions_count(TopicMetadata);
        {error, Reason} ->
          {error, Reason}
      end
  end.

%% @private
-spec lookup_partitions_count_cache(ets:tab(), ?undef | topic()) ->
        {ok, pos_integer()} | {error, any()} | false.
lookup_partitions_count_cache(_Ets, ?undef) -> false;
lookup_partitions_count_cache(Ets, Topic) ->
  try ets:lookup(Ets, ?TOPIC_METADATA_KEY(Topic)) of
    [{_, Count, _Ts}] when is_integer(Count) ->
      {ok, Count};
    [{_, {error, Reason}, Ts}] ->
      case timer:now_diff(os:timestamp(), Ts) =<
            ?UNKNOWN_TOPIC_CACHE_EXPIRE_SECONDS * 1000000 of
        true  -> {error, Reason};
        false -> false
      end;
    [] ->
      false
  catch
    error : badarg ->
      {error, client_down}
  end.

%% @private
-spec do_get_partitions_count(kpro:struct()) ->
        {ok, pos_integer()} | {error, any()}.
do_get_partitions_count(TopicMetadata) ->
  ErrorCode = kf(topic_error_code, TopicMetadata),
  Partitions = kf(partition_metadata, TopicMetadata),
  case ?IS_ERROR(ErrorCode) of
    true  -> {error, ErrorCode};
    false -> {ok, erlang:length(Partitions)}
  end.

%% @private
-spec maybe_start_producer(client(), topic(),
                           partition(), {error, any()}) ->
        ok | {error, any()}.
maybe_start_producer(Client, Topic, Partition, Error) ->
  case safe_gen_call(Client, {auto_start_producer, Topic}, infinity) of
    ok ->
      get_partition_worker(Client, ?PRODUCER_KEY(Topic, Partition));
    {error, disabled} ->
      Error;
    {error, Reason} ->
      {error, Reason}
  end.

%% @private
-spec request_sync(state(), kpro:req(), timeout()) ->
        {Result, state()} when Result :: {ok, kpro:rsp()} | {error, any()}.
request_sync(State, Request, Timeout) ->
  #state{config = Config} = State,
  MaxRetry = proplists:get_value(max_metadata_sock_retry, Config,
                                 ?DEFAULT_MAX_METADATA_SOCK_RETRY),
  request_sync(State, Request, Timeout, MaxRetry).

%% @private
request_sync(State0, Request, Timeout, RetryLeft) ->
  {ok, State} = maybe_restart_metadata_socket(State0),
  SockPid = State#state.meta_sock_pid,
  case brod_sock:request_sync(SockPid, Request, Timeout) of
    {error, tcp_closed} when RetryLeft > 0 ->
      {ok, NewState} = rotate_endpoints(State, tcp_closed),
      request_sync(NewState, Request, Timeout, RetryLeft - 1);
    {error, {sock_down, _} = Reason} when RetryLeft > 0 ->
      {ok, NewState} = rotate_endpoints(State, Reason),
      request_sync(NewState, Request, Timeout, RetryLeft - 1);
    Result ->
      {Result, State}
  end.

%% @private Move the newly failed endpoint to the last in the list.
-spec rotate_endpoints(state(), any()) -> {ok, state()}.
rotate_endpoints(State, Reason) ->
  #state{ client_id           = ClientId
        , bootstrap_endpoints = BootstrapEndpoints0
        } = State,
  [{Host, Port} | Rest] = BootstrapEndpoints0,
  error_logger:error_msg("client ~p metadata socket down ~s:~p~nReason:~p",
                        [ClientId, Host, Port, Reason]),
  BootstrapEndpoints = Rest ++ [{Host, Port}],
  {ok, State#state{bootstrap_endpoints = BootstrapEndpoints}}.

%% @private Maybe restart the metadata socket pid if it is no longer alive.
-spec maybe_restart_metadata_socket(state()) -> {ok, state()}.
maybe_restart_metadata_socket(#state{meta_sock_pid = MetaSockPid} = State) ->
  case brod_utils:is_pid_alive(MetaSockPid) of
    true ->
      {ok, State};
    false -> % can happen when metadata connection closed
      ClientId = State#state.client_id,
      Endpoints = State#state.bootstrap_endpoints,
      {ok, NewMetaSockPid, ReorderedEndpoints} =
        start_metadata_socket(ClientId, Endpoints, State#state.config),
      {ok, State#state{ bootstrap_endpoints = ReorderedEndpoints
                      , meta_sock_pid       = NewMetaSockPid
                      }}
  end.

%% @private
do_start_producer(TopicName, ProducerConfig, State) ->
  SupPid = State#state.producers_sup,
  F = fun() ->
        brod_producers_sup:start_producer(SupPid, self(),
                                          TopicName, ProducerConfig)
      end,
  ensure_partition_workers(TopicName, State, F).

%% @private
do_start_consumer(TopicName, ConsumerConfig, State) ->
  SupPid = State#state.consumers_sup,
  F = fun() ->
        brod_consumers_sup:start_consumer(SupPid, self(),
                                          TopicName, ConsumerConfig)
      end,
  ensure_partition_workers(TopicName, State, F).

%% @private
ensure_partition_workers(TopicName, State, F) ->
  with_ok(
    validate_topic_existence(TopicName, State, _IsRetry = false),
    fun(ok, NewState) ->
      case F() of
        {ok, _Pid} ->
          {ok, NewState};
        {error, {already_started, _Pid}} ->
          {ok, NewState};
        {error, Reason} ->
          {{error, Reason}, NewState}
      end
    end).

%% @private Catch noproc exit exception when making gen_server:call.
-spec safe_gen_call(pid() | atom(), Call, Timeout) -> Return
        when Call    :: term(),
             Timeout :: infinity | integer(),
             Return  :: ok | {ok, term()} | {error, client_down | term()}.
safe_gen_call(Server, Call, Timeout) ->
  try
    gen_server:call(Server, Call, Timeout)
  catch exit : {noproc, _} ->
    {error, client_down}
  end.

%% @private
-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
