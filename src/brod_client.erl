%%%   Copyright (c) 2015-2021 Klarna Bank AB (publ)
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

%% @doc A `brod_client' in brod is a `gen_server' responsible for establishing
%% and maintaining tcp sockets connecting to kafka brokers. It also manages
%% per-topic-partition producer and consumer processes under two-level
%% supervision trees.
%%
%% You can start clients automatically at application startup or on demand.
%% See the <a href="https://hexdocs.pm/brod/readme.html#clients">overview</a>
%% for examples.
-module(brod_client).
-behaviour(gen_server).

-export([ get_consumer/3
        , get_connection/3
        , get_group_coordinator/2
        , get_transactional_coordinator/2
        , get_leader_connection/3
        , get_bootstrap/1
        , get_metadata/2
        , get_metadata_safe/2
        , get_partitions_count/2
        , get_partitions_count_safe/2
        , get_producer/3
        , register_consumer/3
        , register_producer/3
        , deregister_consumer/3
        , deregister_producer/3
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

%% Test export
-export([ lookup_partitions_count_cache/2
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , handle_continue/2
        , init/1
        , terminate/2
        ]).

-export_type([config/0]).

-include("brod_int.hrl").

-define(DEFAULT_RECONNECT_COOL_DOWN_SECONDS, 1).
-define(DEFAULT_GET_METADATA_TIMEOUT_SECONDS, 5).
-define(DEFAULT_UNKNOWN_TOPIC_CACHE_TTL, 120000).

%% ClientId as ets table name.
-define(ETS(ClientId), ClientId).
-define(PRODUCER_KEY(Topic, Partition),
        {producer, Topic, Partition}).
-define(CONSUMER_KEY(Topic, Partition),
        {consumer, Topic, Partition}).
-define(TOPIC_METADATA_KEY(Topic),
        {topics, Topic}).
-define(PRODUCER(Topic, Partition, Pid),
        {?PRODUCER_KEY(Topic, Partition), Pid}).
-define(CONSUMER(Topic, Partition, Pid),
        {?CONSUMER_KEY(Topic, Partition), Pid}).

-type endpoint() :: brod:endpoint().
-type client() :: brod:client().
-type client_id() :: brod:client_id().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type config() :: proplists:proplist().
-type group_id() :: brod:group_id().
-type transactional_id() :: brod:transactional_id().

-type partition_worker_key() :: ?PRODUCER_KEY(topic(), partition())
                              | ?CONSUMER_KEY(topic(), partition()).

-type get_producer_error() :: client_down
                            | {client_down, any()}
                            | {producer_down, any()}
                            | {producer_not_found, topic()}
                            | {producer_not_found, topic(), partition()}.

-type get_consumer_error() :: client_down
                            | {client_down, any()}
                            | {consumer_down, any()}
                            | {consumer_not_found, topic()}
                            | {consumer_not_found, topic(), partition()}.

-type get_worker_error() :: get_producer_error()
                          | get_consumer_error().

-define(dead_since(TS, REASON), {dead_since, TS, REASON}).
-type connection() :: kpro:connection().
-type dead_conn() :: ?dead_since(erlang:timestamp(), any()).
-record(conn,
       { endpoint :: endpoint()
       , pid :: connection() | dead_conn()
       }).
-type conn_state() :: #conn{}.
-record(state,
        { client_id            :: client_id()
        , bootstrap_endpoints  :: [endpoint()]
        , meta_conn            :: ?undef | connection()
        , payload_conns = []   :: [conn_state()]
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
%%
%% The producer is started if auto_start_producers is
%% enabled in client config.
-spec get_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_producer_error()}.
get_producer(Client, Topic, Partition) ->
  case get_partition_worker(Client, ?PRODUCER_KEY(Topic, Partition)) of
    {ok, Pid} ->
      {ok, Pid};
    {error, {producer_not_found, Topic}} = Error ->
      maybe_start_producer(Client, Topic, Partition, Error);
    Error ->
      Error
  end.

%% @doc Get consumer of the given topic-partition.
-spec get_consumer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_consumer_error()}.
get_consumer(Client, Topic, Partition) ->
  get_partition_worker(Client, ?CONSUMER_KEY(Topic, Partition)).

%% @doc Dynamically start a per-topic producer.
%%
%% Return ok if the producer is already started.
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
%%
%% Returns ok if the consumer is already started.
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

%% @doc Get the connection to kafka broker which is a leader for given
%% Topic-Partition.
%%
%% Return already established connection towards the leader broker,
%% Otherwise a new one is established and cached in client state.
%% If the old connection was dead less than a configurable N seconds ago,
%% `{error, LastReason}' is returned.
-spec get_leader_connection(client(), topic(), partition()) ->
        {ok, pid()} | {error, any()}.
get_leader_connection(Client, Topic, Partition) ->
  safe_gen_call(Client, {get_leader_connection, Topic, Partition}, infinity).

-spec get_bootstrap(client()) -> {ok, brod:bootstrap()} | {error, any()}.
get_bootstrap(Client) ->
  safe_gen_call(Client, get_bootstrap, infinity).

%% @doc Get connection to a kafka broker.
%%
%% Return already established connection towards the broker,
%% otherwise a new one is established and cached in client state.
%% If the old connection was dead less than a configurable N seconds ago,
%% `{error, LastReason}' is returned.
-spec get_connection(client(), brod:hostname(), brod:portnum()) ->
        {ok, pid()} | {error, any()}.
get_connection(Client, Host, Port) ->
  safe_gen_call(Client, {get_connection, Host, Port}, infinity).

%% @doc Get topic metadata, if topic is `undefined', will fetch ALL metadata.
-spec get_metadata(client(), all | ?undef | topic()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Client, ?undef) ->
  get_metadata(Client, all);
get_metadata(Client, Topic) ->
  safe_gen_call(Client, {get_metadata, Topic}, infinity).

%% @doc Ensure not topic auto creation even if Kafka has it enabled.
-spec get_metadata_safe(client(), topic()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata_safe(Client, Topic) ->
  safe_gen_call(Client, {get_metadata, {_FetchMetadataForTopic = all, Topic}}, infinity).

%% @doc Get number of partitions for a given topic.
-spec get_partitions_count(client(), topic()) -> {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Topic) ->
  %% Set default allow_topic_auto_creation to true does not imply the topic is always
  %% auto-created. Kafka can have auto-creation disabled, in which case this option has no effect.
  get_partitions_count(Client, Topic, #{allow_topic_auto_creation => true}).

get_partitions_count(Client, Topic, Opts) when is_atom(Client) ->
  %% Ets =:= ClientId
  do_get_partitions_count(Client, Client, Topic, Opts);
get_partitions_count(Client, Topic, Opts) when is_pid(Client) ->
  case safe_gen_call(Client, get_workers_table, infinity) of
    {ok, Ets}       -> do_get_partitions_count(Client, Ets, Topic, Opts);
    {error, Reason} -> {error, Reason}
  end.

%% @doc Get number of partitions for an existing topic.
%% Ensured not to auto create a topic even when Kafka is configured
%% with topic auto creation enabled.
-spec get_partitions_count_safe(client(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count_safe(Client, Topic) ->
    get_partitions_count(Client, Topic, #{allow_topic_auto_creation => false}).

%% @doc Get broker endpoint and connection config for
%% connecting a group coordinator.
-spec get_group_coordinator(client(), group_id()) ->
        {ok, {endpoint(), brod:conn_config()}} | {error, any()}.
get_group_coordinator(Client, GroupId) ->
  safe_gen_call(Client, {get_group_coordinator, GroupId}, infinity).

%% @doc Get broker endpoint and connection config for
%% connecting a transactional coordinator.
-spec get_transactional_coordinator(client(), transactional_id()) ->
        {ok, {endpoint(), brod:conn_config()}} | {error, any()}.
get_transactional_coordinator(Client, TransactionId) ->
  safe_gen_call(Client, {get_transactional_coordinator, TransactionId}, infinity).


%% @doc Register self() as a partition producer.
%%
%% The pid is registered in an ETS table, then the callers
%% may lookup a producer pid from the table and make
%% produce requests to the producer process directly.
-spec register_producer(client(), topic(), partition()) -> ok.
register_producer(Client, Topic, Partition) ->
  Producer = self(),
  Key = ?PRODUCER_KEY(Topic, Partition),
  gen_server:cast(Client, {register, Key, Producer}).

%% @doc De-register the producer for a partition.
%%
%% The partition producer entry is deleted from
%% the ETS table to allow cleanup of purposefully
%% stopped producers and allow later restart.
-spec deregister_producer(client(), topic(), partition()) -> ok.
deregister_producer(Client, Topic, Partition) ->
  Key = ?PRODUCER_KEY(Topic, Partition),
  gen_server:cast(Client, {deregister, Key}).

%% @doc Register self() as a partition consumer.
%%
%% The pid is registered in an ETS table, then the callers may
%% lookup a consumer pid from the table and make subscribe
%% calls to the process directly.
-spec register_consumer(client(), topic(), partition()) -> ok.
register_consumer(Client, Topic, Partition) ->
  Consumer = self(),
  Key = ?CONSUMER_KEY(Topic, Partition),
  gen_server:cast(Client, {register, Key, Consumer}).

%% @doc De-register the consumer for a partition.
%%
%% The partition consumer entry is deleted from the ETS table
%% to allow cleanup of purposefully %% stopped consumers and
%% allow later restart.
-spec deregister_consumer(client(), topic(), partition()) -> ok.
deregister_consumer(Client, Topic, Partition) ->
  Key = ?CONSUMER_KEY(Topic, Partition),
  gen_server:cast(Client, {deregister, Key}).

%%%_* gen_server callbacks =====================================================

%% @private
init({BootstrapEndpoints, ClientId, Config}) ->
  erlang:process_flag(trap_exit, true),
  Tab = ets:new(?ETS(ClientId),
                [named_table, protected, {read_concurrency, true}]),
  {ok, #state{ client_id           = ClientId
             , bootstrap_endpoints = BootstrapEndpoints
             , config              = Config
             , workers_tab         = Tab
             }, {continue, init}}.

%% @private
handle_continue(init, State0) ->
  Endpoints = State0#state.bootstrap_endpoints,
  State1 = ensure_metadata_connection(State0),
  {ok, ProducersSupPid} = brod_producers_sup:start_link(),
  {ok, ConsumersSupPid} = brod_consumers_sup:start_link(),
  State = State1#state{ bootstrap_endpoints = Endpoints
                      , producers_sup       = ProducersSupPid
                      , consumers_sup       = ConsumersSupPid
                      },
  {noreply, State}.

%% @private
handle_info({'EXIT', Pid, Reason}, #state{ client_id     = ClientId
                                         , producers_sup = Pid
                                         } = State) ->
  ?BROD_LOG_ERROR("client ~p producers supervisor down~nReason: ~p",
                  [ClientId, Pid, Reason]),
  {stop, {producers_sup_down, Reason}, State};
handle_info({'EXIT', Pid, Reason}, #state{ client_id     = ClientId
                                         , consumers_sup = Pid
                                         } = State) ->
  ?BROD_LOG_ERROR("client ~p consumers supervisor down~nReason: ~p",
                  [ClientId, Pid, Reason]),
  {stop, {consumers_sup_down, Reason}, State};
handle_info({'EXIT', Pid, Reason}, State) ->
  NewState = handle_connection_down(State, Pid, Reason),
  {noreply, NewState};
handle_info(Info, State) ->
  ?BROD_LOG_WARNING("~p [~p] ~p got unexpected info: ~p",
                    [?MODULE, self(), State#state.client_id, Info]),
  {noreply, State}.

%% @private
handle_call({stop_producer, Topic}, _From, State) ->
  ok = brod_producers_sup:stop_producer(State#state.producers_sup, Topic),
  {reply, ok, State};
handle_call({stop_consumer, Topic}, _From, State) ->
  Reply = brod_consumers_sup:stop_consumer(State#state.consumers_sup, Topic),
  {reply, Reply, State};
handle_call({get_leader_connection, Topic, Partition}, _From, State) ->
  {Result, NewState} = do_get_leader_connection(State, Topic, Partition),
  {reply, Result, NewState};
handle_call(get_bootstrap, _From, State) ->
  #state{bootstrap_endpoints = Endpoints} = State,
  ConnConfig = conn_config(State),
  {reply, {ok, {Endpoints, ConnConfig}}, State};
handle_call({get_connection, Host, Port}, _From, State) ->
  {Result, NewState} = maybe_connect(State, {Host, Port}),
  {reply, Result, NewState};
handle_call({get_group_coordinator, GroupId}, _From, State) ->
  {Result, NewState} = do_get_group_coordinator(State, GroupId),
  {reply, Result, NewState};
handle_call({get_transactional_coordinator, TransactionId}, _From, State) ->
  {Result, NewState} = do_get_transactional_coordinator(State, TransactionId),
  {reply, Result, NewState};
handle_call({start_producer, TopicName, ProducerConfig}, _From, State) ->
  {Reply, NewState} = do_start_producer(TopicName, ProducerConfig, State),
  {reply, Reply, NewState};
handle_call({start_consumer, TopicName, ConsumerConfig}, _From, State) ->
  {Reply, NewState} = do_start_consumer(TopicName, ConsumerConfig, State),
  {reply, Reply, NewState};
handle_call({auto_start_producer, Topic}, _From, State) ->
  Config = State#state.config,
  case config(auto_start_producers, Config, false) of
    true ->
      ProducerConfig = config(default_producer_config, Config, []),
      {Reply, NewState} = do_start_producer(Topic, ProducerConfig, State),
      {reply, Reply, NewState};
    false ->
      {reply, {error, disabled}, State}
  end;
handle_call(get_workers_table, _From, State) ->
  {reply, {ok, State#state.workers_tab}, State};
handle_call(get_producers_sup_pid, _From, State) ->
  {reply, {ok, State#state.producers_sup}, State};
handle_call(get_consumers_sup_pid, _From, State) ->
  {reply, {ok, State#state.consumers_sup}, State};
handle_call({get_metadata, Topic}, _From, State) ->
  {Result, NewState} = do_get_metadata(Topic, State),
  {reply, Result, NewState};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

%% @private
handle_cast({register, Key, Pid}, #state{workers_tab = Tab} = State) ->
  ets:insert(Tab, {Key, Pid}),
  {noreply, State};
handle_cast({deregister, Key}, #state{workers_tab = Tab} = State) ->
  ets:delete(Tab, Key),
  {noreply, State};
handle_cast(Cast, State) ->
  ?BROD_LOG_WARNING("~p [~p] ~p got unexpected cast: ~p",
                    [?MODULE, self(), State#state.client_id, Cast]),
  {noreply, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% @private
terminate(Reason, #state{ client_id     = ClientId
                        , meta_conn     = MetaConn
                        , payload_conns = PayloadConns
                        , producers_sup = ProducersSup
                        , consumers_sup = ConsumersSup
                        }) ->
  case brod_utils:is_normal_reason(Reason) of
    true ->
      ok;
    false ->
      ?BROD_LOG_WARNING("~p [~p] ~p is terminating\nreason: ~p~n",
                        [?MODULE, self(), ClientId, Reason])
  end,
  %% stop producers and consumers first because they are monitoring connections
  shutdown_pid(ProducersSup),
  shutdown_pid(ConsumersSup),
  Shutdown = fun(#conn{pid = Pid}) -> shutdown_pid(Pid) end,
  lists:foreach(Shutdown, PayloadConns),
  shutdown_pid(MetaConn).

%%%_* Internal Functions =======================================================

shutdown_pid(Pid) when is_pid(Pid) ->
  exit(Pid, shutdown),
  ok;
shutdown_pid(_) ->
  ok.

-spec get_partition_worker(client(), partition_worker_key()) ->
        {ok, pid()} | {error, get_worker_error()}.
get_partition_worker(ClientPid, Key) when is_pid(ClientPid) ->
  case erlang:process_info(ClientPid, registered_name) of
    {registered_name, ClientId} -> get_partition_worker(ClientId, Key);

    _ ->
      %% This is a client process started without registered name
      %% have to call the process to get the producer/consumer worker
      %% process registration table.
      get_partition_worker_with_ets(ClientPid, Key)
  end;

get_partition_worker(ClientId, Key) when is_atom(ClientId) ->
  case lookup_partition_worker(ClientId, ?ETS(ClientId), Key) of
    {ok, Pid} ->
      %% This is an attempt to help with a race condition
      %% between ets lookups and what is being supervised.
      %% If the worker process is returned form ets,
      %% but it is not alive then there must be
      %% an in-flight worker deregistration request.
      case is_pid(Pid) andalso is_process_alive(Pid) of
        true -> {ok, Pid};
        false -> get_partition_worker_with_ets(ClientId, Key)
      end;
    Other -> Other
  end.

-spec get_partition_worker_with_ets(client(), partition_worker_key()) ->
  {ok, pid()} | {error, get_worker_error()}.
get_partition_worker_with_ets(Client, Key) ->
  case safe_gen_call(Client, get_workers_table, infinity) of
    {ok, Ets} -> lookup_partition_worker(Client, Ets, Key);
    {error, Reason} -> {error, Reason}
  end.

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

-spec validate_topic_existence(topic(), state(), boolean()) ->
        {Result, state()} when Result :: ok | {error, any()}.
validate_topic_existence(Topic, #state{workers_tab = Ets} = State, IsRetry) ->
  case lookup_partitions_count_cache(Ets, Topic) of
    {ok, _Count} ->
      {ok, State};
    {error, Reason} ->
      {{error, Reason}, State};
    false when IsRetry ->
      {{error, ?unknown_topic_or_partition}, State};
    false ->
      %% Try fetch metadata (and populate partition count cache)
      %% Then validate topic existence again.
      with_ok(do_get_metadata_safe(Topic, State),
              fun(_, S) -> validate_topic_existence(Topic, S, true) end)
  end.

%% Continue with {{ok, Result}, NewState}
%% return whatever error immediately.
-spec with_ok(Result, fun((_, state()) -> Result)) ->
        Result when Result :: {ok | {ok, term()} | {error, any()}, state()}.
with_ok({ok, State}, Continue) -> Continue(ok, State);
with_ok({{ok, OK}, State}, Continue) -> Continue(OK, State);
with_ok({{error, _}, #state{}} = Return, _Continue) -> Return.

%% If allow_topic_auto_creation is set 'false',
%% do not try to fetch metadata per topic name, fetch all topics instead.
%% As sending metadata request with topic name will cause an auto creation
%% of the topic if auto.create.topics.enable is enabled in broker config.
-spec do_get_metadata_safe(topic(), state()) -> {Result, state()}
        when Result :: {ok, kpro:struct()} | {error, any()}.
do_get_metadata_safe(Topic0, #state{config = Config} = State) ->
  Topic =
    case config(allow_topic_auto_creation, Config, true) of
      true  -> Topic0;
      false -> {all, Topic0}
    end,
  do_get_metadata(Topic, State).

-spec do_get_metadata(all | topic() | {all, topic()}, state()) -> {Result, state()}
        when Result :: {ok, kpro:struct()} | {error, any()}.
do_get_metadata({all, Topic}, State) ->
    do_get_metadata(all, Topic, State);
do_get_metadata(Topic, State) when not is_tuple(Topic) ->
    do_get_metadata(Topic, Topic, State).

do_get_metadata(FetchMetadataFor, Topic,
                #state{ client_id   = ClientId
                      , workers_tab = Ets
                      , config = Config
                      } = State0) ->
  Topics = case FetchMetadataFor of
             all -> all; %% in case no topic is given, get all
             _   -> [Topic]
           end,
  State = ensure_metadata_connection(State0),
  Conn = get_metadata_connection(State),
  Request = brod_kafka_request:metadata(Conn, Topics),
  case request_sync(State, Request) of
    {ok, #kpro_rsp{api = metadata, msg = Metadata}} ->
      TopicMetadataArray = kf(topics, Metadata),
      UnknownTopicCacheTtl = config(unknown_topic_cache_ttl, Config,
                                    ?DEFAULT_UNKNOWN_TOPIC_CACHE_TTL),
      ok = update_partitions_count_cache(Ets, TopicMetadataArray, UnknownTopicCacheTtl),
      ok = maybe_cache_unknown_topic_partition(Ets, Topic, TopicMetadataArray,
                                               UnknownTopicCacheTtl),
      {{ok, Metadata}, State};
    {error, Reason} ->
      ?BROD_LOG_ERROR("~p failed to fetch metadata for topics: ~p\n"
                      "reason=~p",
                      [ClientId, Topics, Reason]),
      {{error, Reason}, State}
  end.

%% Ensure there is at least one metadata connection
ensure_metadata_connection(#state{ bootstrap_endpoints = Endpoints
                                 , meta_conn = ?undef
                                 } = State) ->
  ConnConfig = conn_config(State),
  Pid = case kpro:connect_any(Endpoints, ConnConfig) of
          {ok, PidX} -> PidX;
          {error, Reason} -> erlang:exit(Reason)
        end,
  State#state{meta_conn = Pid};
ensure_metadata_connection(State) ->
  State.

%% must be called after ensure_metadata_connection
get_metadata_connection(#state{meta_conn = Conn}) -> Conn.

do_get_leader_connection(State0, Topic, Partition) ->
  State = ensure_metadata_connection(State0),
  MetaConn = get_metadata_connection(State),
  Timeout = timeout(State),
  case kpro:discover_partition_leader(MetaConn, Topic, Partition, Timeout) of
    {ok, Endpoint} -> maybe_connect(State, Endpoint);
    {error, Reason} -> {{error, Reason}, State}
  end.

-spec do_get_group_coordinator(state(), group_id()) ->
        {Result, state()} when Result :: {ok, connection()} | {error, any()}.
do_get_group_coordinator(State0, GroupId) ->
  State = ensure_metadata_connection(State0),
  MetaConn = get_metadata_connection(State),
  Timeout = timeout(State),
  case kpro:discover_coordinator(MetaConn, group, GroupId, Timeout) of
    {ok, Endpoint} ->
      {{ok, {Endpoint, conn_config(State)}}, State};
    {error, Reason} ->
      {{error, Reason}, State}
  end.

-spec do_get_transactional_coordinator(state(), transactional_id()) ->
        {Result, state()} when Result :: {ok, connection()} | {error, any()}.
do_get_transactional_coordinator(State0, TransactionId) ->
  State = ensure_metadata_connection(State0),
  MetaConn = get_metadata_connection(State),
  Timeout = timeout(State),
  case kpro:discover_coordinator(MetaConn, txn, TransactionId, Timeout) of
    {ok, Endpoint} ->
      {{ok, {Endpoint, conn_config(State)}}, State};
    {error, Reason} ->
      {{error, Reason}, State}
  end.

timeout(#state{config = Config}) ->
  timeout(Config);
timeout(Config) ->
  T = config(get_metadata_timeout_seconds, Config,
             ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  timer:seconds(T).

config(Key, Config, Default) ->
  proplists:get_value(Key, Config, Default).

conn_config(#state{client_id = ClientId, config = Config}) ->
  Cfg = conn_config(Config, kpro_connection:all_cfg_keys(), []),
  maps:from_list([{client_id, ensure_binary(ClientId)} | Cfg]).

conn_config([], _ConnCfgKeys, Acc) -> Acc;
conn_config([{K, V} | Rest], ConnCfgKeys, Acc) ->
  NewAcc =
    case lists:member(K, ConnCfgKeys) of
      true -> [{K, V} | Acc];
      false -> Acc
    end,
  conn_config(Rest, ConnCfgKeys, NewAcc);
conn_config([K | Rest], ConnCfgKeys, Acc) when is_atom(K) ->
  %% translate proplist boolean mark to tuple
  conn_config([{K, true} | Rest], ConnCfgKeys, Acc).

ensure_binary(ClientId) when is_atom(ClientId) ->
  ensure_binary(atom_to_binary(ClientId, utf8));
ensure_binary(ClientId) when is_binary(ClientId) ->
  ClientId.

-spec maybe_connect(state(), endpoint()) ->
        {Result, state()} when Result :: {ok, pid()} | {error, any()}.
maybe_connect(#state{} = State, Endpoint) ->
  case find_conn(Endpoint, State#state.payload_conns) of
    {ok, Pid} -> {{ok, Pid}, State};
    {error, Reason} -> maybe_connect(State, Endpoint, Reason)
  end.

-spec maybe_connect(state(), endpoint(), Reason) ->
        {Result, state()} when
          Reason :: not_found | dead_conn(),
          Result :: {ok, pid()} | {error, any()}.
maybe_connect(State, Endpoint, not_found) ->
  %% connect for the first time
  connect(State, Endpoint);
maybe_connect(#state{client_id = ClientId} = State,
              {Host, Port} = Endpoint, ?dead_since(Ts, Reason)) ->
  case is_cooled_down(Ts, State) of
    true ->
      connect(State, Endpoint);
    false ->
      ?BROD_LOG_ERROR("~p (re)connect to ~s:~p aborted.\n"
                      "last failure: ~p",
                      [ClientId, Host, Port, Reason]),
     {{error, Reason}, State}
  end.

-spec connect(state(), endpoint()) -> {Result, state()}
        when Result :: {ok, pid()} | {error, any()}.
connect(#state{ client_id = ClientId
              , payload_conns = Conns
              } = State, {Host, Port} = Endpoint) ->
  Conn =
    case do_connect(Endpoint, State) of
      {ok, Pid} ->
        ?BROD_LOG_INFO("client ~p connected to ~s:~p~n",
                       [ClientId, Host, Port]),
        #conn{ endpoint = Endpoint
             , pid = Pid
             };
      {error, Reason} ->
        ?BROD_LOG_ERROR("client ~p failed to connect to ~s:~p~n"
                        "reason:~p",
                        [ClientId, Host, Port, Reason]),
        #conn{ endpoint = Endpoint
             , pid = mark_dead(Reason)
             }
    end,
  NewConns = lists:keystore(Endpoint, #conn.endpoint, Conns, Conn),
  Result = case Conn#conn.pid of
             P when is_pid(P) -> {ok, P};
             ?dead_since(_, R) -> {error, R}
           end,
  {Result, State#state{payload_conns = NewConns}}.

do_connect(Endpoint, State) ->
  ConnConfig = conn_config(State),
  kpro:connect(Endpoint, ConnConfig).

%% Handle connection pid EXIT event, for payload sockets keep the timestamp,
%% but do not restart yet. Payload connection will be re-established when a
%% per-partition worker restarts and requests for a connection after
%% it is cooled down.
-spec handle_connection_down(state(), pid(), any()) -> state().
handle_connection_down(#state{meta_conn = Pid} = State, Pid, _Reason) ->
  State#state{meta_conn = ?undef};
handle_connection_down(#state{ payload_conns = Conns
                             , client_id     = ClientId
                             } = State, Pid, Reason) ->
  case lists:keytake(Pid, #conn.pid, Conns) of
    {value, #conn{endpoint = {Host, Port}} = Conn, Rest} ->
      ?BROD_LOG_INFO("client ~p: payload connection down ~s:~p~n"
                     "reason:~p", [ClientId, Host, Port, Reason]),
      NewConn = Conn#conn{pid = mark_dead(Reason)},
      State#state{payload_conns = [NewConn | Rest]};
    false ->
      %% stale EXIT message
      State
  end.

mark_dead(Reason) -> ?dead_since(os:timestamp(), Reason).

-spec find_conn(endpoint(), [conn_state()]) ->
        {ok, pid()} %% normal case
      | {error, not_found} %% first call
      | {error, dead_conn()}.
find_conn(Endpoint, Conns) ->
  case lists:keyfind(Endpoint, #conn.endpoint, Conns) of
    #conn{pid = Pid} when is_pid(Pid)        -> {ok, Pid};
    #conn{pid= ?dead_since(_, _) = NotAlive} -> {error, NotAlive};
    false                                    -> {error, not_found}
  end.

%% Check if the connection is down for long enough to retry.
is_cooled_down(Ts, #state{config = Config}) ->
  Threshold = config(reconnect_cool_down_seconds, Config,
                     ?DEFAULT_RECONNECT_COOL_DOWN_SECONDS),
  Now = os:timestamp(),
  Diff = timer:now_diff(Now, Ts) div 1000000,
  Diff >= Threshold.

%% call this function after fetched metadata for all topics
%% to cache the not-found status of a given topic
maybe_cache_unknown_topic_partition(Ets, Topic, TopicMetadataArray, UnknownTopicCacheTtl) ->
  case find_partition_count_in_topic_metadata_array(TopicMetadataArray, Topic) of
    {error, unknown_topic_or_partition} = Err ->
      _ = ets:insert(Ets, {?TOPIC_METADATA_KEY(Topic), Err,
                     {expire, expire_ts(UnknownTopicCacheTtl)}}),
      ok;
    _ ->
      %% do nothing when ok or any other error
      ok
  end.

-spec update_partitions_count_cache(ets:tab(), [kpro:struct()], non_neg_integer()) -> ok.
update_partitions_count_cache(_Ets, [], _UnknownTopicCacheTtl) -> ok;
update_partitions_count_cache(Ets, [TopicMetadata | Rest], UnknownTopicCacheTtl) ->
  Topic = kf(name, TopicMetadata),
  case get_partitions_count_in_metadata(TopicMetadata) of
    {ok, Cnt} ->
      ets:insert(Ets, {?TOPIC_METADATA_KEY(Topic), Cnt, {added, now_ts()}});
    {error, ?unknown_topic_or_partition} = Err ->
      ets:insert(Ets, {?TOPIC_METADATA_KEY(Topic), Err, {expire, expire_ts(UnknownTopicCacheTtl)}});
    {error, _Reason} ->
      ok
  end,
  update_partitions_count_cache(Ets, Rest, UnknownTopicCacheTtl).

%% Get partition counter from cache.
%% If cache is not hit, send meta data request to retrieve.
-spec do_get_partitions_count(client(), ets:tab(), topic(),
                              #{allow_topic_auto_creation := boolean()}) ->
        {ok, pos_integer()} | {error, any()}.
do_get_partitions_count(Client, Ets, Topic, #{allow_topic_auto_creation := AllowAutoCreation}) ->
  case lookup_partitions_count_cache(Ets, Topic) of
    {ok, Result} ->
      {ok, Result};
    {error, Reason} ->
      {error, Reason};
    false ->
      %% The topic's partition count is not cached yet,
      %% try to call get_metadata or get_metadata_safe to populate the cache
      MetadataResponse =
        case AllowAutoCreation of
          true ->
            get_metadata(Client, Topic);
          false ->
            get_metadata_safe(Client, Topic)
        end,
      %% the metadata is returned, no need to look up the cache again
      %% find the topic's partition count from the metadata directly
      find_partition_count_in_metadata(MetadataResponse, Topic)
  end.

find_partition_count_in_metadata({ok, Meta}, Topic) ->
  TopicMetadataArray = kf(topics, Meta),
  find_partition_count_in_topic_metadata_array(TopicMetadataArray, Topic);
find_partition_count_in_metadata({error, Reason}, _) ->
  {error, Reason}.

find_partition_count_in_topic_metadata_array(TopicMetadataArray, Topic) ->
  FilterF = fun(#{name := N}) when N =:= Topic -> true;
               (_) -> false
            end,
  case lists:filter(FilterF, TopicMetadataArray) of
    [TopicMetadata] ->
      get_partitions_count_in_metadata(TopicMetadata);
    [] ->
      {error, unknown_topic_or_partition}
  end.

-spec lookup_partitions_count_cache(ets:tab(), ?undef | topic()) ->
        {ok, pos_integer()} | {error, any()} | false.
lookup_partitions_count_cache(_Ets, ?undef) -> false;
lookup_partitions_count_cache(Ets, Topic) ->
  try ets:lookup(Ets, ?TOPIC_METADATA_KEY(Topic)) of
    [{_, Count, _Ts}] when is_integer(Count) ->
      {ok, Count};
    [{_, {error, Reason}, {expire, ExpireTs}}] when is_integer(ExpireTs) ->
      case is_expired(ExpireTs) of
        true  -> {error, Reason};
        false -> false
      end;
    [] ->
      false
  catch
    error : badarg ->
      {error, client_down}
  end.

-spec get_partitions_count_in_metadata(kpro:struct()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count_in_metadata(TopicMetadata) ->
  ErrorCode = kf(error_code, TopicMetadata),
  Partitions = kf(partitions, TopicMetadata),
  case ?IS_ERROR(ErrorCode) of
    true  -> {error, ErrorCode};
    false -> {ok, erlang:length(Partitions)}
  end.

-spec maybe_start_producer(client(), topic(),
                           partition(), {error, any()}) ->
        ok | {error, any()}.
maybe_start_producer(Client, Topic, Partition, Error) ->
  %% try to start a producer for the given topic if
  %% auto_start_producers option is enabled for the client
  case safe_gen_call(Client, {auto_start_producer, Topic}, infinity) of
    ok ->
      get_partition_worker(Client, ?PRODUCER_KEY(Topic, Partition));
    {error, disabled} ->
      Error;
    {error, Reason} ->
      {error, Reason}
  end.

-spec request_sync(state(), kpro:req()) ->
        {ok, kpro:rsp()} | {error, any()}.
request_sync(State, Request) ->
  Pid = get_metadata_connection(State),
  Timeout = timeout(State),
  kpro:request_sync(Pid, Request, Timeout).

do_start_producer(TopicName, ProducerConfig, State) ->
  SupPid = State#state.producers_sup,
  F = fun() ->
        brod_producers_sup:start_producer(SupPid, self(),
                                          TopicName, ProducerConfig)
      end,
  ensure_partition_workers(TopicName, State, F).

do_start_consumer(TopicName, ConsumerConfig, State) ->
  SupPid = State#state.consumers_sup,
  F = fun() ->
        brod_consumers_sup:start_consumer(SupPid, self(),
                                          TopicName, ConsumerConfig)
      end,
  ensure_partition_workers(TopicName, State, F).

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

%% Catches exit exceptions when making gen_server:call.
-spec safe_gen_call(pid() | atom(), Call, Timeout) -> Return
        when Call    :: term(),
             Timeout :: infinity | integer(),
             Return  :: ok | {ok, term()} | {error, Reason},
             Reason  :: client_down | {client_down, any()} | any().
safe_gen_call(Server, Call, Timeout) ->
  try
    gen_server:call(Server, Call, Timeout)
  catch
    exit : {noproc, _} ->
      {error, client_down};
    exit : {Reason, _} ->
      {error, {client_down, Reason}}
  end.

-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

-spec expire_ts(integer()) -> integer().
expire_ts(Ttl) -> now_ts() + Ttl.

-spec is_expired(integer()) -> boolean().
is_expired(ExpireTs) -> now_ts() - ExpireTs < 0.

-spec now_ts() -> integer().
now_ts() -> erlang:monotonic_time(millisecond).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
