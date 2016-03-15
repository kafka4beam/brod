%%%
%%%   Copyright (c) 2015-2016 Klarna AB
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
%%% @copyright 2015-2016 Klarna AB
%%% @end
%%%=============================================================================

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
        , start_link/2
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

-include("brod_int.hrl").

-define(DEFAULT_RECONNECT_COOL_DOWN_SECONDS, 1).
-define(DEFAULT_GET_METADATA_TIMEOUT_SECONDS, 5).

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

-type partition_worker_key() :: ?PRODUCER_KEY(topic(), partition())
                              | ?CONSUMER_KEY(topic(), partition()).

-type get_producer_error() :: client_down
                            | {producer_down, noproc}
                            | {producer_not_found, topic()}
                            | {producer_not_found, topic(), partition()}.

-type get_consumer_error() :: client_down
                            | {consumer_down, noproc}
                            | {consumer_not_found, topic()}
                            | {consumer_not_found, topic(), partition()}.

-type get_worker_error() :: get_producer_error()
                          | get_consumer_error().

-record(sock,
        { endpoint :: endpoint()
        , sock_pid :: pid() | dead_socket()
        }).

-record(state,
        { client_id            :: client_id()
        , bootstrap_endpoints  :: [endpoint()]
        , meta_sock_pid        :: pid()
        , payload_sockets = [] :: [#sock{}]
        , producers_sup        :: pid()
        , consumers_sup        :: pid()
        , config               :: client_config()
        , workers_tab          :: ets:tab()
        }).

%%%_* APIs =====================================================================

-spec start_link( [endpoint()]
                , client_config()) -> {ok, pid()} | {error, any()}.
start_link(BootstrapEndpoints, Config) ->
  Args = {BootstrapEndpoints, Config},
  gen_server:start_link(?MODULE, Args, []).

-spec start_link( [endpoint()]
                , client_id()
                , client_config()) -> {ok, pid()} | {error, any()}.
start_link(BootstrapEndpoints, ClientId, Config) when is_atom(ClientId) ->
  Args = {BootstrapEndpoints, ClientId, Config},
  gen_server:start_link({local, ClientId}, ?MODULE, Args, []).

-spec stop(client()) -> ok.
stop(Client) ->
  gen_server:call(Client, stop, infinity).

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
-spec start_producer(client(), topic(), producer_config()) ->
                        ok | {error, any()}.
start_producer(Client, TopicName, ProducerConfig) ->
  case get_producer(Client, TopicName, _Partition = 0) of
    {ok, _Pid} ->
      ok; %% already started
    {error, {producer_not_found, TopicName}} ->
      gen_server:call(Client, {start_producer, TopicName, ProducerConfig},
                      infinity);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Stop all partition producers of the given topic.
-spec stop_producer(client(), topic()) -> ok | {error, any()}.
stop_producer(Client, TopicName) ->
  gen_server:call(Client, {stop_producer, TopicName}, infinity).

%% @doc Dynamically start a topic consumer.
%% Returns ok if the consumer is already started.
%% @end.
-spec start_consumer(client(), topic(), consumer_config()) ->
                        ok | {error, any()}.
start_consumer(Client, TopicName, ConsumerConfig) ->
  case get_consumer(Client, TopicName, _Partition = 0) of
    {ok, _Pid} ->
      ok; %% already started
    {error, {consumer_not_found, TopicName}} ->
      gen_server:call(Client, {start_consumer, TopicName, ConsumerConfig},
                      infinity);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Stop all partition consumers of the given topic.
-spec stop_consumer(client(), topic()) -> ok | {error, any()}.
stop_consumer(Client, TopicName) ->
  gen_server:call(Client, {stop_consumer, TopicName}, infinity).

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
-spec get_metadata(client(), ?undef | topic()) -> {ok, kpro_MetadataResponse()}.
get_metadata(Client, Topic) ->
  gen_server:call(Client, {get_metadata, Topic}, infinity).

%% @doc Get number of partitions for a given topic
-spec get_partitions_count(client(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Topic) when is_atom(Client) ->
  %% Ets =:= ClientId
  get_partitions_count(Client, Client, Topic);
get_partitions_count(Client, Topic) when is_pid(Client) ->
  Ets = gen_server:call(Client, get_workers_table, infinity),
  get_partitions_count(Client, Ets, Topic).

%% @doc Get the endpoint of the group coordinator broker.
-spec get_group_coordinator(client(), group_id()) ->
        {ok, endpoint()} | {error, any()}.
get_group_coordinator(Client, GroupId) ->
  gen_server:call(Client, {get_group_coordinator, GroupId}, infinity).

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
  gen_server:call(Client, {get_connection, Host, Port}, infinity).

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
             }};
init({BootstrapEndpoints, Config}) ->
  erlang:process_flag(trap_exit, true),
  Tab = ets:new(workers_tab, [protected, {read_concurrency, true}]),
  self() ! init,
  {ok, #state{ client_id           = ?DEFAULT_CLIENT_ID
             , bootstrap_endpoints = BootstrapEndpoints
             , config              = Config
             , workers_tab         = Tab
             }}.

handle_info(init, State) ->
  ClientId = State#state.client_id,
  Endpoints = State#state.bootstrap_endpoints,
  {ok, MetaSock, ReorderedEndpoints} =
    start_metadata_socket(ClientId, Endpoints),
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
  error_logger:error_msg("client ~p metadata socket down ~s:~p~nReason:~p",
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
  Req = #kpro_GroupCoordinatorRequest{groupId = GroupId},
  {Rsp, NewState} = send_sync(State, Req, timer:seconds(Timeout)),
  Result =
    case Rsp of
      {ok, #kpro_GroupCoordinatorResponse{ errorCode       = EC
                                         , coordinatorHost = Host
                                         , coordinatorPort = Port
                                         }} ->
        case kpro_ErrorCode:is_error(EC) of
          true  -> {error, EC}; %% OBS: {error, EC} is used by group controller
          false -> {ok, {binary_to_list(Host), Port}}
        end;
      {error, Reason} ->
        {error, Reason}
    end,
  {reply, Result, NewState};
handle_call({start_producer, TopicName, ProducerConfig}, _From, State) ->
  {Reply, NewState} = do_start_producer(TopicName, ProducerConfig, State),
  {reply, Reply, NewState};
handle_call({start_consumer, TopicName, ConsumerConfig}, _From, State) ->
  {Result, NewState} = validate_topic_existence(TopicName, State),
  try
    Result =:= ok orelse throw(Result),
    SupPid = State#state.consumers_sup,
    case brod_consumers_sup:start_consumer(SupPid, self(),
                                           TopicName, ConsumerConfig) of
      {ok, _} ->
        {reply, ok, NewState};
      {error, {already_started, _Pid}} ->
        {reply, ok, NewState};
      Error ->
        throw(Error)
    end
  catch throw:E ->
      {reply, E, NewState}
  end;
handle_call({auto_start_producer, Topic}, _From, State) ->
  Config = State#state.config,
  case proplists:get_value(auto_start_producers, Config, false) of
    true ->
      ProducerConfig =
        proplists:get_value(default_producer_config, Config, []),
      {Reply, NewState} = do_start_producer(Topic, ProducerConfig, State),
      {reply, Reply, NewState};
    false ->
      {reply, false, State}
  end;
handle_call(get_workers_table, _From, State) ->
  {reply, State#state.workers_tab, State};
handle_call(get_producers_sup_pid, _From, State) ->
  {reply, State#state.producers_sup, State};
handle_call(get_consumers_sup_pid, _From, State) ->
  {reply, State#state.consumers_sup, State};
handle_call({get_metadata, Topic}, _From, State) ->
  {Result, NewState} = do_get_metadata(Topic, State),
  ok = maybe_update_metadata_cache(Result, NewState),
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
      Ets = gen_server:call(ClientPid, get_workers_table, infinity),
      lookup_partition_worker(ClientPid, Ets, Key)
  end;
get_partition_worker(ClientId, Key) when is_atom(ClientId) ->
  lookup_partition_worker(ClientId, ?ETS(ClientId), Key).

-spec lookup_partition_worker(client(), ets:tab(), partition_worker_key()) ->
        {ok, pid()} | { error, get_worker_error()}.
lookup_partition_worker(Client, Ets, Key) ->
  try
    case ets:lookup(Ets, Key) of
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
    end
  catch error:badarg ->
    %% ets table no exist (or closed)
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
  try
    SupPid = gen_server:call(Client, get_producers_sup_pid, infinity),
    brod_producers_sup:find_producer(SupPid, Topic, Partition)
  catch exit : {noproc, _} ->
    {error, client_down}
  end.

-spec find_consumer(client(), topic(), partition()) ->
        {ok, pid()} | {error, get_consumer_error()}.
find_consumer(Client, Topic, Partition) ->
  try
    SupPid = gen_server:call(Client, get_consumers_sup_pid, infinity),
    brod_consumers_sup:find_consumer(SupPid, Topic, Partition)
  catch exit : {noproc, _} ->
    {error, client_down}
  end.

-spec validate_topic_existence(topic(), #state{}) -> {Result, #state{}}
        when Result :: ok | {error, any()}.
validate_topic_existence(Topic0, #state{config = Config} = State) ->
  %% if allow_topic_auto_creation is set 'false', do not try to fetch
  %% metadata per topic name, fetch all topics instead. As sending
  %% metadata request with topic name will cause an auto creation of
  %% the topic in broker if auto.create.topics.enable is set to true
  %% in broker config.
  Topic =
    case proplists:get_value(allow_topic_auto_creation, Config, true) of
      true  -> Topic0;
      false -> ?undef
    end,
  case do_get_metadata(Topic, State) of
    {{ok, Response}, NewState} ->
      #kpro_MetadataResponse{topicMetadata_L = Topics} = Response,
      case lists:keyfind(Topic0, #kpro_TopicMetadata.topicName, Topics) of
        #kpro_TopicMetadata{} -> {ok, NewState};
        false                 -> {{error, topic_not_found}, NewState}
      end;
    {{error, Reason}, NewState} ->
      {{error, Reason}, NewState}
  end.

-spec do_get_metadata(?undef | topic(), #state{}) -> {Result, #state{}}
        when Result :: {ok, kpro_MetadataResponse()} | {error, any()}.
do_get_metadata(Topic, #state{config = Config} = State) ->
  Topics = case Topic of
             ?undef -> []; %% in case no topic is given, get all
             _      -> [Topic]
           end,
  Request = #kpro_MetadataRequest{topicName_L = Topics},
  Timeout = proplists:get_value(get_metadata_timout_seconds, Config,
                                ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  send_sync(State, Request, timer:seconds(Timeout)).

-spec do_get_connection(#state{}, hostname(), portnum()) ->
        {#state{}, Result} when Result :: {ok, pid()} | {error, any()}.
do_get_connection(#state{} = State, Host, Port) ->
  case find_socket(State, Host, Port) of
    {ok, Pid} ->
      {State, {ok, Pid}};
    {error, Reason} ->
      maybe_connect(State, Host, Port, Reason)
  end.


-spec maybe_connect(#state{}, hostname(), portnum(), Reason) ->
        {#state{}, Result} when
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

-spec connect(#state{}, hostname(), portnum()) -> {#state{}, Result}
        when Result :: {ok, pid()} | {error, any()}.
connect(#state{ client_id       = ClientId
              , payload_sockets = Sockets
              } = State, Host, Port) ->
  Endpoint = {Host, Port},
  case brod_sock:start_link(self(), Host, Port, ClientId, []) of
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
-spec handle_socket_down(#state{}, pid(), any()) -> {ok, #state{}}.
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
      %% is_process_alive is checked and reconnect is done for metadata
      %% socket in maybe_restart_metadata_socket, hence the 'EXIT' message
      %% of old metadata socket pid may end up in this clause, simply ignore
      {ok, State}
  end.

-spec mark_socket_dead(#sock{}, any()) -> {ok, #sock{}}.
mark_socket_dead(Socket, Reason) ->
  {ok, Socket#sock{sock_pid = ?dead_since(os:timestamp(), Reason)}}.

-spec find_socket(#state{}, hostname(), portnum()) ->
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
%%       at same time, if we use brod_util:get_metadata/2 to fetch metadata.
%% NOTE: crash in case failed to connect to all of the endpoints.
%%       should be restarted by supervisor.
%% @end
-spec start_metadata_socket(client_id(), [endpoint()]) ->
                               {ok, pid(),  [endpoint()]} | no_return().
start_metadata_socket(ClientId, [_|_] = Endpoints) ->
  start_metadata_socket(ClientId, Endpoints,
                        _FailedEndpoints = [], _Reason = ?undef).

start_metadata_socket(_ClientId, [], _FailedEndpoints, Reason) ->
  erlang:error(Reason);
start_metadata_socket(ClientId, [Endpoint | Rest] = Endpoints,
                      FailedEndpoints, _Reason) ->
  {Host, Port} = Endpoint,
  case brod_sock:start_link(self(), Host, Port, ClientId, []) of
    {ok, Pid} ->
      ReorderedEndpoints = Endpoints ++ lists:reverse(FailedEndpoints),
      {ok, Pid, ReorderedEndpoints};
    {error, Reason} ->
      start_metadata_socket(ClientId, Rest,
                            [Endpoint | FailedEndpoints], Reason)
  end.

maybe_update_metadata_cache({ok, #kpro_MetadataResponse{} = R}, State) ->
  [TopicMetadata] = R#kpro_MetadataResponse.topicMetadata_L,
  case do_get_partitions_count(TopicMetadata) of
    {ok, PartitionsCnt} ->
      Tab = State#state.workers_tab,
      Topic = TopicMetadata#kpro_TopicMetadata.topicName,
      ets:insert(Tab, {?TOPIC_METADATA_KEY(Topic),
                       PartitionsCnt, TopicMetadata}),
      ok;
    {error, _} ->
      ok
  end;
maybe_update_metadata_cache({error, _}, _State) ->
  ok.

-spec get_partitions_count(client(), ets:tab(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Ets, Topic) ->
  case ets:lookup(Ets, ?TOPIC_METADATA_KEY(Topic)) of
    [{_, PartitionsCnt, _}] ->
      {ok, PartitionsCnt};
    [] ->
      case get_metadata(Client, Topic) of
        {ok, #kpro_MetadataResponse{topicMetadata_L = TopicsMetadata}} ->
          [TopicMetadata] = TopicsMetadata,
          do_get_partitions_count(TopicMetadata);
        {error, Reason} ->
          {error, Reason}
      end
  end.

-spec do_get_partitions_count(kpro_TopicMetadata()) ->
        {ok, pos_integer()} | {error, any()}.
do_get_partitions_count(TopicMetadata) ->
  #kpro_TopicMetadata{ errorCode           = TopicEC
                     , partitionMetadata_L = Partitions
                     } = TopicMetadata,
  case kpro_ErrorCode:is_error(TopicEC) of
    true  -> {error, TopicEC};
    false -> {ok, erlang:length(Partitions)}
  end.

maybe_start_producer(Client, Topic, Partition, Error) ->
  case gen_server:call(Client, {auto_start_producer, Topic}) of
    ok ->
      get_partition_worker(Client, ?PRODUCER_KEY(Topic, Partition));
    {error, topic_not_found} = Error ->
      Error;
    false ->
      Error
  end.

-spec send_sync(#state{}, kpro_Request(), timer:time()) ->
        {Result, #state{}} when Result :: {ok, kpro_Response()}
                                        | {error, any()}.
send_sync(State, Request, Timeout) ->
  send_sync(State, Request, Timeout, 0).

send_sync(State, Request, Timeout, RetryCnt) ->
  RetryCnt >= length(State#state.bootstrap_endpoints) andalso
    erlang:error(no_reachable_endpoint),
  {ok, NewState} = maybe_restart_metadata_socket(State),
  SockPid = NewState#state.meta_sock_pid,
  case brod_sock:send_sync(SockPid, Request, Timeout) of
    {error, tcp_closed} ->
      send_sync(NewState, Request, Timeout, RetryCnt+1);
    Result ->
      {Result, State}
  end.

%% @private Maybe restart the metadata socket pid if it is no longer alive.
-spec maybe_restart_metadata_socket(#state{}) -> {ok, #state{}} | no_return().
maybe_restart_metadata_socket(#state{meta_sock_pid = MetaSockPid} = State) ->
  case is_pid(MetaSockPid) andalso is_process_alive(MetaSockPid) of
    true ->
      {ok, State};
    false -> % can happen when metadata connection closed
      ClientId = State#state.client_id,
      Endpoints = State#state.bootstrap_endpoints,
      {ok, NewMetaSockPid, ReorderedEndpoints} =
        start_metadata_socket(ClientId, Endpoints),
      {ok, State#state{ bootstrap_endpoints = ReorderedEndpoints
                      , meta_sock_pid       = NewMetaSockPid
                      }}
  end.

do_start_producer(TopicName, ProducerConfig, State) ->
  {Result, NewState} = validate_topic_existence(TopicName, State),
  try
    Result =:= ok orelse throw(Result),
    SupPid = State#state.producers_sup,
    case brod_producers_sup:start_producer(SupPid, self(),
                                           TopicName, ProducerConfig) of
      {ok, _} ->
        {ok, NewState};
      {error, {already_started, _Pid}} ->
        {ok, NewState};
      Error ->
        throw(Error)
    end
  catch throw:E ->
      {E, NewState}
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
