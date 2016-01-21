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

-module(brod_client).
-behaviour(gen_server).

-export([ find_producer/3
        , find_consumer/3
        , get_connection/3
        , get_leader_connection/3
        , get_metadata/2
        , get_partitions/2
        , get_producer/3
        , register_producer/3
        , start_link/4
        , start_link/5
        , stop/1
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
-define(PRODUCER(Topic, Partition, Pid),
        {?PRODUCER_KEY(Topic, Partition), Pid}).

-record(sock,
        { endpoint :: endpoint()
        , sock_pid :: pid() | dead_socket()
        }).

-record(state,
        { client_id     :: client_id()
        , endpoints     :: [endpoint()]
        , meta_sock     :: pid()
        , sockets = []  :: [#sock{}]
        , producers_sup :: pid() | undefined
        , consumers_sup :: pid() | undefined
        , config        :: client_config()
        , producers_tab :: ets:tab()
        }).

%%%_* APIs =====================================================================

-spec start_link( client_id()
                , [endpoint()]
                , [{topic(), producer_config()}]
                , [{topic(), consumer_config()}]
                , client_config()) -> {ok, pid()} | {error, any()}.
start_link(ClientId, Endpoints, Producers, Consumers, Config)
  when is_atom(ClientId) ->
  Args = {ClientId, Endpoints, Producers, Consumers, Config},
  gen_server:start_link({local, ClientId}, ?MODULE, Args, []).

-spec start_link( [endpoint()]
                , [{topic(), producer_config()}]
                , [{topic(), consumer_config()}]
                , client_config()) -> {ok, pid()} | {error, any()}.
start_link(Endpoints, Producers, Consumers, Config) ->
  Args = {Endpoints, Producers, Consumers, Config},
  gen_server:start_link(?MODULE, Args, []).

-spec stop(client()) -> ok.
stop(Client) ->
  gen_server:call(Client, stop, infinity).

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
  case brod_client:get_metadata(Client, Topic) of
    {ok, Metadata} ->
      #metadata_response{ brokers = Brokers
                        , topics  = [TopicMetadata]
                        } = Metadata,
      #topic_metadata{ error_code = TopicErrorCode
                     , partitions = Partitions
                     } = TopicMetadata,
      brod_kafka:is_error(TopicErrorCode) andalso erlang:throw(TopicErrorCode),
      #partition_metadata{ error_code = PartitionEC
                         , leader_id  = LeaderId} =
        lists:keyfind(Partition, #partition_metadata.id, Partitions),
      brod_kafka:is_error(PartitionEC) andalso erlang:throw(PartitionEC),
      LeaderId >= 0 orelse erlang:throw({no_leader, {Client, Topic, Partition}}),
      #broker_metadata{host = Host, port = Port} =
        lists:keyfind(LeaderId, #broker_metadata.node_id, Brokers),

      get_connection(Client, Host, Port);
    {error, Reason} ->
      {error, Reason}
  end.

-spec get_metadata(client(), topic()) -> {ok, #metadata_response{}}.
get_metadata(Client, Topic) ->
  gen_server:call(Client, {get_metadata, Topic}, infinity).

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

%% @doc Get all partition numbers of a given topic.
-spec get_partitions(client(), topic()) -> {ok, [partition()]} | {error, any()}.
get_partitions(Client, Topic) ->
  case get_metadata(Client, Topic) of
    {ok, #metadata_response{topics = TopicsMetadata}} ->
      [TopicMetadata] = TopicsMetadata,
      try
        {ok, do_get_partitions(TopicMetadata)}
      catch
        throw:Reason ->
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Register self() as a partition producer the pid is registered in an ETS
%% table, then the callers may lookup a producer pid from the table and make
%% produce requests to the producer process directly.
%% @end
-spec register_producer(client(), topic(), partition()) -> ok.
register_producer(Client, Topic, Partition) ->
  Producer = self(),
  gen_server:cast(Client, {register_producer, Topic, Partition, Producer}).

%% @doc Get producer of a given topic-partition.
-spec get_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, Reason}
          when Reason :: client_down
                       | {producer_down, noproc}
                       | {producer_not_found, topic()}
                       | {producer_not_found, topic(), partition()}.
get_producer(ClientPid, Topic, Partition) when is_pid(ClientPid) ->
  case erlang:process_info(ClientPid, registered_name) of
    {registered_name, ClientId} ->
      get_producer(ClientId, Topic, Partition);
    Err when Err =:= [] orelse Err =:= ?undef ->
      {error, client_down}
  end;
get_producer(ClientId, Topic, Partition) when is_atom(ClientId) ->
  try
    case ets:lookup(?ETS(ClientId), ?PRODUCER_KEY(Topic, Partition)) of
      [] ->
        %% not yet registered, 2 possible reasons:
        %% 1. caller is too fast, producers are starting up
        %%    make a synced call all the way down the supervision tree
        %%    to the partition producer sup should resolve the race
        %% 2. bad argument, no such worker started, supervisors should know
        find_producer(ClientId, Topic, Partition);
      [?PRODUCER(Topic, Partition, Pid)] ->
        {ok, Pid}
    end
  catch error:badarg ->
    {error, client_down}
  end.

-spec find_producer(client(), topic(), partition()) ->
                       {ok, pid()} | {error, Reason} when
        Reason :: {producer_not_found, topic()}
                | {producer_not_found, topic(), partition()}
                | {producer_down, noproc}
                | client_down.
find_producer(Client, Topic, Partition) ->
  try
    SupPid = gen_server:call(Client, get_producers_sup_pid, infinity),
    brod_producers_sup:find_producer(SupPid, Topic, Partition)
  catch exit : {noproc, _} ->
    {error, client_down}
  end.

find_consumer(Client, Topic, Partition) ->
  try
    SupPid = gen_server:call(Client, get_consumers_sup_pid, infinity),
    brod_consumers_sup:find_consumer(SupPid, Topic, Partition)
  catch exit : {noproc, _} ->
    {error, client_down}
  end.

%%%_* gen_server callbacks =====================================================

init({ClientId, Endpoints, Producers, Consumers, Config}) ->
  erlang:process_flag(trap_exit, true),
  Tab = ets:new(?ETS(ClientId),
                [named_table, protected, {read_concurrency, true}]),
  self() ! {init, Producers, Consumers},
  {ok, #state{ client_id     = ClientId
             , endpoints     = Endpoints
             , config        = Config
             , producers_tab = Tab
             }};
init({Endpoints, Producers, Consumers, Config}) ->
  erlang:process_flag(trap_exit, true),
  Tab = ets:new(producers_tab, [protected, {read_concurrency, true}]),
  self() ! {init, Producers, Consumers},
  {ok, #state{ client_id     = ?DEFAULT_CLIENT_ID
             , endpoints     = Endpoints
             , config        = Config
             , producers_tab = Tab
             }}.

handle_info({init, Producers, Consumers}, State) ->
  ClientId = State#state.client_id,
  Endpoints = State#state.endpoints,
  Sock = start_metadata_socket(ClientId, Endpoints),
  {ok, ProducersSupPid} = maybe_start_producers_sup(Producers),
  {ok, ConsumersSupPid} = maybe_start_consumers_sup(Consumers),
  {noreply, State#state{ meta_sock     = Sock
                       , producers_sup = ProducersSupPid
                       , consumers_sup = ConsumersSupPid
                       }};

handle_info({'EXIT', Pid, Reason}, #state{ client_id     = ClientId
                                         , producers_sup = Pid
                                         } = State) ->
  error_logger:error_msg("client ~p producers supervisor down~nReason: ~p",
                         [ClientId, Pid, Reason]),
  %% shutdown all producers?
  {noreply, State#state{producers_sup = ?undef}};
handle_info({'EXIT', Pid, Reason}, #state{ client_id     = ClientId
                                         , consumers_sup = Pid
                                         } = State) ->
  error_logger:error_msg("client ~p consumers supervisor down~nReason: ~p",
                         [ClientId, Pid, Reason]),
  {noreply, State#state{consumers_sup = ?undef}};
handle_info({'EXIT', Pid, Reason},
            #state{ client_id = ClientId
                  , meta_sock = #sock{ sock_pid = Pid
                                     , endpoint = {Host, Port}
                                     }
                  , endpoints = Endpoints
                  } = State) ->
  error_logger:info_msg("client ~p metadata socket down ~s:~p~nReason:~p",
                        [ClientId, Host, Port, Reason]),
  NewSock = start_metadata_socket(ClientId, Endpoints),
  {noreply, State#state{meta_sock = NewSock}};
handle_info({'EXIT', Pid, Reason}, State) ->
  {ok, NewState} = handle_socket_down(State, Pid, Reason),
  {noreply, NewState};
handle_info(Info, State) ->
  error_logger:warning_msg("~p [~p] ~p got unexpected info: ~p",
                          [?MODULE, self(), State#state.client_id, Info]),
  {noreply, State}.

handle_call(get_producers_sup_pid, _From, State) ->
  {reply, State#state.producers_sup, State};
handle_call(get_consumers_sup_pid, _From, State) ->
  {reply, State#state.consumers_sup, State};
handle_call({get_metadata, Topic}, _From, State) ->
  Result = do_get_metadata(Topic, State),
  {reply, Result, State};
handle_call({get_connection, Host, Port}, _From, State) ->
  {NewState, Result} = do_get_connection(State, Host, Port),
  {reply, Result, NewState};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({register_producer, Topic, Partition, ProducerPid},
            #state{producers_tab = Tab} = State) ->
  ets:insert(Tab, ?PRODUCER(Topic, Partition, ProducerPid)),
  {noreply, State};
handle_cast(Cast, State) ->
  error_logger:warning_msg("~p [~p] ~p got unexpected cast: ~p",
                          [?MODULE, self(), State#state.client_id, Cast]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(Reason, #state{ client_id     = ClientId
                        , meta_sock     = MetaSock
                        , sockets       = Sockets
                        , producers_sup = ProducersSup
                        , consumers_sup = ConsumersSup
                        }) ->
  case brod_utils:is_normal_reason(Reason) of
    true ->
      ok;
    false ->
      error_logger:warning_msg("~p [~p] ~p is terminating, reason: ~p~n",
                               [?MODULE, self(), ClientId, Reason])
  end,
  %% stop producers and consumers first because they are monitoring socket pids
  brod_utils:shutdown_pid(ProducersSup),
  brod_utils:shutdown_pid(ConsumersSup),
  lists:foreach(
    fun(?undef) -> ok;
       (#sock{sock_pid = Pid}) -> brod_utils:shutdown_pid(Pid)
    end, [MetaSock | Sockets]).

%%%_* Internal Functions =======================================================

-spec do_get_partitions(#topic_metadata{}) -> [partition()].
do_get_partitions(#topic_metadata{ error_code = TopicErrorCode
                                 , partitions = Partitions}) ->
  brod_kafka:is_error(TopicErrorCode) andalso
    erlang:throw(TopicErrorCode),
  lists:map(
    fun(#partition_metadata{ error_code = PartitionErrorCode
                           , id         = Partition
                           }) ->
      brod_kafka:is_error(PartitionErrorCode) andalso
        erlang:throw(PartitionErrorCode),
      Partition
    end, Partitions).

-spec do_get_metadata(topic(), #state{}) ->
        {ok, #metadata_response{}} | {error, any()}.
do_get_metadata(Topic, #state{ meta_sock = #sock{sock_pid = SockPid}
                             , config    = Config
                             }) ->
  Request = #metadata_request{topics = [Topic]},
  Timeout = proplists:get_value(get_metadata_timout_seconds, Config,
                                ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  brod_sock:send_sync(SockPid, Request, timer:seconds(Timeout)).

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
      error_logger:error_msg("~p (re)connect to ~p:~p~n aborted, "
                             "last failure reason:~p",
                             [ClientId, Host, Port, Reason]),
     {State, {error, Reason}}
  end.

-spec connect(#state{}, hostname(), portnum()) -> {#state{}, Result}
        when Result :: {ok, pid()} | {error, any()}.
connect(#state{ client_id = ClientId
              , sockets   = Sockets
              } = State, Host, Port) ->
  case brod_sock:start_link(self(), Host, Port, ClientId, []) of
    {ok, Pid} ->
      S = #sock{ endpoint = {Host, Port}
               , sock_pid = Pid
               },
      error_logger:info_msg("client ~p connected to ~s:~p~n",
                            [ClientId, Host, Port]),
      NewSockets = lists:keystore({Host, Port}, #sock.endpoint, Sockets, S),
      {State#state{sockets = NewSockets}, {ok, Pid}};
    {error, Reason} ->
      error_logger:error_msg("client ~p failed to connect to ~s:~p~n"
                             "reason:~p",
                             [ClientId, Host, Port, Reason]),
      {ok, NewState} = mark_socket_dead(State, {Host, Port}, Reason),
      {NewState, {error, Reason}}
  end.

%% @private Handle socket pid EXIT event, keep the timestamp.
%% But do not restart yet. Connection will be re-established when a
%% per-partition producer restarts and requests for a connection after
%% it is cooled down.
%% @end
-spec handle_socket_down(#state{}, pid(), any()) -> {ok, #state{}}.
handle_socket_down(#state{ client_id = ClientId
                         , sockets   = Sockets
                         } = State, Pid, Reason) ->
  case lists:keyfind(Pid, #sock.sock_pid, Sockets) of
    #sock{endpoint = {Host, Port} = Endpoint}  ->
      error_logger:info_msg("client ~p: payload socket down ~s:~p~n"
                            "reason:~p",
                            [ClientId, Host, Port, Reason]),
      mark_socket_dead(State, Endpoint, Reason)
  end.

-spec mark_socket_dead(#state{}, endpoint(), any()) -> {ok, #state{}}.
mark_socket_dead(#state{sockets = Sockets} = State, Endpoint, Reason) ->
  Conn = #sock{ endpoint = Endpoint
              , sock_pid = ?dead_since(os:timestamp(), Reason)
              },
  NewSockets = lists:keystore(Endpoint, #sock.endpoint, Sockets, Conn),
  {ok, State#state{sockets = NewSockets}}.

-spec find_socket(#state{}, hostname(), portnum()) ->
        {ok, pid()} %% normal case
      | {error, not_found} %% first call
      | {error, dead_socket()}.
find_socket(#state{sockets = Sockets}, Host, Port) ->
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

%% @private Establish a dedicated socket to kafka cluster endpoint(s) for
%%          metadata retrievals.
%% NOTE: This socket is not intended for kafka payload. This is to avoid
%%       burst of connection usage when many partition producers (re)start
%%       at same time, if we use brod_util:get_metadata/2 to fetch metadata.
%% NOTE: crash in case failed to connect to all of the endpoints.
%%       should be restarted by supervisor.
%% @end
-spec start_metadata_socket(client_id(), [endpoint()]) -> pid() | no_return().
start_metadata_socket(ClientId, [_|_] = Endpoints) ->
  start_metadata_socket(ClientId, Endpoints, ?undef).

start_metadata_socket(_ClientId, [], Reason) ->
  erlang:error(Reason);
start_metadata_socket(ClientId, [Endpoint | Endpoints], _Reason) ->
  {Host, Port} = Endpoint,
  case brod_sock:start_link(self(), Host, Port, ClientId, []) of
    {ok, Pid}       -> #sock{ endpoint = Endpoint
                            , sock_pid = Pid
                            };
    {error, Reason} -> start_metadata_socket(ClientId, Endpoints, Reason)
  end.

maybe_start_producers_sup([]) ->
  {ok, undefined};
maybe_start_producers_sup(Producers) ->
  brod_producers_sup:start_link(self(), Producers).

maybe_start_consumers_sup([]) ->
  {ok, undefined};
maybe_start_consumers_sup(Consumers) ->
  brod_consumers_sup:start_link(self(), Consumers).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
