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
%%% @doc brod supervisor
%%% Hiarchy:
%%%   clients-supervisor (one_for_one, registered name: brod_sup)
%%%     |
%%%     +--client-1-worker
%%%     |    |
%%%     |    +-- client-1-topics-sup (one_for_one)
%%%     |          |
%%%     |          +-- client-1-topic-1-worker
%%%     |          |     |
%%%     |          |     +-- client-1-topic-1-partitions-sup (one_for_one)
%%%     |          |           |
%%%     |          |           +-- client-1-topic-1-partition-1-worker
%%%     |          |           |
%%%     |          |           +-- client-1-topic-1-partition-2-worker
%%%     |          |           |...
%%%     |          |
%%%     |          +-- client-1-topic-2-worker
%%%     |          |     |...
%%%     |          |...
%%%     |
%%%     +-- client-2-worker
%%%           |...
%%%
%%% @copyright 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_sup).
-behaviour(brod_supervisor).

-export([ init/1
        , start_link/0
        , start_link_topics_sup/1
        , start_link_partitions_sup/5
        ]).

-include("brod_int.hrl").

%% By deafult, restart client process after a 10-seconds delay
-define(DEFAULT_CLIENT_RESTART_DELAY, 10).

%% By default, restart producer proess after a 10-seconds delay
-define(DEFAULT_PRODUCER_RESTART_DELAY, 11).

%% By default, restart partition producer process after a 2-seconds delay
-define(DEFAULT_PARTITION_RESTART_DELAY, 2).

%%%_* APIs ---------------------------------------------------------------------

%% @doc Start root supervisor.
%%
%% To start permanent clients, a minimal example of app env (sys.config):
%%    [
%%    %% Permanent clients
%%      { clients
%%      , [ {client_1 %% unique client ID
%%          , [ {endpoints, [{"localhost", 9092}]}
%%            , {restart_delay_seconds, 10}
%%              %% @see brod:start_link_client/2 for more client configs
%%            ]
%%          }
%%        , {client_2, ...}
%%        ]
%%      }
%%    %% permanent per-topic producers
%%    , { producers
%%      , [ { client_1         %% client ID
%%          , <<"test-topic">> %% topic name
%%          , [ {restart_delay_seconds, 10}
%%            , {partition_worker_restart_delay_seconds, 2}
%%            , {required_acks, -1}
%%            , {partitionner, roundrobin}
%%              %% @see brod:start_link_producer/3 for more producer configs
%%            ]
%%          }
%%        ]
%%      }
%%    ].
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, clients_sup).

%% @doc Start a supervisor to manage all topic-producers of
%% a specific client
%% @end
-spec start_link_topics_sup(client_id()) -> {ok, pid()}.
start_link_topics_sup(ClientId) ->
  brod_supervisor:start_link(?MODULE, {producers_sup, ClientId}).

%% @doc Start a supervisor to manage all partition-producers of
%% a specific topic
%% @end
-spec start_link_partitions_sup(client(), topic(), [partition()],
                                producer_config(), pid()) ->
        {ok, pid()}.
start_link_partitions_sup(Client, Topic, Partitions, Config, TopicProducer) ->
  Arg = {partitions_sup, Client, Topic, Partitions, Config, TopicProducer},
  brod_supervisor:start_link(?MODULE, Arg).

%% @doc brod_supervisor callback.
init(clients_sup) ->
  Clients = application:get_env(brod, clients, []),
  ClientSpecs =
    [client_spec(ClientId, Config) || {ClientId, Config} <- Clients],
  %% A client may crash and restart due to network failure
  %% e.g. when none of the kafka endpoints are reachable.
  %% In this case, restart it right away will very likely fail again,
  %% hence we set MaxR=0 here to stop it from restarting immediately
  %% and it should be restarted after a configurable N-secons delay
  {ok, {one_for_one, 0, 1}, ClientSpecs};
init({producers_sup, ClientId}) ->
  Producers = application:get_env(brod, producers, []),
  ProducerSpecs =
    lists:filtermap(
      fun({ClientId0, TopicName, Config}) ->
        ClientId0 =:= ClientId andalso
          {true, producer_spec(ClientId, TopicName, Config)}
      end, Producers),
  %% producers may crash in case of exception (error code received)
  %% when fetching topic metata from kafka
  %% In this case, restart it right away will very likely fail again
  %% as kakfa errors may last at least seconds.
  %% hence we set MaxR=0 here to stop it from restarting immediately
  %% and it should be restarted after a configurable N-seconds delay
  {ok, {{one_for_one, 0, 1}, ProducerSpecs}};
init({partitions_sup, Client, Topic, Partitions, Config, TopicProducer}) ->
  PartitionSpecs =
    [ partition_spec(Client, Topic, Partition, Config, TopicProducer)
    || Partition <- Partitions ],
  {ok, {{one_for_one, 0, 1}, PartitionSpecs}}.

client_spec(ClientId, Config) ->
  DelaySeconds = get_client_restart_delay_seconds(Config),
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, [ClientId, Config]}
  , _Restart  = {permanent, DelaySeconds}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

producer_spec(ClientId, TopicName, Config) ->
  DelaySeconds = proplists:get_value(restart_delay_seconds, Config,
                                     ?DEFAULT_PRODUCER_RESTART_DELAY),
  { _Id       = TopicName
  , _Start    = {brod_producer, start_link, [ClientId, TopicName, Config]}
  , _Restart  = {permanent, DelaySeconds}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_producer]
  }.

partition_spec(Client, Topic, Partition, Config, TopicProducer) ->
  DelaySeconds = proplists:get_value(partition_worker_restart_delay_seconds,
                                     Config, ?DEFAULT_PARTITION_RESTART_DELAY),
  { _Id       = Partition
  , _Start    = {brod_partition_producer, start_link,
                 [Client, Topic, Partition, Config, TopicProducer]}
  , _Restart  = {permanent, DelaySeconds}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_partition_producer]
  }.

%% @private Get configured delay seconds before clients restart.
%% NOTE: assuming the client exists in app env.
-spec get_client_restart_delay_seconds(client_id()) -> pos_integer().
get_client_restart_delay_seconds(ClientId) when is_atom(ClientId) ->
  {ok, Clients} = application:get_env(brod, clients),
  {ClientId, Config} = lists:keyfind(ClientId, 1, Clients),
  get_client_restart_delay_seconds(Config);
get_client_restart_delay_seconds(Config) when is_list(Config) ->
  proplists:get_value(restart_delay_seconds, Config,
                      ?DEFAULT_CLIENT_RESTART_DELAY).

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
