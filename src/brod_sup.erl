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
%%%
%%% Hiarchy:
%%%   brod_sup (one_for_one)
%%%     |
%%%     +--client_1
%%%     |    |
%%%     |    +-- producers_sup (one_for_one)
%%%     |    |     |
%%%     |    |     +-- topic_1_worker
%%%     |    |     |     |
%%%     |    |     |     +-- producer_sup (one for one)
%%%     |    |     |           |
%%%     |    |     |           +-- partition_0_worker
%%%     |    |     |           |
%%%     |    |     |           +-- partition_1_worker
%%%     |    |     |           |...
%%%     |    |     |
%%%     |    |     +-- topic_2_worker
%%%     |    |     |     |...
%%%     |    |     |...
%%%     |    |
%%%     |    +-- consumers_sup (one_for_one)
%%%     |          |
%%%     |          +-- topic_1_worker
%%%     |          |     |
%%%     |          |     +-- consumer_sup (one for one)
%%%     |          |     |...
%%%     |          |...
%%%     |
%%%     +-- client_2
%%%     |     |...
%%%     |...
%%%
%%% @copyright 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_sup).
-behaviour(brod_supervisor).

-export([ init/1
        , start_link/0
        , start_link_producers_sup/2
        , start_link_producer_sup/4
        ]).

-include("brod_int.hrl").

%% By deafult, restart client process after a 10-seconds delay
-define(DEFAULT_CLIENT_RESTART_DELAY, 10).

%% By default, restart topic worker proess after a 10-seconds delay
-define(DEFAULT_TOPIC_WORKER_RESTART_DELAY, 10).

%% By default, restart partition producer process after a 2-seconds delay
-define(DEFAULT_PRODUCER_RESTART_DELAY, 2).

%%%_* APIs ---------------------------------------------------------------------

%% @doc Start root supervisor.
%%
%% To start permanent clients, a minimal example of app env (sys.config):
%%  [
%%     %% Permanent clients
%%    { clients
%%    , [ {client_1 %% unique client ID
%%        , [ { endpoints, [{"localhost", 9092}]}
%%          , { config
%%            , [ {restart_delay_seconds, 10}
%%                %% @see brod:start_link_client/4 for more client configs
%%              ]
%%            }
%%          , { producers
%%            , [ { <<"test-topic">> %% topic name
%%                , [ {restart_delay_seconds, 10}
%%                  , {producer_restart_delay_seconds, 2}
%%                  , {required_acks, -1}
%%                  %% @see brod:start_link_client/4 for more producer configs
%%                  ]
%%                }
%%              ]
%%            }
%%          ]
%%        }
%%      , {client_2, ...}
%%      ]
%%    }
%%  ].
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, clients_sup).

%% @doc Start a supervisor to manage all topic workers of a client.
-spec start_link_producers_sup(client_id(), [{topic(), producer_config()}]) ->
        {ok, pid()}.
start_link_producers_sup(ClientId, Producers) ->
  brod_supervisor:start_link(?MODULE, {producers_sup, ClientId, Producers}).

%% @doc Start a supervisor to manage all partition producers of a topic.
%%      This supervisor is started in brod_producers worker after it fetchs
%%      metadata of the topic from kafka.
%% @end
-spec start_link_producer_sup(client_id(), topic(),
                              partition(), producer_config()) ->
        {ok, pid()}.
start_link_producer_sup(ClientId, Topic, Partition, Config) ->
  brod_supervisor:start_link(?MODULE, {producer_sup, ClientId, Topic,
                                       Partition, Config}).

%% @doc brod_supervisor callback.
init(clients_sup) ->
  Clients = application:get_env(brod, clients, []),
  ClientSpecs =
    [ client_spec(ClientId, Args) || {ClientId, Args} <- Clients ],
  %% A client may crash and restart due to network failure
  %% e.g. when none of the kafka endpoints are reachable.
  %% In this case, restart right away will very likely fail again.
  %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
  %% before supervisor tries to restart it.
  {ok, {one_for_one, 0, 1}, ClientSpecs};
init({producers_sup, ClientId, Producers}) ->
  ProducerSpecs =
    [ producer_topic_worker_spec(ClientId, TopicName, Config)
    || {TopicName, Config} <- Producers ],
  %% brod_producers may crash in case of exception (error code received)
  %% when fetching topic metata from kafka.
  %% In this case, restart right away will very likely fail again
  %% as kafka errors may very likely last seconds.
  %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
  %% before supervisor tries to restart it.
  {ok, {{one_for_one, 0, 1}, ProducerSpecs}};
init({producer_sup, ClientId, Topic, Partitions, Config}) ->
  PartitionSpecs =
    [ producer_spec(ClientId, Topic, Partition, Config)
    || Partition <- Partitions ],
  %% Producer may crash in case of exception in case of network failure,
  %% or error code received in produce response (e.g. leader transition)
  %% In any case, restart right away will erry likely fail again.
  %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
  %% before supervisor tries to restart it.
  {ok, {{one_for_one, 0, 1}, PartitionSpecs}}.

client_spec(ClientId, Args) ->
  Endpoints = proplists:get_value(endpoints, Args),
  Producers = proplists:get_value(producers, Args),
  Config0   = proplists:get_value(config, Args, []),
  DelaySecs = proplists:get_value(restart_delay_seconds, Config0,
                                  ?DEFAULT_CLIENT_RESTART_DELAY),
  Config    = proplists:delete(restart_delay_seconds, Config0),
  [_|_]     = Endpoints, %% assert
  StartArgs = [ClientId, Endpoints, Config, Producers],
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, StartArgs}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

producer_topic_worker_spec(ClientId, TopicName, Config0) ->
  DelaySecs = proplists:get_value(restart_delay_seconds, Config0,
                                  ?DEFAULT_TOPIC_WORKER_RESTART_DELAY),
  Config    = proplists:delete(restart_delay_seconds, Config0),
  { _Id       = TopicName
  , _Start    = {brod_producers, start_link, [ClientId, TopicName, Config]}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_producers]
  }.

producer_spec(ClientId, Topic, Partition, Config0) ->
  DelaySecs = proplists:get_value(producer_restart_delay_seconds, Config0,
                                  ?DEFAULT_PRODUCER_RESTART_DELAY),
  Config    = proplists:delete(producer_restart_delay_seconds, Config0),
  Args      = [ClientId, Topic, Partition, Config],
  { _Id       = Partition
  , _Start    = {brod_producer, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_producer]
  }.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
