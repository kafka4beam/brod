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
-module(brod_sup).
-behaviour(brod_supervisor).

-export([ init/1
        , start_link/0
        , start_link_client_sup/1
        , start_link_producer_sup/1
        ]).

-include("brod_int.hrl").

%% By deafult, restart client process after a 10-seconds delay
-define(DEFAULT_CLIENT_RESTART_DELAY, 10).

%% By default, restart producer proess after a 10-seconds delay
-define(DEFAULT_PRODUCER_RESTART_DELAY, 11).

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
%%              {required_acks, -1}
%%              {partitionner, roundrobin}
%%              %% @see brod:start_link_producer/3 for more producer configs
%%            ]
%%          }
%%        ]
%%      }
%%    ].
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, root).

-spec start_link_client_sup(client_id()) -> {ok, pid()}.
start_link_client_sup(ClientId) ->
  brod_supervisor:start_link(?MODULE, {client_sup, ClientId}).

-spec start_link_producer_sup(client_id()) -> {ok, pid()}.
start_link_producer_sup(ClientId) ->
  brod_supervisor:start_link(?MODULE, {producer_sup, ClientId}).

%% @doc brod_supervisor callback.
init(root) ->
  Clients = application:get_env(brod, clients, []),
  ClientSupSpecs =
    [client_sup_spec(ClientId) || {ClientId, _Config} <- Clients],
  {ok, {one_for_one, 5, 10}, ClientSupSpecs};
init({client_sup, ClientId}) ->
  ClientSpec = client_spec(ClientId),
  ProducerSupSpec = producer_sup_spec(ClientId),
  %% A client may crash and restart due to network failure
  %% e.g. when none of the kafka endpoints are reachable.
  %% In this case, restart it right away will very likely fail again,
  %% hence we set MaxR=0 here to stop it from restarting immediately
  %% and it should be restarted after a configurable N-secons delay
  %%
  %% In case a client crashes,
  %% restart all its producers by restating
  %% their supervisor. i.e. rest_for_one
  {ok, {rest_for_one, 0, 1}, [ClientSpec, ProducerSupSpec]};
init({producer, ClientId}) ->
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
  {ok, {{one_for_one, 0, 1}, ProducerSpecs}}.

client_sup_spec(ClientId) ->
  { _Id       = {client_sup, ClientId}
  , _Start    = {?MODULE, start_link_client_sup, [ClientId]}
  , _Restart  = permanent
  , _Shutdown = 5000
  , _Type     = supervisor
  , _Module   = [?MODULE]
  }.

client_spec(ClientId) ->
  {ok, Clients} = application:get_env(brod, clients),
  {ClientId, Config} = lists:keyfind(ClientId, 1, Clients),
  DelaySeconds = get_client_restart_delay_seconds(Config),
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, [ClientId, Config]}
  , _Restart  = {permanent, DelaySeconds}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

producer_sup_spec(ClientId) ->
  %% Restart producer supervisor 1 second after client restart.
  DelaySeconds = get_client_restart_delay_seconds(ClientId) + 1,
  { _Id       = producer_sup
  , _Start    = {?MODULE, start_link_producer_sup, [ClientId]}
  , _Restart  = {permanent, DelaySeconds}
  , _Shutdown = 5000
  , _Type     = supervisor
  , _Module   = [?MODULE]
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

%% @private Get configured delay seconds bfore clients restart.
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
