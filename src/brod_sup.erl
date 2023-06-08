%%%
%%%   Copyright (c) 2015-2021 Klarna Bank AB (publ)
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
%%% @private brod supervisor
%%%
%%% Hierarchy:
%%%   brod_sup (one_for_one)
%%%     |
%%%     +--client_1
%%%     |    |
%%%     |    +-- producers_sup level 1
%%%     |    |     |
%%%     |    |     +-- producers_sup level 2 for topic 1
%%%     |    |     |     |
%%%     |    |     |     +-- partition_0_worker
%%%     |    |     |     |
%%%     |    |     |     +-- partition_1_worker
%%%     |    |     |     |...
%%%     |    |     |
%%%     |    |     +-- producers_sup level 2 for topic 2
%%%     |    |     |     |...
%%%     |    |     |...
%%%     |    |
%%%     |    +-- consumers_sup level 1
%%%     |          |
%%%     |          +-- consumer_sup level 2 for topic 1
%%%     |          |     |
%%%     |          |     +-- partition_0_worker
%%%     |          |     |
%%%     |          |     +-- partition_1_worker
%%%     |          |     |...
%%%     |          |
%%%     |          +-- consumer_sup level 2 for topic 2
%%%     |          |     |...
%%%     |          |...
%%%     |
%%%     +-- client_2
%%%     |     |...
%%%     |...
%%%
%%% @end
%%%=============================================================================

-module(brod_sup).
-behaviour(brod_supervisor3).

-export([ init/1
        , post_init/1
        , start_link/0
        , start_client/3
        , stop_client/1
        , find_client/1
        ]).

-include("brod_int.hrl").

-define(SUP, ?MODULE).

%% By default, restart client process after a 10-seconds delay
-define(DEFAULT_CLIENT_RESTART_DELAY, 10).

%%%_* APIs =====================================================================

%% @doc Start root supervisor.
%%
%% To start permanent clients add 'clients' section in sys.config.
%% So far only 'endpoints' config is mandatory, other options are optional.
%%
%% ```
%%  [
%%     %% Permanent clients
%%    { clients
%%    , [ {client_1 %% unique client ID
%%        , [ {endpoints, [{"localhost", 9092}]}
%%          , {restart_delay_seconds, 10}
%%          , {get_metadata_timeout_seconds, 5}
%%          , {reconnect_cool_down_seconds, 1}
%%          , {allow_topic_auto_creation, true}
%%          , {auto_start_producers, false}
%%          , {default_producer_config, []}
%%          ]
%%        }
%%      ]
%%    }
%%  ].
%% '''
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor3:start_link({local, ?SUP}, ?MODULE, clients_sup).

-spec start_client([brod:endpoint()],
                   brod:client_id(),
                   brod:client_config()) -> ok | {error, any()}.
start_client(Endpoints, ClientId, Config) ->
  ClientSpec = client_spec(Endpoints, ClientId, Config),
  case brod_supervisor3:start_child(?SUP, ClientSpec) of
    {ok, _Pid} -> ok;
    Error      -> Error
  end.

-spec stop_client(brod:client_id()) -> ok | {error, any()}.
stop_client(ClientId) ->
  _ = brod_supervisor3:terminate_child(?SUP, ClientId),
  brod_supervisor3:delete_child(?SUP, ClientId).

-spec find_client(brod:client_id()) -> [pid()].
find_client(Client) ->
  brod_supervisor3:find_child(?SUP, Client).

%% @doc brod_supervisor3 callback
init(clients_sup) ->
  %% start and link it to root supervisor
  {ok, _} = brod_kafka_apis:start_link(),
  Clients = application:get_env(brod, clients, []),
  ClientSpecs =
    lists:map(fun({ClientId, Args}) ->
                is_atom(ClientId) orelse exit({bad_client_id, ClientId}),
                client_spec(ClientId, Args)
              end, Clients),
  %% A client may crash and restart due to network failure
  %% e.g. when none of the kafka endpoints are reachable.
  %% In this case, restart right away will very likely fail again.
  %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
  %% before supervisor tries to restart it.
  {ok, {{one_for_one, 0, 1}, ClientSpecs}}.

%% @doc brod_supervisor3 callback.
post_init(_) ->
  ignore.

%%%_* Internal functions =======================================================
client_spec(ClientId, Config) ->
  Endpoints = proplists:get_value(endpoints, Config, []),
  client_spec(Endpoints, ClientId, Config).

client_spec([], ClientId, _Config) ->
  Error = lists:flatten(
            io_lib:format("No endpoints found in brod client '~p' config",
                          [ClientId])),
  exit(Error);
client_spec(Endpoints, ClientId, Config0) ->
  DelaySecs = proplists:get_value(restart_delay_seconds, Config0,
                                  ?DEFAULT_CLIENT_RESTART_DELAY),
  Config1   = proplists:delete(restart_delay_seconds, Config0),
  Config    = brod_utils:init_sasl_opt(Config1),
  StartArgs = [Endpoints, ClientId, Config],
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, StartArgs}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
