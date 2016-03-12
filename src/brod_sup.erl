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
%%%     |          +-- consumer_sup level 2 for topic 1
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
-behaviour(supervisor3).

-export([ init/1
        , post_init/1
        , start_link/0
        ]).

-include("brod_int.hrl").

%% By deafult, restart client process after a 10-seconds delay
-define(DEFAULT_CLIENT_RESTART_DELAY, 10).

%%%_* APIs =====================================================================

%% @doc Start root supervisor.
%%
%% To start permanent clients, a minimal example of app env (sys.config):
%% ```
%%  [
%%     %% Permanent clients
%%    { clients
%%    , [ {client_1 %% unique client ID
%%        , [ { endpoints, [{"localhost", 9092}]}
%%          , { config
%%            , [ {restart_delay_seconds, 10}
%%                %% @see brod:start_link_client/5 for more client configs
%%              ]
%%            }
%%          ]
%%        }
%%      ]
%%    }
%%  ].
%% '''
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor3:start_link({local, ?MODULE}, ?MODULE, clients_sup).

%% @doc supervisor3 callback.
init(clients_sup) ->
  Clients = application:get_env(brod, clients, []),
  ClientSpecs =
    [ client_spec(ClientId, Args) || {ClientId, Args} <- Clients ],
  %% A client may crash and restart due to network failure
  %% e.g. when none of the kafka endpoints are reachable.
  %% In this case, restart right away will very likely fail again.
  %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
  %% before supervisor tries to restart it.
  {ok, {{one_for_one, 0, 1}, ClientSpecs}}.

%% @doc supervisor3 callback.
post_init(_) ->
  ignore.

%%%_* Internal functions =======================================================
client_spec(ClientId, Args) ->
  Endpoints = proplists:get_value(endpoints, Args, []),
  ok        = verify_config(Endpoints),
  Config0   = proplists:get_value(config, Args, []),
  DelaySecs = proplists:get_value(restart_delay_seconds, Config0,
                                  ?DEFAULT_CLIENT_RESTART_DELAY),
  Config    = proplists:delete(restart_delay_seconds, Config0),
  StartArgs = [Endpoints, ClientId, Config],
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, StartArgs}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

verify_config([]) ->
  exit("No endpoints found in brod client config.");
verify_config(_Endpoints) ->
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
