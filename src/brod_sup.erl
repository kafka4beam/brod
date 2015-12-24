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
%%%     |    |     |   |
%%%     |    |     |   +-- partition_0_worker
%%%     |    |     |   |
%%%     |    |     |   +-- partition_1_worker
%%%     |    |     |   |...
%%%     |    |     |
%%%     |    |     +-- producers_sup level 2 for topic 2
%%%     |    |     |   |...
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
%%                %% @see brod:start_link_client/4 for more client configs
%%              ]
%%            }
%%          , { producers
%%            , [ { <<"test-topic">> %% topic name
%%                , [ {topic_restart_delay_seconds, 2}
%%                  , {partition_restart_delay_seconds, 2}
%%                  , {required_acks, -1}
%%                  %% @see brod:start_link_client/4 for more producer configs
%%                  ]
%%                }
%%              ]
%%            }
%%          ]
%%        }
%%      , {client_2 %% unique client ID
%%        , [ { endpoints, [{"localhost", 9092}]}
%%          , { config
%%            , [ {restart_delay_seconds, 10}
%%                %% @see brod:start_link_client/4 for more client configs
%%              ]
%%            }
%%          , { consumers
%%            , [ { <<"test-topic">> %% topic name
%%                , [] %% equiv all partitions
%%                }
%%              , { <<"test-topic-2">> %% topic name
%%                , [ {partitions, [1,2,3]}
%%                  ]
%%                }
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
  Producers = proplists:get_value(producers, Args, []),
  Consumers = proplists:get_value(consumers, Args, []),
  ok = verify_config(Endpoints, Producers, Consumers),
  Config0   = proplists:get_value(config, Args, []),
  DelaySecs = proplists:get_value(restart_delay_seconds, Config0,
                                  ?DEFAULT_CLIENT_RESTART_DELAY),
  Config    = proplists:delete(restart_delay_seconds, Config0),
  StartArgs = [ClientId, Endpoints, Producers, Consumers, Config],
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, StartArgs}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

verify_config([], _Producers, _Consumers) ->
  exit("No endpoints found in brod client config.");
verify_config(_Endpoints, [], []) ->
  exit("At least one non-empty list of {producers, _} or {consumers, _}"
      " required in brod client config.");
verify_config(_Endpoints, _Producers, _Consumers) ->
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
