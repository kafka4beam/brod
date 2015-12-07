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
-behaviour(supervisor).

%% TODO add API to start / stop / delete clients
-export([ start_link/0
        , init/1
        ]).

-include("brod_int.hrl").

%%%_* APIs ---------------------------------------------------------------------

%% @doc Start supervisor of per-topic permanent producer supervisors.
%%
%% To start permanent clients, a minimal example of app evn (sys.config):
%%    [ { clients
%%      , [ {client_1
%%          , [ {endpoints, [{"localhost", 9092}]}
%%            ]
%%          }
%%        , {client_2, ...}
%%        ]
%%      }
%%    ].
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, clients_sup).

%% @doc brod_supervisor callback.
init(clients_sup) ->
  init({clients_sup, application:get_env(brod, clients, _Default = [])});
init({clients_sup, Clients}) ->
  SpecFun =
    fun({ClientId, Args}) ->
      { _Id       = ClientId
      , _Start    = {brod_client, start_link, [ClientId, Args]}
      , _Restart  = permanent
      , _Shutdown = 5000
      , _Type     = worker
      , _Module   = [brod_client]
      }
    end,
  Children = [SpecFun(Client) || Client <- Clients],
  {ok, {{one_for_one, 5, 10}, Children}}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
