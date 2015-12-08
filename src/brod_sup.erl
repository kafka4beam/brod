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

-export([ start_link/0
        , start_client/2
        , init/1
        ]).

-include("brod_int.hrl").

%%%_* APIs ---------------------------------------------------------------------

%% @doc Start root supervisor.
%%
%% To start permanent clients, a minimal example of app env (sys.config):
%%    [ { clients
%%      , [ {client_1
%%          , [ {endpoints, [{"localhost", 9092}]}
%%            ]
%%          }
%%        , {client_2, ...}
%%        ]
%%      }
%%    ].
%% TODO: add permanent producers
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, clients_sup).

%% @doc Start client under brod_sup.
-spec start_client(client_id(), client_config()) -> ok | {error, any()}.
start_client(ClientId, Config) ->
  case brod_supervisor:find_child(brod_sup, ClientId) of
    [ ] ->
      Spec = client_spec(ClientId, Config),
      case brod_supervisor:start_child(brod_sup, Spec) of
        {ok, Pid} when is_pid(Pid)        -> ok;
        {ok, Pid, _Info} when is_pid(Pid) -> ok;
        {error, Reason}                   -> {error, Reason};
        Other                             -> {error, {failed, Other}}
      end;
    [_] ->
      {error, already_started}
  end.

%% @doc brod_supervisor callback.
init(clients_sup) ->
  init({clients_sup, application:get_env(brod, clients, _Default = [])});
init({clients_sup, Clients}) ->
  Children = lists:map(fun({ClientId, Config}) ->
                          client_spec(ClientId, Config)
                       end, Clients),
  {ok, {{one_for_one, 5, 10}, Children}}.

client_spec(ClientId, Config) ->
  { _Id       = ClientId
  , _Start    = {brod_client, start_link, [ClientId, Config]}
  , _Restart  = {permanent, 30} %% TODO make it configurable
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_client]
  }.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
