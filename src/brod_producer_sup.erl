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
-module(brod_producer_sup).

-behaviour(supervisor).

%% API
-export([ start_link/0
        , start_worker/2
        ]).

%% Supervisor callbacks
-export([init/1]).

%%%_* API functions ============================================================
start_link() ->
  supervisor:start_link(?MODULE, []).

start_worker(SupPid, Args) ->
  supervisor:start_child(SupPid, Args).

%%%_* Supervisor callbacks =====================================================
init([]) ->
  Spec = worker(brod_producer_worker, 5000, permanent),
  %% allow max 5 restarts within 30 seconds
  %% if a worker can't reconnect to the same broker it most likely
  %% means we need to reset the state, refresh metadata and retry
  %% all outstanding messages
  {ok, {{simple_one_for_one, 5, 30}, [Spec]}}.

%%%_* Internal functions =======================================================
worker(Module, Timeout, Restart) ->
  MFA = {Module, start_link, []},
  {Module, MFA, Restart, Timeout, worker, [Module]}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
