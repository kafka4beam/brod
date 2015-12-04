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
-module(brod_app).
-behaviour(application).

-export([ start/2
        , start/0
        , stop/1
        ]).

%% application callback
start(_StartType, _StartArgs) -> start().

%% @doc Start brod application.
%%
%% Permanent producers can be configured in app env (sys.config).
%% A minimal example of app env with permanent producer args:
%%    [ { producers
%%      , [ {producer_id_1, [{hosts, [{"localhost", 9092}]}]}
%%        , {producer_id_2, ...}
%%        ]
%%      }
%%    ].
%% @see brod:start_link_producer/2 for more info about all producer args
%%
%% @end
start() ->
  _ = application:load(brod), %% ensure loaded
  %% if no producer is configured in app env, start the supervisor empty
  PermanentProducers = application:get_env(brod, producers, _Default = []),
  brod_sup:start_link(PermanentProducers).

stop(_State) -> ok.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
