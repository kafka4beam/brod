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

%% @doc Application callback
start(_StartType, _StartArgs) ->
  brod_sup:start_link().

%% @doc Start brod application.
start() ->
  application:start(brod).

stop(_State) -> ok.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
