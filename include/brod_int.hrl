%%%
%%%   Copyright (c) 2014-2017, Klarna AB
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

-ifndef(__BROD_INT_HRL).
-define(__BROD_INT_HRL, true).

-include("brod.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

-define(undef, undefined).

-define(OFFSET_EARLIEST, earliest).
-define(OFFSET_LATEST, latest).
-define(IS_SPECIAL_OFFSET(O), (O =:= ?OFFSET_EARLIEST orelse
                               O =:= ?OFFSET_LATEST orelse
                               O =:= -2 orelse
                               O =:= -1)).

-record(socket, { pid     :: pid()
                , host    :: string()
                , port    :: integer()
                , node_id :: integer()
                }).

-type sasl_opt() :: {plain, User :: string() | binary(),
                            Pass :: string() | binary() |
                                    fun(() -> string() | binary())}
                  | {plain, File :: file:name_all()}
                  | {callback, module(), term()}
                  | ?undef.

%% Is kafka error code
-define(IS_ERROR(EC), kpro_error_code:is_error(EC)).

-endif. % include brod_int.hrl

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
