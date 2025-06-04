%%%
%%%   Copyright (c) 2014-2021, Klarna Bank AB (publ)
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
-include_lib("kernel/include/logger.hrl").

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

-record(cbm_init_data,
        { committed_offsets :: brod_topic_subscriber:committed_offsets()
        , cb_fun            :: brod_topic_subscriber:cb_fun()
        , cb_data           :: term()
        }).

-type sasl_opt() :: {plain, User :: string() | binary(),
                            Pass :: string() | binary() |
                                    fun(() -> string() | binary())}
                  | {plain, File :: file:name_all()}
                  | {callback, module(), term()}
                  | ?undef.

%% Is kafka error code
-define(IS_ERROR(EC), ((EC) =/= ?no_error)).

-define(KV(Key, Value), {Key, Value}).
-define(TKV(Ts, Key, Value), {Ts, Key, Value}).

-define(acked, brod_produce_req_acked).
-define(buffered, brod_produce_req_buffered).

-define(KAFKA_0_9,  {0,  9}).
-define(KAFKA_0_10, {0, 10}).

-ifdef(OTP_RELEASE).
-define(BIND_STACKTRACE(Var), :Var).
-define(GET_STACKTRACE(Var), ok).
-else.
-define(BIND_STACKTRACE(Var), ).
-define(GET_STACKTRACE(Var), Var = erlang:get_stacktrace()).
-endif.

%% Brod logging wrappers around Logger API calls. Insert 'brod' domain
%% to allow applications to filter Brod logs as they wish.
-define(BROD_LOG_WARNING(Fmt, Args), ?LOG_WARNING(Fmt, Args, #{domain => [brod]})).
-define(BROD_LOG_ERROR(Fmt, Args),   ?LOG_ERROR(  Fmt, Args, #{domain => [brod]})).
-define(BROD_LOG_INFO(Fmt, Args),    ?LOG_INFO(   Fmt, Args, #{domain => [brod]})).
-define(BROD_LOG(Level, Fmt, Args),  ?LOG(Level,  Fmt, Args, #{domain => [brod]})).

-endif. % include brod_int.hrl

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
