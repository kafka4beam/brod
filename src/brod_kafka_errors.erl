%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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
%%% @doc A kafka error code handling.
%%%      [https://github.com/apache/kafka/blob/0.8.2/clients/src/
%%%       main/java/org/apache/kafka/common/protocol/Errors.java]
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_kafka_errors).

-export([ desc/1
        , decode/1
        , is_error/1
        ]).

-include("brod_int.hrl").

-define(IS_INT16(X), (X >= -32768 andalso X < 32768)).

%% @doc Return true if it is not ZERO error code.
is_error(0)        -> false;
is_error(?EC_NONE) -> false;
is_error(_)        -> true.

%% @doc Decode kafka protocol error code integer into atoms
%% for undefined error codes, return the original integer
%% @end
-spec decode(integer()) -> error_code().
decode(Ec) -> kpro_errorCode:decode(Ec).

%% @doc Get description string of error codes.
-spec desc(error_code()) -> binary().
desc(Ec) -> kpro_errorCode:desc(Ec).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
