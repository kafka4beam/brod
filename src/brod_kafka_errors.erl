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

-define(KAFKA_VERSION, <<"0.8.2">>).

-define(IS_INT16(X), (X >= -32768 andalso X < 32768)).

%% @doc Return true if it is not ZERO error code.
is_error(0)        -> false;
is_error(?EC_NONE) -> false;
is_error(_)        -> true.

%% @doc Decode kafka protocol error code integer into atoms
%% for undefined error codes, return the original integer
%% @end
-spec decode(integer()) -> error_code().
decode(-1) -> ?EC_UNKNOWN;
decode(0)  -> ?EC_NONE;
decode(1)  -> ?EC_OFFSET_OUT_OF_RANGE;
decode(2)  -> ?EC_CORRUPT_MESSAGE;
decode(3)  -> ?EC_UNKNOWN_TOPIC_OR_PARTITION;
decode(5)  -> ?EC_LEADER_NOT_AVAILABLE;
decode(6)  -> ?EC_NOT_LEADER_FOR_PARTITION;
decode(7)  -> ?EC_REQUEST_TIMED_OUT;
decode(10) -> ?EC_MESSAGE_TOO_LARGE;
decode(12) -> ?EC_OFFSET_METADATA_TOO_LARGE;
decode(13) -> ?EC_NETWORK_EXCEPTION;
decode(17) -> ?EC_INVALID_TOPIC_EXCEPTION;
decode(18) -> ?EC_RECORD_LIST_TOO_LARGE;
decode(19) -> ?EC_NOT_ENOUGH_REPLICAS;
decode(20) -> ?EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND;
decode(X)  -> (true = is_integer(X)) andalso X.

%% @doc Get description string of error codes.
-spec desc(error_code()) -> binary().
desc(ErrorCode) when is_integer(ErrorCode) -> do_desc(decode(ErrorCode));
desc(ErrorCode) when is_atom(ErrorCode)    -> do_desc(ErrorCode).

%% @private Get description string for erro codes, take decoded error code only.
-spec do_desc(error_code()) -> binary().
do_desc(?EC_UNKNOWN) ->
  <<"The server experienced an unexpected error when processing the request">>;
do_desc(?EC_NONE) ->
  <<"no error">>;
do_desc(?EC_OFFSET_OUT_OF_RANGE) ->
  <<"The requested offset is not within the range of "
    "offsets maintained by the server.">>;
do_desc(?EC_CORRUPT_MESSAGE) ->
  <<"The message contents does not match the message CRC "
    "or the message is otherwise corrupt.">>;
do_desc(?EC_UNKNOWN_TOPIC_OR_PARTITION) ->
  <<"This server does not host this topic-partition.">>;
do_desc(?EC_LEADER_NOT_AVAILABLE) ->
  <<"There is no leader for this topic-partition as "
    "we are in the middle of a leadership election.">>;
do_desc(?EC_NOT_LEADER_FOR_PARTITION) ->
  <<"This server is not the leader for that topic-partition.">>;
do_desc(?EC_REQUEST_TIMED_OUT) ->
  <<"The request timed out.">>;
do_desc(?EC_MESSAGE_TOO_LARGE) ->
  <<"The request included a message larger than "
    "the max message size the server will accept.">>;
do_desc(?EC_OFFSET_METADATA_TOO_LARGE) ->
  <<"The metadata field of the offset request was too large.">>;
do_desc(?EC_NETWORK_EXCEPTION) ->
  <<"The server disconnected before a response was received.">>;
do_desc(?EC_INVALID_TOPIC_EXCEPTION) ->
  <<"The request attempted to perform an operation on an invalid topic.">>;
do_desc(?EC_RECORD_LIST_TOO_LARGE) ->
  <<"The request included message batch larger than "
    "the configured segment size on the server.">>;
do_desc(?EC_NOT_ENOUGH_REPLICAS) ->
  <<"Messages are rejected since there are "
    "fewer in-sync replicas than required.">>;
do_desc(?EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND) ->
  <<"Messages are written to the log, but to "
    "fewer in-sync replicas than required.">>;
do_desc(X) when is_integer(X) ->
  <<"Undefeind error code for kafka", ?KAFKA_VERSION/binary>>.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
