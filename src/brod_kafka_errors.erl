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
%%%      [https://github.com/apache/kafka/blob/0.9.0/clients/src/
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

-define(KAFKA_VERSION, <<"0.9.0.0">>).

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
decode(4)  -> ?EC_INVALID_MESSAGE_SIZE;
decode(5)  -> ?EC_LEADER_NOT_AVAILABLE;
decode(6)  -> ?EC_NOT_LEADER_FOR_PARTITION;
decode(7)  -> ?EC_REQUEST_TIMED_OUT;
decode(8)  -> ?EC_BROKER_NOT_AVAILABLE;
decode(9)  -> ?EC_REPLICA_NOT_AVAILABLE;
decode(10) -> ?EC_MESSAGE_TOO_LARGE;
decode(11) -> ?EC_STALE_CONTROLLER_EPOCH;
decode(12) -> ?EC_OFFSET_METADATA_TOO_LARGE;
decode(13) -> ?EC_NETWORK_EXCEPTION;
decode(14) -> ?EC_GROUP_LOAD_IN_PROGRESS;
decode(15) -> ?EC_GROUP_COORDINATOR_NOT_AVAILABLE;
decode(16) -> ?EC_NOT_COORDINATOR_FOR_GROUP;
decode(17) -> ?EC_INVALID_TOPIC_EXCEPTION;
decode(18) -> ?EC_MESSAGE_LIST_TOO_LARGE;
decode(19) -> ?EC_NOT_ENOUGH_REPLICAS;
decode(20) -> ?EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND;
decode(21) -> ?EC_INVALID_REQUIRED_ACKS;
decode(22) -> ?EC_ILLEGAL_GENERATION;
decode(23) -> ?EC_INCONSISTENT_GROUP_PROTOCOL;
decode(24) -> ?EC_INVALID_GROUP_ID;
decode(25) -> ?EC_UNKNOWN_MEMBER_ID;
decode(26) -> ?EC_INVALID_SESSION_TIMEOUT;
decode(27) -> ?EC_REBALANCE_IN_PROGRESS;
decode(28) -> ?EC_INVALID_COMMIT_OFFSET_SIZE;
decode(29) -> ?EC_TOPIC_AUTHORIZATION_FAILED;
decode(30) -> ?EC_GROUP_AUTHORIZATION_FAILED;
decode(31) -> ?EC_CLUSTER_AUTHORIZATION_FAILED;
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
do_desc(?EC_INVALID_MESSAGE_SIZE) ->
  <<"The message has a negative size.">>;
do_desc(?EC_LEADER_NOT_AVAILABLE) ->
  <<"There is no leader for this topic-partition as "
    "we are in the middle of a leadership election.">>;
do_desc(?EC_NOT_LEADER_FOR_PARTITION) ->
  <<"This server is not the leader for that topic-partition.">>;
do_desc(?EC_REQUEST_TIMED_OUT) ->
  <<"Request exceeds the user-specified time limit in the request.">>;
do_desc(?EC_BROKER_NOT_AVAILABLE) ->
  <<"The broker is not available.">>;
do_desc(?EC_REPLICA_NOT_AVAILABLE) ->
  <<"The replica is not available for the requested topic-partition">>;
do_desc(?EC_MESSAGE_TOO_LARGE) ->
  <<"The request included a message larger than "
    "the max message size the server will accept.">>;
do_desc(?EC_STALE_CONTROLLER_EPOCH) ->
  <<"The controller moved to another broker.">>;
do_desc(?EC_OFFSET_METADATA_TOO_LARGE) ->
  <<"The metadata field of the offset request was too large.">>;
do_desc(?EC_NETWORK_EXCEPTION) ->
  <<"The server disconnected before a response was received.">>;
do_desc(?EC_GROUP_LOAD_IN_PROGRESS) ->
  <<"The coordinator is loading and hence can't process "
    "requests for this group.">>;
do_desc(?EC_GROUP_COORDINATOR_NOT_AVAILABLE) ->
  <<"The group coordinator is not available.">>;
do_desc(?EC_NOT_COORDINATOR_FOR_GROUP) ->
  <<"This is not the correct coordinator for this group.">>;
do_desc(?EC_INVALID_TOPIC_EXCEPTION) ->
  <<"The request attempted to perform an operation on an invalid topic.">>;
do_desc(?EC_MESSAGE_LIST_TOO_LARGE) ->
  <<"The request included message batch larger than "
    "the configured segment size on the server.">>;
do_desc(?EC_NOT_ENOUGH_REPLICAS) ->
  <<"Messages are rejected since there are "
    "fewer in-sync replicas than required.">>;
do_desc(?EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND) ->
  <<"Messages are written to the log, but to "
    "fewer in-sync replicas than required.">>;
do_desc(?EC_INVALID_REQUIRED_ACKS) ->
  <<"Produce request specified an invalid value for required acks.">>;
do_desc(?EC_ILLEGAL_GENERATION) ->
  <<"Specified group generation id is not valid.">>;
do_desc(?EC_INCONSISTENT_GROUP_PROTOCOL) ->
  <<"The group member's supported protocols are incompatible "
    "with those of existing members.">>;
do_desc(?EC_INVALID_GROUP_ID) ->
  <<"The configured groupId is invalid.">>;
do_desc(?EC_UNKNOWN_MEMBER_ID) ->
  <<"The coordinator is not aware of this member.">>;
do_desc(?EC_INVALID_SESSION_TIMEOUT) ->
  <<"The session timeout is not within an acceptable range.">>;
do_desc(?EC_REBALANCE_IN_PROGRESS) ->
  <<"The group is rebalancing, so a rejoin is needed.">>;
do_desc(?EC_INVALID_COMMIT_OFFSET_SIZE) ->
  <<"The committing offset data size is not valid.">>;
do_desc(?EC_TOPIC_AUTHORIZATION_FAILED) ->
  <<"Topic authorization failed.">>;
do_desc(?EC_GROUP_AUTHORIZATION_FAILED) ->
  <<"Group authorization failed.">>;
do_desc(?EC_CLUSTER_AUTHORIZATION_FAILED) ->
  <<"Cluster authorization failed.">>;
do_desc(X) when is_integer(X) ->
  <<"Undefined error code for kafka ", ?KAFKA_VERSION/binary>>.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
