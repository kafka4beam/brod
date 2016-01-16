%%%
%%%   Copyright (c) 2014-2016, Klarna AB
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

-ifndef(__BROD_KAFKA_HRL).
-define(__BROD_KAFKA_HRL, true).

-define(API_VERSION, 0).
-define(MAGIC_BYTE, 0).

-define(REPLICA_ID, -1).

%% Api keys
-define(API_KEY_PRODUCE,            0).
-define(API_KEY_FETCH,              1).
-define(API_KEY_OFFSET,             2).
-define(API_KEY_METADATA,           3).
-define(API_KEY_OFFSET_COMMIT,      8).
-define(API_KEY_OFFSET_FETCH,       9).
-define(API_KEY_GROUP_COORDINATOR, 10).
-define(API_KEY_JOIN_GROUP,        11).
-define(API_KEY_HEARTBEAT,         12).
-define(API_KEY_LEAVE_GROUP,       13).
-define(API_KEY_SYNC_GROUP,        14).
-define(API_KEY_DESCRIBE_GROUPS,   15).
-define(API_KEY_LIST_GROUPS,       16).

-type api_key() :: ?API_KEY_PRODUCE..?API_KEY_LIST_GROUPS.

%% Compression
-define(COMPRESS_NONE, 0).
-define(COMPRESS_GZIP, 1).
-define(COMPRESS_SNAPPY, 2).

-endif. % include brod_kafka.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

