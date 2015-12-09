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

-ifndef(__BROD_INT_HRL).
-define(__BROD_INT_HRL, true).

-include("brod.hrl").

%% Error code macros, mirrored (including TODOs) from:
%% https://github.com/apache/kafka/blob/0.8.2/clients/src/
%%       main/java/org/apache/kafka/common/protocol/Errors.java
-define(EC_UNKNOWN,                    'UnknownServerException').           % -1
-define(EC_NONE,                       'no_error').                         %  0
-define(EC_OFFSET_OUT_OF_RANGE,        'OffsetOutOfRangeException').        %  1
-define(EC_CORRUPT_MESSAGE,            'CorruptRecordException').           %  2
-define(EC_UNKNOWN_TOPIC_OR_PARTITION, 'UnknownTopicOrPartitionException'). %  3
%% TODO: errorCode 4 for InvalidFetchSize
-define(EC_LEADER_NOT_AVAILABLE,       'LeaderNotAvailableException').
-define(EC_NOT_LEADER_FOR_PARTITION,   'NotLeaderForPartitionException').   %  6
-define(EC_REQUEST_TIMED_OUT,          'TimeoutException').                 %  7
%% TODO: errorCode 8, 9, 11
-define(EC_MESSAGE_TOO_LARGE,          'RecordTooLargeException').          % 10
-define(EC_OFFSET_METADATA_TOO_LARGE,  'OffsetMetadataTooLarge').           % 12
-define(EC_NETWORK_EXCEPTION,          'NetworkException').                 % 13
%% TODO: errorCode 14, 15, 16
-define(EC_INVALID_TOPIC_EXCEPTION,    'InvalidTopicException').            % 17
-define(EC_RECORD_LIST_TOO_LARGE,      'RecordBatchTooLargeException').     % 18
-define(EC_NOT_ENOUGH_REPLICAS,        'NotEnoughReplicasException').       % 19
-define(EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        'NotEnoughReplicasAfterAppendException').                           % 20

-define(MAX_CORR_ID, 2147483647). % 2^31 - 1

%% {cluster_id(), topic(), partition()} => pid()
-define(PARTITION_WORKER_TAB, t_partition_workers).

-type error_code() :: atom() | integer().

-type hostname()        :: string().
-type portnum()         :: pos_integer().
-type endpoint()        :: {hostname(), portnum()}.
-type cluster_id()      :: atom().
-type leader_id()       :: non_neg_integer().
-type topic()           :: binary().
-type partition()       :: non_neg_integer().
-type corr_id()         :: 0..?MAX_CORR_ID.
-type offset()          :: integer().
-type kafka_kv()        :: {binary(), binary()}.
-type client_config()   :: proplists:proplist().
-type producer_config() :: proplists:proplist().
-type client()          :: client_id() | pid().


-record(socket, { pid     :: pid()
                , host    :: string()
                , port    :: integer()
                , node_id :: integer()
                }).


%% 'from' is handle cases when worker's buffer is full
%% and we need to block user's process via {noreply, State}
%% and then release via gen_server:reply
-record(request, { ref        :: reference()
                 , sender_pid :: pid()
                 , from       :: term() % pretending it's opaque
                 , topic      :: topic()
                 , partition  :: partition()
                 , data       :: [kafka_kv()]
                 }).

%%%_* metadata request ---------------------------------------------------------
-record(metadata_request, {topics = [] :: [binary()]}).

%%%_* metadata response --------------------------------------------------------
%% 'isrs' - 'in sync replicas', the subset of the replicas
%% that are "caught up" to the leader
-record(partition_metadata, { error_code :: error_code()
                            , id         :: partition()
                            , leader_id  :: integer()
                            , replicas   :: [integer()]
                            , isrs       :: [integer()]
                            }).

-record(topic_metadata, { error_code :: error_code()
                        , name       :: topic()
                        , partitions :: [#partition_metadata{}]}).

-record(broker_metadata, { node_id :: integer()
                         , host    :: string()
                         , port    :: integer()
                         }).

-record(metadata_response, { brokers = [] :: [#broker_metadata{}]
                           , topics  = [] :: [#topic_metadata{}]
                           }).

%%%_* produce request ----------------------------------------------------------
-ifdef(otp_before_17).
-type produce_request_data() :: [{topic(), dict()}].
-else.
-type produce_request_data() :: [{topic(),
                                  dict:dict(partition(), [kafka_kv()])}].
-endif.

-record(produce_request, { acks    :: integer()
                         , timeout :: integer()
                         , data    :: produce_request_data()
                         }).

%%%_* produce response ---------------------------------------------------------
-record(produce_offset, { partition  :: partition()
                        , error_code :: error_code()
                        , offset     :: offset()
                        }).

-record(produce_topic, { topic   :: topic()
                       , offsets :: [#produce_offset{}]
                       }).

-record(produce_response, {topics = [] :: [#produce_topic{}]}).

%%%_* offset request -----------------------------------------------------------
%% Protocol allows to request offsets for any number of topics and partitions
%% at once, but we use only single pair assuming the most cases users spawn
%% separate connections for each topic-partition.
-record(offset_request, { topic             :: topic()
                        , partition         :: partition()
                        , time              :: integer()
                        , max_n_offsets = 1 :: integer()
                        }).

%%%_* offset response ----------------------------------------------------------
-record(partition_offsets, { partition  :: partition()
                           , error_code :: error_code()
                           , offsets    :: [offset()]
                           }).

-record(offset_topic, { topic      :: topic()
                      , partitions :: [#partition_offsets{}]
                      }).

-record(offset_response, {topics :: [#offset_topic{}]}).

%%%_* fetch request ------------------------------------------------------------
%% Protocol allows to subscribe on data from any number of topics and partitions
%% at once, but we use only single pair assuming the most cases users spawn
%% separate connections for each topic-partition.
-record(fetch_request, { max_wait_time :: integer()
                       , min_bytes     :: integer()
                       , topic         :: topic()
                       , partition     :: partition()
                       , offset        :: offset()
                       , max_bytes     :: integer()
                       }).

%%%_* fetch response -----------------------------------------------------------
%% definition of #message{} is in include/brod.hrl
-record(partition_messages, { partition      :: partition()
                            , error_code     :: error_code()
                            , high_wm_offset :: integer()
                            , last_offset    :: integer()
                            , messages       :: [#message{}]
                            }).

-record(topic_fetch_data, { topic      :: topic()
                          , partitions :: [#partition_messages{}]
                          }).

-record(fetch_response, {topics = [#topic_fetch_data{}]}).

-define(undef, undefined).

-endif. % include brod_int.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

