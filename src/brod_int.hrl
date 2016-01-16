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

-ifndef(__BROD_INT_HRL).
-define(__BROD_INT_HRL, true).

-include("brod.hrl").

%% Error code macros, from:
%% https://github.com/apache/kafka/blob/0.9.0/clients/src/
%%       main/java/org/apache/kafka/common/protocol/Errors.java
-define(EC_UNKNOWN,                    'UnknownError').                     % -1
-define(EC_NONE,                       'no_error').                         %  0
-define(EC_OFFSET_OUT_OF_RANGE,        'OffsetOutOfRange').                 %  1
-define(EC_CORRUPT_MESSAGE,            'CorruptMessage').                   %  2
-define(EC_UNKNOWN_TOPIC_OR_PARTITION, 'UnknownTopicOrPartition').          %  3
-define(EC_INVALID_MESSAGE_SIZE,       'InvalidMessageSize').               %  4
-define(EC_LEADER_NOT_AVAILABLE,       'LeaderNotAvailable').               %  5
-define(EC_NOT_LEADER_FOR_PARTITION,   'NotLeaderForPartition').            %  6
-define(EC_REQUEST_TIMED_OUT,          'RequestTimedOut').                  %  7
-define(EC_BROKER_NOT_AVAILABLE,       'BrokerNotAvailable').               %  8
-define(EC_REPLICA_NOT_AVAILABLE,      'ReplicaNotAvailable').              %  9
-define(EC_MESSAGE_TOO_LARGE,          'MessageTooLarge').                  % 10
-define(EC_STALE_CONTROLLER_EPOCH,     'StaleControllerEpoch').             % 11
-define(EC_OFFSET_METADATA_TOO_LARGE,  'OffsetMetadataTooLarge').           % 12
-define(EC_NETWORK_EXCEPTION,          'NetworkException').                 % 13
-define(EC_GROUP_LOAD_IN_PROGRESS,     'GroupLoadInProgress').              % 14
-define(EC_GROUP_COORDINATOR_NOT_AVAILABLE,
        'GroupCoordinatorNotAvailable').                                    % 15
-define(EC_NOT_COORDINATOR_FOR_GROUP,  'NotCoordinatorForGroup').           % 16
-define(EC_INVALID_TOPIC_EXCEPTION,    'InvalidTopicException').            % 17
-define(EC_MESSAGE_LIST_TOO_LARGE,     'MessageListTooLargeException').     % 18
-define(EC_NOT_ENOUGH_REPLICAS,        'NotEnoughReplicasException').       % 19
-define(EC_NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        'NotEnoughReplicasAfterAppendException').                           % 20
-define(EC_INVALID_REQUIRED_ACKS,      'InvalidRequiredAcks').              % 21
-define(EC_ILLEGAL_GENERATION,         'IllegalGeneration').                % 22
-define(EC_INCONSISTENT_GROUP_PROTOCOL, 'InconsistentGroupProtocol').       % 23
-define(EC_INVALID_GROUP_ID,           'InvalidGroupId').                   % 24
-define(EC_UNKNOWN_MEMBER_ID,          'UnknownMemberId').                  % 25
-define(EC_INVALID_SESSION_TIMEOUT,    'InvalidSessionTimeout').            % 26
-define(EC_REBALANCE_IN_PROGRESS,      'RebalanceInProgress').              % 27
-define(EC_INVALID_COMMIT_OFFSET_SIZE, 'InvalidCommitOffsetSize').          % 28
-define(EC_TOPIC_AUTHORIZATION_FAILED, 'TopicAuthorizationFailed').         % 29
-define(EC_GROUP_AUTHORIZATION_FAILED, 'GroupAuthorizationFailed').         % 30
-define(EC_CLUSTER_AUTHORIZATION_FAILED, 'ClusterAuthorizationFailed').     % 31

-define(MAX_CORR_ID, 2147483647). % 2^31 - 1

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
-type client_config()   :: brod_client_config().
-type producer_config() :: brod_producer_config().
-type consumer_config() :: brod_consumer_config().
-type client()          :: client_id() | pid().
-type required_acks()   :: -1..1.

-record(socket, { pid     :: pid()
                , host    :: string()
                , port    :: integer()
                , node_id :: integer()
                }).

-define(undef, undefined).

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

-type produce_request_data() :: [{topic(), [{partition(), [kafka_kv()]}]}].

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

%%%_* offset commit request ----------------------------------------------------
%% Offset commit api version description from kafka protocol docs:
%% In v0 and v1, the time stamp of each partition is defined as the
%% commit time stamp, and the offset coordinator will retain the
%% committed offset until its commit time stamp + offset retention
%% time specified in the broker config; if the time stamp field is not
%% set, brokers will set the commit time as the receive time before
%% committing the offset, users can explicitly set the commit time
%% stamp if they want to retain the committed offset longer on the
%% broker than the configured offset retention time.
%% In v2, we removed the time stamp field but add a global retention
%% time field (see KAFKA-1634 for details); brokers will then always
%% set the commit time stamp as the receive time, but the committed
%% offset can be retained until its commit time stamp + user specified
%% retention time in the commit request. If the retention time is not
%% set, the broker offset retention time will be used as default.

-record(offset_commit_request_partition, { partition :: integer()
                                         , offset    :: integer()
                                         , timestamp :: integer() % v1 only
                                         , metadata  :: binary()
                                         }).

-record(offset_commit_request_topic,
        { topic      :: binary()
        , partitions :: [#offset_commit_request_partition{}]
        }).

-record(offset_commit_request,
        { version                      :: 0..2
        , consumer_group_id            :: binary()  % v 0,1,2
        , consumer_group_generation_id :: integer() % v   1,2
        , consumer_id                  :: binary()  % v   1,2
        , retention_time               :: integer() % v     2
        , topics                       :: [#offset_commit_request_topic{}]
        }).

%%%_* offset_commit response ---------------------------------------------------
-record(offset_commit_response_partition, { partition  :: integer()
                                          , error_code :: integer()
                                          }).

-record(offset_commit_response_topic,
        { topic      :: binary()
        , partitions :: [#offset_commit_response_partition{}]
        }).

-record(offset_commit_response, {topics = [#offset_commit_response_topic{}]}).

%%%_* offset fetch request -----------------------------------------------------
-record(offset_fetch_request_topic, { topic      :: binary()
                                    , partitions :: [integer()]
                                    }).

-record(offset_fetch_request,
       { consumer_group :: binary()
       , topics         :: [#offset_fetch_request_topic{}]
       }).

%%%_* offset fetch response ----------------------------------------------------
-record(offset_fetch_response_partition, { partition  :: integer()
                                         , offset     :: integer()
                                         , metadata   :: binary()
                                         , error_code :: integer()
                                         }).

-record(offset_fetch_response_topic,
        { topic      :: binary()
        , partitions :: [#offset_fetch_response_partition{}]
        }).

-record(offset_fetch_response, {topics :: [#offset_fetch_response_topic{}]}).

%%%_* group coordinator request ------------------------------------------------
-record(group_coordinator_request, {group_id :: binary()}).

%%%_* group coordinator response -----------------------------------------------
-record(group_coordinator_response, { error_code       :: integer()
                                    , coordinator_id   :: integer()
                                    , coordinator_host :: binary()
                                    , coordinator_port :: integer()
                                    }).

%%%_* describe groups request --------------------------------------------------
-record(describe_groups_request, {groups :: [binary()]}).

%%%_* describe groups response -------------------------------------------------
-record(group_member, { member_id         :: binary()
                      , client_id         :: binary()
                      , client_host       :: binary()
                      , member_metadata   :: binary()
                      , member_assignment :: binary()
                      }).

-record(describe_group_response, { error_code    :: integer()
                                 , group_id      :: binary()
                                 , state         :: binary()
                                 , protocol_type :: binary()
                                 , protocol      :: binary()
                                 , members       :: [#group_member{}]
                                 }).

-record(describe_groups_response, {groups = [#describe_group_response{}]}).

-endif. % include brod_int.hrl

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
