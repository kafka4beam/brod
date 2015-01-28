-ifndef(__BROD_INT_HRL).
-define(__BROD_INT_HRL, true).

-include("../include/brod.hrl").

-record(socket, { pid     :: pid()
                , host    :: string()
                , port    :: integer()
                , node_id :: integer()
                }).

%%%_* metadata request ---------------------------------------------------------
-record(metadata_request, {topics = [] :: [binary()]}).

%%%_* metadata response --------------------------------------------------------
%% 'isrs' - 'in sync replicas', the subset of the replicas
%% that are "caught up" to the leader
-record(partition_metadata, { error_code :: integer()
                            , id         :: integer()
                            , leader_id  :: integer()
                            , replicas   :: [integer()]
                            , isrs       :: [integer()]
                            }).

-record(topic_metadata, { error_code :: integer()
                        , name       :: binary()
                        , partitions :: [#partition_metadata{}]}).

-record(broker_metadata, { node_id :: integer()
                         , host    :: string()
                         , port    :: integer()
                         }).

-record(metadata_response, { brokers = [] :: [#broker_metadata{}]
                           , topics  = [] :: [#topic_metadata{}]
                           }).

%%%_* produce request ----------------------------------------------------------
-record(produce_request, { acks    :: integer()
                         , timeout :: integer()
                         %% [{Topic :: binary(), [dict(Partition -> [{K, V}])]}]
                         %% skipping spec to make it working with R15..R17
                         , data
                         }).

%%%_* produce response ---------------------------------------------------------
-record(produce_offset, { partition  :: integer()
                        , error_code :: integer()
                        , offset     :: integer()
                        }).

-record(produce_topic, { topic   :: binary()
                       , offsets :: [#produce_offset{}]
                       }).

-record(produce_response, {topics = [] :: [#produce_topic{}]}).

%%%_* offset request -----------------------------------------------------------
%% Protocol allows to request offsets for any number of topics and partitions
%% at once, but we use only single pair assuming the most cases users spawn
%% separate connections for each topic-partition.
-record(offset_request, { topic             :: binary()
                        , partition         :: integer()
                        , time              :: integer()
                        , max_n_offsets = 1 :: integer()
                        }).

%%%_* offset response ----------------------------------------------------------
-record(partition_offsets, { partition  :: integer()
                           , error_code :: integer()
                           , offsets    :: [integer()]
                           }).

-record(offset_topic, { topic      :: binary()
                      , partitions :: [#partition_offsets{}]
                      }).

-record(offset_response, {topics :: [#offset_topic{}]}).

%%%_* fetch request ------------------------------------------------------------
%% Protocol allows to subscribe on data from any number of topics and partitions
%% at once, but we use only single pair assuming the most cases users spawn
%% separate connections for each topic-partition.
-record(fetch_request, { max_wait_time :: integer()
                       , min_bytes     :: integer()
                       , topic         :: binary()
                       , partition     :: integer()
                       , offset        :: integer()
                       , max_bytes     :: integer()
                       }).

%%%_* fetch response -----------------------------------------------------------
%% definition of #message{} is in include/brod.hrl
-record(partition_messages, { partition      :: integer()
                            , error_code     :: integer()
                            , high_wm_offset :: integer()
                            , last_offset    :: integer()
                            , messages       :: [#message{}]
                            }).

-record(topic_fetch_data, { topic      :: binary()
                          , partitions :: [#partition_messages{}]
                          }).

-record(fetch_response, {topics = [#topic_fetch_data{}]}).

-endif. % include brod_int.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

