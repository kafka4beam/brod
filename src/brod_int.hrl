-ifndef(__BROD_INT_HRL).
-define(__BROD_INT_HRL, true).

%%%_* metadata response --------------------------------------------------------
-record(broker_metadata, { node_id :: integer()
                         , host    :: binary()
                         , port    :: integer()
                         }).

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

%%%_* produce request ----------------------------------------------------------
-record(produce, { acks    :: integer()
                 , timeout :: integer()
                 , topics  :: dict() % Topic -> dict(Partition -> [{K, V}])
                 }).

%%%_* produce response ---------------------------------------------------------
-record(offset, { partition_id :: integer()
                , error_code   :: integer()
                , offset       :: integer()
                }).

-record(topic_offsets, { topic   :: binary()
                       , offsets :: [#offset{}]
                       }).

%%%_* methods ------------------------------------------------------------------
-define(PRODUCE,        produce).
-define(FETCH,          fetch).
-define(OFFSET,         offset).
-define(METADATA,       metadata).
-define(LEADER_AND_ISR, leader_and_isr).
-define(STOP_REPLICA,   stop_replica).
-define(OFFSET_COMMIT,  offset_commit).
-define(OFFSET_FETCH,   offset_fetch).

-endif. % include brod_int.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

