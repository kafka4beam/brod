-ifndef(__BROD_HRL).
-define(__BROD_HRL, true).

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

-record(metadata, { brokers = [#broker_metadata{}]
                  , topics = [#topic_metadata{}]
                  }).

%%%_* produce request ----------------------------------------------------------
-record(message, { key   :: binary()
                 , value :: binary()
                 }).

-record(partition_messages, { id       :: integer()
                            , messages :: [#message{}]
                            }).

-record(data, { topic      :: binary()
              , partitions :: [#partition_messages{}]
              }).

%%%_* produce response ---------------------------------------------------------
-record(offset, { partition_id :: integer()
                , error_code   :: integer()
                , offset       :: integer()
                }).

-record(topic_offsets, { topic   :: binary()
                       , offsets :: [#offset{}]
                       }).

-endif. % include brod.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

