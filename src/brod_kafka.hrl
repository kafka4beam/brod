-ifndef(__BROD_KAFKA_HRL).
-define(__BROD_KAFKA_HRL, true).

-define(CLIENT_ID,      <<"brod">>).
-define(CLIENT_ID_SIZE, 4).

-define(API_VERSION, 0).
-define(MAGIC_BYTE, 0).

-define(REPLICA_ID, -1).

%% Api keys
-define(API_KEY_PRODUCE,        0).
-define(API_KEY_FETCH,          1).
-define(API_KEY_OFFSET,         2).
-define(API_KEY_METADATA,       3).
-define(API_KEY_LEADER_AND_ISR, 4).
-define(API_KEY_STOP_REPLICA,   5).
-define(API_KEY_OFFSET_COMMIT,  6).
-define(API_KEY_OFFSET_FETCH,   7).

%% Compression
-define(COMPRESS_NONE, 0).
-define(COMPRESS_GZIP, 1).
-define(COMPRESS_SNAPPY, 2).

-endif. % include brod_kafka.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

