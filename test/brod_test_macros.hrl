-ifndef(BROD_TEST_MACROS_HRL).
-define(BROD_TEST_MACROS_HRL, true).

-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("hut/include/hut.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

%%====================================================================
%% Macros
%%====================================================================

-define(TEST_CLIENT_ID, brod_test_client).

-define(KAFKA_HOST, "localhost").
-define(KAFKA_PORT, 9092).

-define(topic(TestCase, N),
        list_to_binary(atom_to_list(TestCase) ++ integer_to_list(N))).
-define(topic(N), ?topic(?FUNCTION_NAME, N)).
-define(topic, ?topic(1)).

-define(group_id(TestCase, N),
        list_to_binary(atom_to_list(TestCase) ++ "_grp" ++ integer_to_list(N))).
-define(group_id(N), ?group_id(?FUNCTION_NAME, N)).
-define(group_id, ?group_id(1)).

-endif.
