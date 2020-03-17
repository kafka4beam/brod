-ifndef(BROD_TEST_MACROS_HRL).
-define(BROD_TEST_MACROS_HRL, true).

-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("hut/include/hut.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

%%====================================================================
%% Macros
%%====================================================================

-define(CLIENT_ID, brod_test_client).
-define(KAFKA_HOST, "localhost").
-define(KAFKA_PORT, 9092).

-define(topic(TestCase, N),
        list_to_binary(atom_to_list(TestCase) ++ integer_to_list(N))).
-define(topic(TestCase), ?topic(TestCase, 1)).
-define(topic, ?topic(?FUNCTION_NAME)).

-define(group_id(TestCase),
        list_to_binary(atom_to_list(TestCase) ++ "_grp" ++ integer_to_list(N))).
-define(group_id(TestCase), ?group_id(TestCase, 1)).
-define(group_id, ?group_id(?FUNCTION_NAME)).

-endif.
