%%%
%%%   Copyright (c) 2019-2021, Klarna Bank AB (publ)
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

%% @private
-module(brod_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_create_update_delete_topics/1
        , t_auto_start_producers_for_new_partitions/1
        , t_delete_topics_not_found/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).
-define(TIMEOUT, 280000).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
  case kafka_test_helper:kafka_version() of
    {0, 9} ->
      {skip, "no_topic_management_apis"};
    _ ->
      {ok, _} = application:ensure_all_started(brod),
      Config
  end.

end_per_suite(_Config) ->
  ok.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%%%_* Test functions ===========================================================

t_create_update_delete_topics(Config) when is_list(Config) ->
  Topic = iolist_to_binary(["test-topic-", integer_to_list(erlang:system_time())]),
  TopicConfig = [
    #{
      configs => [],
      num_partitions => 1,
      assignments => [],
      replication_factor => 1,
      name => Topic
    }
  ],
  TopicPartitionConfig = [
    #{
      topic => Topic,
      new_partitions => #{
        count => 2,
        assignment => undefined
      }
    }
  ],
  try
    ?assertEqual(ok,
      brod:create_topics(?HOSTS, TopicConfig, #{timeout => ?TIMEOUT},
        #{connect_timeout => ?TIMEOUT})),

    ?assertEqual(ok,
      brod:create_partitions(?HOSTS, TopicPartitionConfig, #{timeout => ?TIMEOUT},
        #{connect_timeout => ?TIMEOUT}))
  after
    ?assertEqual(ok, brod:delete_topics(?HOSTS, [Topic], ?TIMEOUT,
                                        #{connect_timeout => ?TIMEOUT}))
  end.

t_auto_start_producers_for_new_partitions(Config) when is_list(Config) ->
  Topic = iolist_to_binary(["test-topic-", integer_to_list(erlang:system_time())]),
  TopicConfig = [
    #{
      configs => [],
      num_partitions => 1,
      assignments => [],
      replication_factor => 1,
      name => Topic
    }
  ],
  TopicPartitionConfig = [
    #{
      topic => Topic,
      new_partitions => #{
        count => 2,
        assignment => undefined
      }
    }
  ],
  Client = ?FUNCTION_NAME,
  try
    ?assertEqual(ok,
      brod:create_topics(?HOSTS, TopicConfig, #{timeout => ?TIMEOUT},
        #{connect_timeout => ?TIMEOUT})),

    ok = brod:start_client(?HOSTS, Client,
                           [{metadata_refresh_interval_seconds, 1}]),
    ok = brod:start_producer(Client, Topic, []),
    ?assertMatch({ok, _}, brod:get_producer(Client, Topic, 0)),
    ?assertMatch({error, _}, brod:get_producer(Client, Topic, 1)),

    ?assertEqual(ok,
      brod:create_partitions(?HOSTS, TopicPartitionConfig, #{timeout => ?TIMEOUT},
        #{connect_timeout => ?TIMEOUT})),

    %% the periodic metadata refresh should discover the new partition
    %% and start a producer for it
    ok = wait_for_producer(Client, Topic, 1, 10)
  after
    _ = brod:stop_client(Client),
    ?assertEqual(ok, brod:delete_topics(?HOSTS, [Topic], ?TIMEOUT,
                                        #{connect_timeout => ?TIMEOUT}))
  end.

t_delete_topics_not_found(Config) when is_list(Config) ->
  ?assertEqual({error, unknown_topic_or_partition},
    brod:delete_topics(?HOSTS, [<<"no-such-topic">>], ?TIMEOUT,
      #{connect_timeout => ?TIMEOUT})).

%%%_* Help functions ===========================================================

wait_for_producer(_Client, Topic, Partition, 0) ->
  erlang:error({producer_not_started, Topic, Partition});
wait_for_producer(Client, Topic, Partition, Retries) ->
  case brod:get_producer(Client, Topic, Partition) of
    {ok, Pid} when is_pid(Pid) ->
      ok;
    {error, _} ->
      timer:sleep(1000),
      wait_for_producer(Client, Topic, Partition, Retries - 1)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
