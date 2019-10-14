%%%
%%%   Copyright (c) 2019, Klarna Bank AB (publ)
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
-export([ t_create_topics/1
        , t_delete_topics/1
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
  case os:getenv("KAFKA_VERSION") of
    "0.9" -> {skip,
      "The given Kafka test image does not have support for these apis"};
    _ -> Config
  end.

end_per_suite(_Config) ->
  ok.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%%%_* Test functions ===========================================================

t_create_topics(Config) when is_list(Config) ->
  Topic = <<"test-create-topic">>,
  TopicConfig = [
    #{
      config_entries => [],
      num_partitions => 1,
      replica_assignment => [],
      replication_factor => 1,
      topic => Topic
    }
  ],
  ?assertEqual(ok,
    brod:create_topics(?HOSTS, TopicConfig, #{timeout => ?TIMEOUT},
      #{connect_timeout => ?TIMEOUT})).

t_delete_topics(Config) when is_list(Config) ->
  ?assertEqual(ok, brod:delete_topics(?HOSTS, [?TOPIC], ?TIMEOUT,
    #{connect_timeout => ?TIMEOUT})).

t_delete_topics_not_found(Config) when is_list(Config) ->
  ?assertEqual({error, unknown_topic_or_partition},
    brod:delete_topics(?HOSTS, [<<"no-such-topic">>], ?TIMEOUT,
      #{connect_timeout => ?TIMEOUT})).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
