%%%
%%%   Copyright (c) 2017-2018, Klarna Bank AB (publ)
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

-module(brod_cg_commits_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_set_then_reset/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod.hrl").

-define(CLIENT_ID, ?MODULE).
-define(BROKERS, [{"localhost", 9092}]).
-define(TOPIC1, <<"brod-group-subscriber-1">>).
-define(TOPIC2, <<"brod-group-subscriber-2">>).
-define(TOPIC3, <<"brod-group-subscriber-3">>).
-define(GROUP_ID, list_to_binary(atom_to_list(?MODULE))).

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) ->
  ClientId       = ?CLIENT_ID,
  BootstrapHosts = ?BROKERS,
  ClientConfig =
    case os:getenv("KAFKA_VERSION") of
      "0.9" ++ _ -> [{query_api_versions, false}];
      _ -> []
    end,
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  [{client_config, ClientConfig} | Config].

end_per_testcase(_Case, Config) when is_list(Config) ->
  ok = brod:stop_client(?CLIENT_ID),
  ok.

all() ->
  [F || {F, _A} <- module_info(exports),
        case atom_to_list(F) of
          "t_" ++ _ -> true;
          _         -> false
        end].

%%%_* Test cases ===============================================================

t_set_then_reset(Config) when is_list(Config) ->
  ClientConfig = proplists:get_value(client_config, Config),
  Topic = <<"brod-group-subscriber-1">>,
  Partitions = [0, 1, 2],
  Offsets0 = [{0, 0}, {1, 0}, {2, 0}],
  ok = do_commit(Topic, Offsets0),
  {ok, Rsp0} =
    brod_utils:fetch_committed_offsets(?BROKERS, ClientConfig,
                                       ?GROUP_ID, [{Topic, Partitions}]),
  ok = assert_offsets([{Topic, Offsets0}], Rsp0),
  Offsets1 = [{0, 1}, {1, 1}, {2, 1}],
  ok = do_commit(Topic, Offsets1),
  {ok, Rsp1} =
    brod_utils:fetch_committed_offsets(?BROKERS, ClientConfig,
                                       ?GROUP_ID, [{Topic, Partitions}]),
  ok = assert_offsets([{Topic, Offsets1}], Rsp1),
  ok.

%% assuming ExpectedOffsets are sorted
assert_offsets(ExpectedOffsets, Rsp) ->
  RetrievedOffsets = transform_rsp(Rsp, []),
  ?assertEqual(ExpectedOffsets, RetrievedOffsets).

transform_rsp([], Acc) ->
  lists:keysort(1, Acc);
transform_rsp([Struct | Rest], Acc) ->
  Topic = kpro:find(topic, Struct),
  PartitionRsp = kpro:find(partition_responses, Struct),
  Partitions = transform_rsp_partitions(PartitionRsp, []),
  transform_rsp(Rest, [{Topic, Partitions} | Acc]).

transform_rsp_partitions([], Acc) ->
  lists:keysort(1, Acc);
transform_rsp_partitions([Struct | Rest], Acc) ->
  Partition = kpro:find(partition, Struct),
  Offset = kpro:find(offset, Struct),
  transform_rsp_partitions(Rest, [{Partition, Offset} | Acc]).

do_commit(Topic, Offsets) ->
  Input = [{id, ?GROUP_ID},
           {topic, Topic},
           {offsets, Offsets}],
  {ok, Pid} = brod_cg_commits:start_link(?CLIENT_ID, Input),
  ok = brod_cg_commits:sync(Pid),
  ok = brod_cg_commits:stop(Pid),
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
