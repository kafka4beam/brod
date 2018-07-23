%%%
%%%   Copyright (c) 2017-2018 Klarna Bank AB (publ)
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

-module(brod_kafka_apis_tests).

-include_lib("eunit/include/eunit.hrl").

-define(WITH_MECK(Versions, EXPR),
        fun() ->
          try
            ok = setup(Versions),
            EXPR
          after
            ok = clear()
          end
        end()).

start_stop_test() ->
  _ = application:stop(brod), %% other tests might have it started
  {ok, _Pid} = brod_kafka_apis:start_link(),
  ?assert(lists:member(brod_kafka_apis, ets:all())),
  ok = brod_kafka_apis:stop().

only_one_version_test() ->
  %% we support only one version, no need to lookup
  ?assertEqual(0, brod_kafka_apis:pick_version(conn, list_groups)).

pick_brod_max_version_test() ->
  %% brod supports max = 5, kafka supports max = 100
  ?WITH_MECK(#{produce => {0, 100}},
             ?assertEqual(5, brod_kafka_apis:pick_version(self(), produce))).

pick_kafka_max_version_test() ->
  %% brod supports max = 2, kafka supports max = 1
  ?WITH_MECK(#{produce => {0, 1}},
             ?assertEqual(1, brod_kafka_apis:pick_version(self(), produce))).

pick_min_brod_version_test() ->
  %% no versions received from kafka
  ?WITH_MECK(#{},
             ?assertEqual(0, brod_kafka_apis:pick_version(self(), produce))).

pick_min_brod_version_2_test() ->
  %% received 'fetch' API version, lookup 'produce'
  ?WITH_MECK(#{fetch => {0, 0}},
             ?assertEqual(0, brod_kafka_apis:pick_version(self(), produce))).

no_version_range_intersection_test() ->
  %% brod supports 0 - 2, kafka supports 6 - 7
  ?WITH_MECK(#{produce => {6, 7}},
             ?assertError({unsupported_vsn_range, _, _, _},
                          brod_kafka_apis:pick_version(self(), produce))).

setup(Versions) ->
  _ = application:stop(brod), %% other tests might have it started
  _ = brod_kafka_apis:start_link(),
  meck:new(kpro, [passthrough, no_passthrough_cover, no_history]),
  meck:expect(kpro, get_api_versions, fun(_) -> {ok, Versions} end),
  ok.

clear() ->
  brod_kafka_apis:stop(),
  meck:unload(kpro),
  ok.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
