%%%
%%%   Copyright (c) 2017-2021 Klarna Bank AB (publ)
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

pick_brod_max_version_test() ->
  %% brod supports max = 7, kafka supports max = 100
  ?WITH_MECK(#{produce => {0, 100}},
             ?assertEqual(7, brod_kafka_apis:pick_version(self(), produce))).

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
  %% brod supports 0 - 11, kafka supports 80 - 90
  ?WITH_MECK(#{produce => {80, 90}},
             ?assertError({unsupported_vsn_range, _, _, _},
                          brod_kafka_apis:pick_version(self(), produce))).

pick_static_membership_versions_test() ->
  Versions =
    #{ sync_group => {0, 4}
     , offset_commit => {0, 8}
     , heartbeat => {0, 4}
     },
  ?WITH_MECK(
    Versions,
    begin
      ?assertEqual(3, brod_kafka_apis:pick_version(self(), sync_group)),
      ?assertEqual(7, brod_kafka_apis:pick_version(self(), offset_commit)),
      ?assertEqual(4, brod_kafka_apis:pick_version(self(), heartbeat))
    end).

supports_version_test() ->
  Versions =
    #{ join_group => {0, 6}
     , sync_group => {0, 4}
     , offset_commit => {0, 8}
     , heartbeat => {0, 4}
     },
  ?WITH_MECK(
    Versions,
    begin
      ?assert(brod_kafka_apis:supports_version(self(), join_group, 5)),
      ?assert(brod_kafka_apis:supports_version(self(), sync_group, 3)),
      ?assert(brod_kafka_apis:supports_version(self(), offset_commit, 7)),
      ?assert(brod_kafka_apis:supports_version(self(), heartbeat, 3)),
      ?assertNot(brod_kafka_apis:supports_version(self(), sync_group, 4))
    end).

unsupported_or_unknown_version_test() ->
  ?WITH_MECK(
    #{ heartbeat => {0, 2}
     , offset_commit => {0, 8}
     },
    begin
      ?assertNot(brod_kafka_apis:supports_version(self(), heartbeat, 3)),
      ?assertNot(brod_kafka_apis:supports_version(self(), sync_group, 3)),
      ?assertNot(brod_kafka_apis:supports_version(self(), offset_commit, 8))
    end).

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
