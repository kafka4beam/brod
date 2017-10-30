%%%
%%%   Copyright (c) 2017, Klarna AB
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

start_stop_test() ->
  _ = application:stop(brod), %% other tests might have it started
  {ok, _Pid} = brod_kafka_apis:start_link(),
  ?assert(lists:member(brod_kafka_apis, ets:all())),
  ok = brod_kafka_apis:stop().

only_one_version_test() ->
  ?assertEqual(0, brod_kafka_apis:pick_version(pid, list_groups_request)).

pick_min_version_test() ->
  %% use min version when ther is no versions received from broker
  {ok, Pid} = brod_kafka_apis:start_link(),
  ?assertEqual(0, brod_kafka_apis:pick_version(Pid, produce_request)),
  ok = brod_kafka_apis:stop().

pick_brod_max_version_test() ->
  %% brod supports max = 2, kafka supports max = 7
  {ok, _Pid} = brod_kafka_apis:start_link(),
  Sock = self(), %% faking it
  versions_received(client, Sock, [{produce_request, {0, 7}}]),
  ?assertEqual(2, brod_kafka_apis:pick_version(Sock, produce_request)),
  ok = brod_kafka_apis:stop().

pick_kafka_max_version_test() ->
  %% brod supports max = 2, kafka supports max = 1
  {ok, _Pid} = brod_kafka_apis:start_link(),
  Sock = self(), %% faking it
  versions_received(client, Sock, [{produce_request, {0, 1}}]),
  ?assertEqual(1, brod_kafka_apis:pick_version(Sock, produce_request)),
  ok = brod_kafka_apis:stop().

pick_min_brod_version_test() ->
  %% no versions received from kafka
  {ok, _Pid} = brod_kafka_apis:start_link(),
  Sock = self(), %% faking it
  ?assertEqual(0, brod_kafka_apis:pick_version(Sock, produce_request)),
  ok = brod_kafka_apis:stop().

pick_min_brod_version_2_test() ->
  %% no versions received from kafka
  {ok, _Pid} = brod_kafka_apis:start_link(),
  Sock = self(), %% faking it
  versions_received(client, Sock, [{fetch_request, {0, 0}}]),
  ?assertEqual(0, brod_kafka_apis:pick_version(Sock, produce_request)),
  ok = brod_kafka_apis:stop().

no_overlapping_version_range_test() ->
  %% brod supports 0 - 2, kafka supports 6 - 7
  {ok, _Pid} = brod_kafka_apis:start_link(),
  Sock = self(), %% faking it
  versions_received(client, Sock, [{produce_request, {6, 7}}]),
  ?assertEqual(0, brod_kafka_apis:pick_version(Sock, produce_request)),
  ok = brod_kafka_apis:stop().

add_sock_pid_test() ->
  {ok, _Pid} = brod_kafka_apis:start_link(),
  Sock1 = spawn(fun() -> receive after infinity -> ok end end),
  Sock2 = spawn(fun() -> receive after infinity -> ok end end),
  Versions = [{produce_request, {2, 3}}],
  ?assertEqual({error, unknown_host},
               brod_kafka_apis:maybe_add_sock_pid(host, Sock1)),
  brod_kafka_apis:versions_received(client, Sock1, Versions, host),
  ok = brod_kafka_apis:maybe_add_sock_pid(host, Sock2),
  ?assertEqual(2, brod_kafka_apis:pick_version(Sock1, produce_request)),
  ?assertEqual(2, brod_kafka_apis:pick_version(Sock2, produce_request)),
  ok = brod_kafka_apis:stop().

versions_received(Client, SockPid, Versions) ->
  brod_kafka_apis:versions_received(Client, SockPid, Versions, host).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
