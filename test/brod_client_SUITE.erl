%%%
%%%   Copyright (c) 2015, Klarna AB
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

%%%=============================================================================
%%% @doc
%%% @copyright 2015 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_client_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

-define(CLIENT, ?MODULE).
-define(HOSTS, [{"localhost", 9092}]).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_Case, Config) ->
  Config.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

t_skip_unreachable_endpoint(Config) when is_list(Config) ->
  {ok, Pid} = brod:start_link_client(?CLIENT, [{"badhost", 9092} | ?HOSTS],
                                     _Config = [], _Producers = []),
  ?assert(is_pid(Pid)),
  _Res = brod_client:get_partitions(Pid, <<"some-unknown-topic">>),
  % auto.create.topics.enabled is 'true' in default spotify/kafka container
  % ?assertEqual({error, 'UnknownTopicOrPartitionException'}, _Res),
  Ref = erlang:monitor(process, Pid),
  ok = brod:stop_client(Pid),
  receive
    {'DOWN', Ref, process, Pid, Reason} ->
      ?assertEqual(normal, Reason)
  after 5000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end.

%%%_* Help functions ===========================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
