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
-module(brod_producer_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

-define(CLIENT, ?MODULE).
-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) ->
  Producer = {?TOPIC, []},
  case whereis(?MODULE) of
    ?undef -> ok;
    Pid_   -> brod:stop_client(Pid_)
  end,
  Pid =
    erlang:spawn(
      fun() ->
        brod:start_link_client(?MODULE, ?HOSTS, _Config = [], [Producer]),
        receive stop ->
          ok = brod:stop_client(?MODULE)
        end
      end),
  [{producer, Pid} | Config].

end_per_testcase(_Case, Config) ->
  {producer, Pid} = lists:keyfind(producer, 1, Config),
  try
    Ref = erlang:monitor(process, Pid),
    Pid ! stop,
    receive
      {'DOWN', Ref, process, Pid, _} -> ok
    end
  catch _ : _ ->
    ok
  end,
  Config.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

t_produce_sync(Config) when is_list(Config) ->
  Partition = 0,
  Key = <<"key">>,
  Value = iolist_to_binary(
            io_lib:format(
              "~p", [calendar:now_to_universal_time(os:timestamp())])),
  {ok, ConsumerPid} = brod:start_link_consumer(?HOSTS, ?TOPIC, Partition),
  Tester = self(),
  Ref = make_ref(),
  Callback = fun(_Offset, K, V) ->
               Tester ! {Ref, K, V}
             end,
  ok = brod:consume(ConsumerPid, Callback, -1),
  ok = brod:produce_sync(?MODULE, ?TOPIC, Partition, Key, Value),
  receive
    {Ref, K, V} ->
      ok = brod:stop_consumer(ConsumerPid),
      ?assertEqual(Key, K),
      ?assertEqual(Value, V)
  after 5000 ->
    erlang:error(timeout)
  end.

%%%_* Help functions ===========================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
