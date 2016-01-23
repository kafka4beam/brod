%%%
%%%   Copyright (c) 2015, 2016, Klarna AB
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
%%% @copyright 20150-2016 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_consumer_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  Client = Case,
  Topic = ?TOPIC,
  Producer = {Topic, []},
  Consumer = {Topic, []},
  case whereis(Client) of
    ?undef -> ok;
    Pid_   -> brod:stop_client(Pid_)
  end,
  {ok, ClientPid} = brod:start_link_client(Client, ?HOSTS, [Producer], [Consumer], []),
  [{client, Client}, {client_pid, ClientPid} | Config].

end_per_testcase(_Case, Config) ->
  Pid = ?config(client_pid),
  try
    Ref = erlang:monitor(process, Pid),
    brod:stop_client(Pid),
    receive
      {'DOWN', Ref, process, Pid, _} -> ok
    end
  catch _ : _ ->
    ok
  end.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

%% consumer should be smart enough to try greater max_bytes to fetch message set
t_max_bytes_too_small(Config) ->
  Client = ?config(client_pid),
  Partition = 0,
  Key = make_unique_key(),
  Value = make_bytes(2000),
  Options = [{max_bytes, 1500}],
  {ok, ConsumerPid} =
    brod:subscribe(Client, self(), ?TOPIC, Partition, Options),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  receive
    {ConsumerPid, #kafka_message_set{messages = Messages}} ->
      [#kafka_message{key = KeyReceived}] = Messages,
      ?assertEqual(Key, KeyReceived);
    Msg ->
      ct:fail("unexpected message received:\n~p", [Msg])
  end.

%%%_* Help functions ===========================================================

%% os:timestamp should be unique enough for testing
make_unique_key() ->
  iolist_to_binary(["key-", make_ts_str()]).

make_ts_str() ->
  Ts = os:timestamp(),
  {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

make_bytes(Bytes) ->
  iolist_to_binary(lists:duplicate(Bytes, 0)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
