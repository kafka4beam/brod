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

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_produce_sync/1
        , t_produce_async/1
        , t_producer_topic_not_found/1
        , t_producer_partition_not_found/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

subscriber_loop(Client, TesterPid) ->
  receive
    {ConsumerPid, #kafka_message_set{messages = Messages}} ->
      lists:foreach(fun(#kafka_message{offset = Offset, key = K, value = V}) ->
                      TesterPid ! {K, V},
                      ok = brod:consume_ack(ConsumerPid, Offset)
                    end, Messages),
      subscriber_loop(Client, TesterPid);
    Msg ->
      ct:fail("unexpected message received by test subscriber.\n~p", [Msg])
  end.

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  Client = Case,
  Topic = ?TOPIC,
  Partition = 0,
  Producer = {Topic, []},
  Consumer = {Topic, []},
  case whereis(Client) of
    ?undef -> ok;
    Pid_   -> brod:stop_client(Pid_)
  end,
  TesterPid = self(),
  {ok, ClientPid} = brod:start_link_client(Client, ?HOSTS,
                                           [Producer], [Consumer], []),
  Subscriber = spawn_link(fun() -> subscriber_loop(Client, TesterPid) end),
  {ok, _ConsumerPid} = brod:subscribe(Client, Subscriber, Topic, Partition, []),
  [{client, Client}, {client_pid, ClientPid},
   {subscriber, Subscriber} | Config].

end_per_testcase(_Case, Config) ->
  Subscriber = ?config(subscriber),
  unlink(Subscriber),
  exit(Subscriber, kill),
  Pid = ?config(client_pid),
  try
    Ref = erlang:monitor(process, Pid),
    brod:stop_client(Pid),
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
  Client = ?config(client),
  Partition = 0,
  {K1, V1} = make_unique_kv(),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, K1, V1),
  {K2, V2} = make_unique_kv(),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, K2, V2),
  ReceiveFun =
    fun(ExpectedK, ExpectedV) ->
      receive
        {K, V} ->
          ?assertEqual(ExpectedK, K),
          ?assertEqual(ExpectedV, V)
        after 5000 ->
          ct:fail({?MODULE, ?LINE, timeout, ExpectedK, ExpectedV})
      end
    end,
  ReceiveFun(K1, V1),
  ReceiveFun(K2, V2).

t_produce_async(Config) when is_list(Config) ->
  Client = ?config(client),
  Partition = 0,
  {Key, Value} = make_unique_kv(),
  {ok, CallRef} = brod:produce(Client, ?TOPIC, Partition, Key, Value),
  receive
    #brod_produce_reply{ call_ref = CallRef
                       , result   = brod_produce_req_acked
                       } ->
      ok
  after 5000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end,
  receive
    {K, V} ->
      ?assertEqual(Key, K),
      ?assertEqual(Value, V)
  after 5000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end.

t_producer_topic_not_found(Config) when is_list(Config) ->
  Client = ?config(client),
  ?assertEqual({error, {producer_not_found, <<"no-such-topic">>}},
               brod:produce(Client, <<"no-such-topic">>, 0, <<"k">>, <<"v">>)).


t_producer_partition_not_found(Config) when is_list(Config) ->
  Client = whereis(t_producer_partition_not_found),
  ?assert(is_pid(Client)),
  ?assertEqual({error, {producer_not_found, ?TOPIC, 100}},
               brod:produce(Client, ?TOPIC, 100, <<"k">>, <<"v">>)).

%%%_* Help functions ===========================================================

%% os:timestamp should be unique enough for testing
make_unique_kv() ->
  { iolist_to_binary(["key-", make_ts_str()])
  , iolist_to_binary(["val-", make_ts_str()])
  }.

make_ts_str() ->
  Ts = os:timestamp(),
  {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
