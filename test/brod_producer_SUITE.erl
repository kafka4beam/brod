%%%
%%%   Copyright (c) 2015-2017, Klarna AB
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
%%% @copyright 2015-2017 Klarna AB
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
        , t_produce_no_ack/1
        , t_produce_async/1
        , t_producer_topic_not_found/1
        , t_producer_partition_not_found/1
        , t_produce_partitioner/1
        , t_produce_batch/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

subscriber_loop(Client, TesterPid) ->
  receive
    {ConsumerPid, KMS} ->
      #kafka_message_set{ messages = Messages
                        , partition = Partition} = KMS,
      lists:foreach(fun(#kafka_message{offset = Offset, key = K, value = V}) ->
                      TesterPid ! {Partition, K, V},
                      ok = brod:consume_ack(ConsumerPid, Offset)
                    end, Messages),
      subscriber_loop(Client, TesterPid);
    Msg ->
      ct:fail("unexpected message received by test subscriber.\n~p", [Msg])
  end.

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  try ?MODULE:Case({'init', Config})
  catch error : function_clause ->
    init_client(Case, Config)
  end.

init_client(Case, Config) ->
  Client = Case,
  Topic = ?TOPIC,
  brod:stop_client(Client),
  TesterPid = self(),
  ok = brod:start_client(?HOSTS, Client),
  ok = brod:start_producer(Client, Topic, []),
  ok = brod:start_consumer(Client, Topic, []),
  Subscriber = spawn_link(fun() -> subscriber_loop(Client, TesterPid) end),
  {ok, _ConsumerPid1} = brod:subscribe(Client, Subscriber, Topic, 0, []),
  {ok, _ConsumerPid2} = brod:subscribe(Client, Subscriber, Topic, 1, []),
  [{client, Client}, {subscriber, Subscriber} | Config].

end_per_testcase(_Case, Config) ->
  Subscriber = ?config(subscriber),
  unlink(Subscriber),
  exit(Subscriber, kill),
  Pid = whereis(?config(client)),
  try
    Ref = erlang:monitor(process, Pid),
    brod:stop_client(?config(client)),
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
        {_, K, V} ->
          ?assertEqual(ExpectedK, K),
          ?assertEqual(ExpectedV, V)
        after 5000 ->
          ct:fail({?MODULE, ?LINE, timeout, ExpectedK, ExpectedV})
      end
    end,
  ReceiveFun(K1, V1),
  ReceiveFun(K2, V2).

t_produce_no_ack({init, Config}) ->
  Client = t_produce_no_ack,
  Topic = ?TOPIC,
  case whereis(Client) of
    ?undef -> ok;
    Pid_   -> brod:stop_client(Pid_)
  end,
  TesterPid = self(),
  {ok, ClientPid} = brod:start_link_client(?HOSTS, Client),
  ok = brod:start_producer(Client, Topic, [{required_acks, 0}]),
  ok = brod:start_consumer(Client, Topic, []),
  Subscriber = spawn_link(fun() -> subscriber_loop(Client, TesterPid) end),
  {ok, _ConsumerPid1} = brod:subscribe(Client, Subscriber, Topic, 0, []),
  {ok, _ConsumerPid2} = brod:subscribe(Client, Subscriber, Topic, 1, []),
  [{client, Client}, {client_pid, ClientPid},
   {subscriber, Subscriber} | Config];
t_produce_no_ack(Config) when is_list(Config) ->
  Client = ?config(client),
  Partition = 0,
  {K1, V1} = make_unique_kv(),
  {K2, V2} = make_unique_kv(),
  {ok, Ref1} = brod:produce(Client, ?TOPIC, Partition, K1, V1),
  ok = brod:sync_produce_request(Ref1),
  {ok, Ref2} = brod:produce(Client, ?TOPIC, Partition, K2, V2),
  ok = brod:sync_produce_request(Ref2),
  ReceiveFun =
    fun(ExpectedK, ExpectedV) ->
      receive
        {_, K, V} ->
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
    {_, K, V} ->
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
  Client = whereis(?config(client)),
  ?assertEqual({error, {producer_not_found, ?TOPIC, 100}},
               brod:produce(Client, ?TOPIC, 100, <<"k">>, <<"v">>)).

t_produce_partitioner(Config) when is_list(Config) ->
  Client = ?config(client),
  {K1, V1} = make_unique_kv(),
  {K2, V2} = make_unique_kv(),
  PartFun = fun(Topic, PartitionsCnt, Key, _Value) ->
                ?assertEqual(?TOPIC, Topic),
                ?assertEqual(2, PartitionsCnt),
                case Key of
                  K1 -> {ok, 0};
                  K2 -> {ok, 1}
                end
            end,
  ReceiveFun =
    fun(ExpectedP, ExpectedK, ExpectedV) ->
      receive
        {P, K, V} ->
          ?assertEqual(ExpectedP, P),
          ?assertEqual(ExpectedK, K),
          ?assertEqual(ExpectedV, V)
        after 5000 ->
          ct:fail({?MODULE, ?LINE, timeout, ExpectedP, ExpectedK, ExpectedV})
      end
    end,
  ok = brod:produce_sync(Client, ?TOPIC, PartFun, K1, V1),
  ReceiveFun(0, K1, V1),
  ok = brod:produce_sync(Client, ?TOPIC, PartFun, K2, V2),
  ReceiveFun(1, K2, V2).

t_produce_batch(Config) when is_list(Config) ->
  Client = ?config(client),
  Partition = 0,
  {K1, V1} = make_unique_kv(),
  {K2, V2} = make_unique_kv(),
  {K3, V3} = make_unique_kv(),
  Batch = [{K1, V1}, {K2, V2}, {<<>>, [{K3, V3}]}],
  ok = brod:produce_sync(Client, ?TOPIC, Partition, undefined, Batch),
  ReceiveFun =
    fun(ExpectedK, ExpectedV) ->
      receive
        {_, K, V} ->
          ?assertEqual(ExpectedK, K),
          ?assertEqual(ExpectedV, V)
        after 5000 ->
          ct:fail({?MODULE, ?LINE, timeout, ExpectedK, ExpectedV})
      end
    end,
  ReceiveFun(K1, V1),
  ReceiveFun(K2, V2),
  ReceiveFun(K3, V3).


%%%_* Help functions ===========================================================

%% os:timestamp should be unique enough for testing
make_unique_kv() ->
  { iolist_to_binary(["key-", make_ts_str()])
  , iolist_to_binary(["val-", make_ts_str()])
  }.

make_ts_str() -> brod_utils:os_time_utc_str().

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
