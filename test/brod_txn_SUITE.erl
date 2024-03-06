-module(brod_txn_SUITE).
%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

-export([ t_multiple_writes_transaction/1
        , t_simple_transaction/1
        , t_abort_transaction/1
        , t_batch_transaction/1
        ]).

-include_lib("stdlib/include/assert.hrl").

-include("include/brod.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC_1, list_to_binary(atom_to_list(?MODULE)++"_1")).
-define(TOPIC_2, list_to_binary(atom_to_list(?MODULE)++"_2")).
-define(TIMEOUT, 280000).
-define(config(Name), proplists:get_value(Name, Config)).

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
  brod:stop_client(Client),
  ClientConfig = client_config(),
  ok = brod:start_client(?HOSTS, Client, ClientConfig),
  TesterPid = self(),
  Subscriber = spawn_link(fun() -> subscriber_loop(TesterPid) end),
  Topics = [?TOPIC_1, ?TOPIC_2],
  lists:foreach(fun(Topic) ->
                    ok = brod:start_consumer(Client, Topic, []),
                    brod:subscribe(Client, Subscriber, Topic, 0, [])
                end, Topics),

  [{client, Client},
   {client_config, ClientConfig},
   {topics, Topics} | Config].

end_per_testcase(_Case, Config) ->
  Subscriber = ?config(subscriber),
  is_pid(Subscriber) andalso unlink(Subscriber),
  is_pid(Subscriber) andalso exit(Subscriber, kill),
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

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

subscriber_loop(TesterPid) ->
  receive
    {ConsumerPid, KMS} ->
      #kafka_message_set{ messages = Messages
                        , partition = Partition} = KMS,
      lists:foreach(fun(#kafka_message{offset = Offset, key = K, value = V}) ->
                        TesterPid ! {Partition, K, V},
                        ok = brod:consume_ack(ConsumerPid, Offset)
                    end, Messages),
      subscriber_loop(TesterPid);
    Msg ->
      ct:fail("unexpected message received by test subscriber.\n~p", [Msg])
  end.

receive_messages(none) ->
  receive
    {_Partition, _K, _V} = M -> {unexpected_message, M}
  after 1000 -> ok
  end;

receive_messages(ExpectedMessages) ->
  case sets:is_empty(ExpectedMessages) of
    true -> ok;
    _ ->
      receive
        {_Partition, _K, _V} = M ->
          case sets:is_element(M, ExpectedMessages) of
            false -> {unexpected_message, M};
            true ->
              receive_messages(sets:del_element(M, ExpectedMessages))
          end
      after ?TIMEOUT ->
              {still_waiting_for, ExpectedMessages}
      end
  end.

rand() -> base64:encode(crypto:strong_rand_bytes(8)).

t_simple_transaction(Config) when is_list(Config) ->

  {ok, Tx} = brod:transaction(?config(client), <<"transaction-id">>, []),
  ?assertMatch(true, is_process_alive(Tx)),

  Results = lists:map(fun(Topic) ->
                          Partition = 0,
                          Key = rand(),
                          Value = rand(),
                          {ok, _Offset} = brod:txn_produce(Tx, Topic, Partition, Key, Value),
                          {Partition, Key, Value}
                      end, ?config(topics)),

  ?assertMatch(ok, receive_messages(none)),
  ?assertMatch(ok, brod:commit(Tx)),
  ?assertMatch(false, is_process_alive(Tx)),
  ?assertMatch(ok, receive_messages(sets:from_list(Results))),
  ?assertMatch(ok, receive_messages(none)),
  ok.

t_batch_transaction(Config) when is_list(Config) ->

  {ok, Tx} = brod:transaction(?config(client), <<"transaction-id">>, []),
  ?assertMatch(true, is_process_alive(Tx)),

  Results =
  lists:flatten(lists:map(fun(Topic) ->
                Batch = lists:map(
                          fun(_) ->
                              #{key => rand()
                              , value => rand()
                              , ts => kpro_lib:now_ts()}
                          end, lists:seq(1, 10)),

                Partition = 0,
                {ok, _Offset} = brod:txn_produce(Tx, Topic, Partition, Batch),

                lists:map(fun(#{key := Key
                              , value := Value}) ->
                              {Partition, Key, Value}
                          end, Batch)

            end, ?config(topics))),

  ?assertMatch(ok, receive_messages(none)),
  ?assertMatch(ok, brod:commit(Tx)),
  ?assertMatch(false, is_process_alive(Tx)),
  ?assertMatch(ok, receive_messages(sets:from_list(Results))),
  ?assertMatch(ok, receive_messages(none)),
  ok.

t_abort_transaction(Config) when is_list(Config) ->

  {ok, Tx} = brod:transaction(?config(client), <<"transaction-id">>, []),
  ?assertMatch(true, is_process_alive(Tx)),

  _ = lists:map(fun(Topic) ->
                    Partition = 0,
                    Key = rand(),
                    Value = rand(),
                    {ok, _Offset} = brod:txn_produce(Tx, Topic, Partition, Key, Value),
                    {Partition, Key, Value}
                end, ?config(topics)),

  ?assertMatch(ok, receive_messages(none)),
  ?assertMatch(ok, brod:abort(Tx)),
  ?assertMatch(false, is_process_alive(Tx)),
  ?assertMatch(ok, receive_messages(none)),
  ok.

t_multiple_writes_transaction(Config) when is_list(Config) ->

  {ok, Tx} = brod:transaction(?config(client), <<"transaction-id">>, []),
  ?assertMatch(true, is_process_alive(Tx)),

  FirstWave = lists:map(fun(Topic) ->
                            Partition = 0,
                            Key = rand(),
                            Value = rand(),
                            {ok, _Offset} = brod:txn_produce(Tx, Topic, Partition, Key, Value),
                            {Partition, Key, Value}
                        end, ?config(topics)),

  SecondWave = lists:map(fun(Topic) ->
                             Partition = 0,
                             Key = rand(),
                             Value = rand(),
                             {ok, _Offset} = brod:txn_produce(Tx, Topic, Partition, Key, Value),
                             {Partition, Key, Value}
                         end, ?config(topics)),

  Results = lists:append(FirstWave, SecondWave),

  ?assertMatch(ok, receive_messages(none)),
  ?assertMatch(ok, brod:commit(Tx)),
  ?assertMatch(false, is_process_alive(Tx)),
  ?assertMatch(ok, receive_messages(sets:from_list(Results))),
  ?assertMatch(ok, receive_messages(none)),
  ok.

