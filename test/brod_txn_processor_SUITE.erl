-module(brod_txn_processor_SUITE).

-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

-export([ t_simple_test/1
        , t_broken_test/1
        ]).

-export([ init/2
        , handle_message/4
        ]).

-include_lib("stdlib/include/assert.hrl").

-include("include/brod.hrl").

-define(HOSTS, [{"localhost", 9092}]).

-define(INPUT_TOPIC, <<"brod_txn_subscriber_input">>).
-define(OUTPUT_TOPIC_1, <<"brod_txn_subscriber_output_1">>).
-define(OUTPUT_TOPIC_2, <<"brod_txn_subscriber_output_2">>).
-define(GROUP_ID, <<"group_id_for_testing">>).
-define(PROCESSOR_GROUP_ID, <<"processor_group_id_for_testing">>).
-define(TIMEOUT, 10000).
-define(config(Name), proplists:get_value(Name, Config)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  case kafka_test_helper:kafka_version() of
    {0, Minor} when Minor < 11 ->
      {skip, "no_transaction"};
    _ ->
      {ok, _} = application:ensure_all_started(brod),
      Config
  end.

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

  [ {client, Client}
  , {client_config, ClientConfig} | Config].

end_per_testcase(_Case, Config) ->
  brod:stop_client(?config(client)),
  Config.

all() -> [F || {F, _A} <- module_info(exports),
               case atom_to_list(F) of
                 "t_" ++ _ -> true;
                 _         -> false
               end].

client_config() -> kafka_test_helper:client_config().

rand() ->
  iolist_to_binary([base64:encode(crypto:strong_rand_bytes(8))]).

produce_messages(Client) ->
  ok = brod:start_producer(Client, ?INPUT_TOPIC, []),

  lists:map(fun(_) ->
                  Key = rand(),
                  Value = rand(),
                  {ok, _} = brod:produce_sync_offset(Client, ?INPUT_TOPIC, 0, Key, Value),

                  Key
                end, lists:seq(1, 10)).

receive_messages(ExpectedMessages) ->
  case sets:is_empty(ExpectedMessages) of
    true -> ok;
    false ->
      receive
        {'EXIT', _, _} ->
          receive_messages(ExpectedMessages);
        {_Topic, _Key} = M ->
          receive_messages(sets:del_element(M, ExpectedMessages))
      after
        ?TIMEOUT ->
          {error, timeout}
      end
  end.

t_simple_test(Config) ->
  Client = ?config(client),
  {ok, FetcherPid} = start_fetchers(self(), Client),
  {ok, ProcessorPid} = start_processor(Client),

  ?assertMatch(true, is_process_alive(FetcherPid)),
  ?assertMatch(true, is_process_alive(ProcessorPid)),

  Keys = produce_messages(Client),

  ExpectedMessages = sets:from_list(
                       lists:flatten(
                         lists:map(fun(Key) ->
                                       [{?OUTPUT_TOPIC_1, Key},
                                        {?OUTPUT_TOPIC_2, Key}]
                                   end, Keys))),

  ?assertMatch(ok, receive_messages(ExpectedMessages)),

  ?assertMatch(ok, gen_server:stop(FetcherPid)),
  ?assertMatch(ok, gen_server:stop(ProcessorPid)),

  ok.

t_broken_test(Config) ->

  Client = ?config(client),
  {ok, FetcherPid} = start_fetchers(self(), Client),
  process_flag(trap_exit, true),
  {ok, ProcessorPid} = start_broken_processor(Client),

  ?assertMatch(true, is_process_alive(FetcherPid)),

  Keys = produce_messages(Client),

  ExpectedMessages = sets:from_list(
                       lists:flatten(
                         lists:map(fun(Key) ->
                                       [{?OUTPUT_TOPIC_1, Key},
                                        {?OUTPUT_TOPIC_2, Key}]
                                   end, Keys))),
  process_flag(trap_exit, false),

  ?assertMatch({error, timeout}, receive_messages(ExpectedMessages)),

  ?assertMatch(ok, gen_server:stop(FetcherPid)),
  ?assertMatch(ok, gen_server:stop(ProcessorPid)),

  ok.

start_broken_processor(Client) ->
  brod:txn_do(
    fun(Transaction, #kafka_message_set{ topic     = _Topic
                                       , partition = Partition
                                       , messages  = Messages
                                       } = _MessageSet) ->
        lists:foreach(fun(#kafka_message{ key = Key
                                        , value = Value
                                        }) ->
                          brod:txn_produce(Transaction,
                                           ?OUTPUT_TOPIC_1,
                                           Partition,
                                           [#{ key => Key
                                             , value => Value
                                             }]),

                          brod:txn_produce(Transaction,
                                           ?OUTPUT_TOPIC_2,
                                           Partition,
                                           [#{ key => Key
                                             , value => Value
                                             }]),
                          %% this should break a few things .)
                          false = is_process_alive(self())
                      end, Messages),
        ok
    end, Client, #{ topics => [?INPUT_TOPIC]
                  , group_id => ?PROCESSOR_GROUP_ID}).

start_processor(Client) ->
  brod:txn_do(
    fun(Transaction, #kafka_message_set{ topic     = _Topic
                                       , partition = Partition
                                       , messages  = Messages
                                       } = _MessageSet) ->

        lists:foreach(fun(#kafka_message{ key = Key
                                        , value = Value}) ->
                          brod:txn_produce(Transaction,
                                           ?OUTPUT_TOPIC_1,
                                           Partition,
                                           [#{ key => Key
                                             , value => Value
                                             }]),

                          brod:txn_produce(Transaction,
                                           ?OUTPUT_TOPIC_2,
                                           Partition,
                                           [#{ key => Key
                                             , value => Value
                                             }])
                      end, Messages),
        ok
    end, Client, #{ topics => [?INPUT_TOPIC]
                  , group_id => ?GROUP_ID}).


start_fetchers(ObserverPid, Client) ->
  brod:start_link_group_subscriber(Client,
                                   ?GROUP_ID,
                                   [?OUTPUT_TOPIC_1, ?OUTPUT_TOPIC_2],
                                   [],
                                   [{isolation_level, read_committed}],
                                   message_set,
                                   ?MODULE,
                                   #{ client => Client
                                    , observer_pid => ObserverPid}).


%========== group subscriber callbacks
init(GroupId, #{ client := Client
               , observer_pid := ObserverPid}) ->
  {ok, #{ client => Client
        , group_id => GroupId
        , observer_pid => ObserverPid}}.

handle_message(Topic,
               Partition,
               #kafka_message_set{ topic     = Topic
                                 , partition = Partition
                                 , messages  = Messages
                                 },
               #{observer_pid := ObserverPid} = State) ->

  lists:foreach(fun(#kafka_message{key = Key}) ->
                  ObserverPid ! {Topic, Key}
                end, Messages),

  {ok, ack, State}.
