-module(brod_offset_txn_SUITE).

-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

-export([ t_simple_test/1
        , t_no_commit_test/1
        ]).

-export([ init/2
        , handle_message/4
        , get_committed_offsets/3
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("include/brod.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC_OUTPUT_1, <<"brod_txn_subscriber_output_1">>).
-define(TOPIC_OUTPUT_2, <<"brod_txn_subscriber_output_2">>).
-define(TOPIC_INPUT, <<"brod_txn_subscriber_input">>).
-define(CLIENT_ID, client_consumer_group).
-define(GROUP_ID, <<"group_id_for_testing">>).
-define(TIMEOUT, 4000).
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

  [ {client, Client}
  , {client_config, ClientConfig} | Config].

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

init(GroupId,
     #{ client := Client
      , observer := OPid}) ->
  {ok, #{ client => Client
        , observer => OPid
        , group_id => GroupId}}.

handle_message(Topic,
               Partition,
               #kafka_message{ offset = Offset
                             , key = Key
                             , value = Value},
               #{ client := Client
                , group_id := GroupId
                , observer := ObserverPid} =  State) ->

  {ok, Tx} = brod:transaction(Client, <<"some_transaction">>, []),
  {ok, _} = brod:txn_produce(Tx, ?TOPIC_OUTPUT_1, Partition, Key, Value),
  {ok, _} = brod:txn_produce(Tx, ?TOPIC_OUTPUT_2, Partition, Key, Value),
  ok = brod:txn_add_offsets(Tx, GroupId, #{{Topic, Partition} => Offset}),

  case Value of
    <<"no_commit">> ->
      ok = brod:abort(Tx);
    _ ->
      ok = brod:commit(Tx)
  end,

  ObserverPid ! {offset, Offset},

  {ok, ack_no_commit, State}.

get_committed_offsets(GroupId, TPs, #{client := Client} = State) ->
  {ok, Offsets} = brod:fetch_committed_offsets(Client, GroupId),
  TPOs =
  lists:filter(fun({TP, _Offset}) ->
                   lists:member(TP, TPs)
               end,
               lists:foldl(fun(#{ name := Topic
                                , partitions := Partitions}, TPOs) ->
                               lists:append(TPOs,
                                            lists:map(fun(#{ committed_offset := COffset
                                                           , partition_index := Partition}) ->
                                                          {{Topic, Partition}, COffset}
                                                      end, Partitions))
                           end, [], Offsets)),
  {ok,
   TPOs,
   State}.

get_offset() ->
  timer:sleep(100),
  case get_committed_offsets(?GROUP_ID,
                             [{?TOPIC_INPUT, 0}],
                             #{client => ?CLIENT_ID}) of

    {ok, [{{?TOPIC_INPUT, 0}, Offset}], _} -> Offset;
    {ok, [], _} -> 0
  end.

send_no_commit_message() ->
  send_message(rand(), <<"no_commit">>).

send_simple_message() ->
  send_message(rand(), <<"simple">>).

send_message(Key, Value) ->
  brod:start_producer(?CLIENT_ID, ?TOPIC_INPUT, []),
  {ok, Offset} = brod:produce_sync_offset(?CLIENT_ID, ?TOPIC_INPUT, 0, Key, Value),
  Offset.

start_subscriber() ->
  GroupConfig = [{offset_commit_policy, consumer_managed}],

  ConsumerConfig = [ {prefetch_count, 3}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 100}
                   , {partition_restart_delay_seconds, 1}
                   , {begin_offset, 0}
                   ],

  brod:start_link_group_subscriber(?CLIENT_ID,
                                   ?GROUP_ID,
                                   [?TOPIC_INPUT],
                                   GroupConfig,
                                   ConsumerConfig,
                                   message,
                                   ?MODULE,
                                   #{ client => ?CLIENT_ID
                                    , observer => self()}).

wait_to_last() ->
  receive
    _ -> wait_to_last()
  after ?TIMEOUT -> done
  end.

wait_for_offset(ExpectedOffset) ->
  receive
    {offset, Offset} when Offset == ExpectedOffset ->
      done;
    {offset, _UnexpectedOffset} ->
      wait_for_offset(ExpectedOffset)
  after ?TIMEOUT -> timeout
  end.

rand() -> base64:encode(crypto:strong_rand_bytes(8)).

t_simple_test(Config) when is_list(Config)  ->
  ok = brod:start_client(?HOSTS, ?CLIENT_ID, []),
  {ok, SubscriberPid} = start_subscriber(),
  done = wait_to_last(),
  InitialOffset = get_offset(),
  {ok, OffsetOutput1} = brod:resolve_offset(?HOSTS, ?TOPIC_OUTPUT_1, 0),
  {ok, OffsetOutput2} = brod:resolve_offset(?HOSTS, ?TOPIC_OUTPUT_2, 0),
  MessageOffset = send_simple_message(),
  done = wait_for_offset(MessageOffset),
  CurrentOffset = get_offset(),
  ?assertMatch(MessageOffset, CurrentOffset),

  ?assertMatch(true, InitialOffset =< CurrentOffset),
  ok = brod_group_subscriber:stop(SubscriberPid),
  ?assertMatch(false, is_process_alive(SubscriberPid)),

  {ok, {_, MessagesO1}} = brod:fetch(?CLIENT_ID,
                                     ?TOPIC_OUTPUT_1,
                                     0, OffsetOutput1,
                                     #{isolation_level => read_committed}),
  ?assertMatch(1, length(MessagesO1)),

  {ok, {_, MessagesO2}} = brod:fetch(?CLIENT_ID,
                                     ?TOPIC_OUTPUT_2,
                                     0, OffsetOutput2,
                                     #{isolation_level => read_committed}),
  ?assertMatch(1, length(MessagesO2)),
  ok.

t_no_commit_test(Config) when is_list(Config) ->
  ok = brod:start_client(?HOSTS, ?CLIENT_ID, []),
  {ok, SubscriberPid} = start_subscriber(),
  wait_to_last(),
  {ok, OutputOffset1} = brod:resolve_offset(?HOSTS, ?TOPIC_OUTPUT_1, 0),
  {ok, OutputOffset2} = brod:resolve_offset(?HOSTS, ?TOPIC_OUTPUT_2, 0),
  InitialOffset = send_no_commit_message(),
  done = wait_for_offset(InitialOffset),
  CurrentOffset = get_offset(),

  ?assertMatch(true, InitialOffset >= CurrentOffset),
  ok = brod_group_subscriber:stop(SubscriberPid),
  false = is_process_alive(SubscriberPid),

  {ok, {_, MessagesO1}} = brod:fetch(?CLIENT_ID,
                                     ?TOPIC_OUTPUT_1,
                                     0, OutputOffset1,
                                     #{isolation_level => read_committed}),
  ?assertMatch(0, length(MessagesO1)),

  {ok, {_, MessagesO2}} = brod:fetch(?CLIENT_ID,
                                     ?TOPIC_OUTPUT_2,
                                     0, OutputOffset2,
                                     #{isolation_level => read_committed}),
  ?assertMatch(0, length(MessagesO2)),
  ok.
