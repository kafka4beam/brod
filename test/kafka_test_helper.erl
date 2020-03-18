-module(kafka_test_helper).

-export([ init_per_suite/1
        , common_init_per_testcase/3
        , common_end_per_testcase/2
        , produce/2
        , produce/3
        , produce/4
        , create_topic/3
        , get_acked_offsets/1
        , check_committed_offsets/2
        , wait_n_messages/3
        , wait_n_messages/2
        , wait_for_port/2
        , consumer_config/0
        ]).

-define(CLIENT_ID, brod_test_client).
-include("brod_test_macros.hrl").

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  [ {proper_timeout, 10000}
  | Config].

common_init_per_testcase(Module, Case, Config) ->
  %% Create a client and a producer for putting test data to Kafka.
  %% By default name of the test topic is equal to the name of
  %% testcase.
  {ok, _} = application:ensure_all_started(brod),
  Topics = try Module:Case(topics) of
               L -> L
           catch
             _:_ -> [{?topic(Case, 1), 1}]
           end,
  [prepare_topic(I) || I <- Topics],
  Config.

common_end_per_testcase(Case, Config) ->
  brod:stop_client(?CLIENT_ID),
  application:stop(brod).

produce(TopicPartition, Value) ->
  produce(TopicPartition, <<>>, Value).

produce(TopicPartition, Key, Value) ->
  produce(TopicPartition, Key, Value, []).

produce({Topic, Partition}, Key, Value, Headers) ->
  ?tp(test_topic_produce, #{ topic     => Topic
                           , partition => Partition
                           , key       => Key
                           , value     => Value
                           , headers   => Headers
                           }),
  {ok, Offset} = brod:produce_sync_offset( ?CLIENT_ID
                                         , Topic
                                         , Partition
                                         , <<>>
                                         , [#{value => Value, key => Key, headers => Headers}]
                                         ),
  Offset.

prepare_topic(Topic) when is_binary(Topic) ->
  prepare_topic({Topic, 1});
prepare_topic({Topic, NumPartitions}) ->
  prepare_topic({Topic, NumPartitions, 2});
prepare_topic({Topic, NumPartitions, NumReplicas}) ->
  ok = create_topic(Topic, NumPartitions, NumReplicas),
  case brod:start_client(bootstrap_hosts(), ?CLIENT_ID, []) of
    ok -> ok;
    {error, already_started} -> ok
  end,
  ok = brod:start_producer(?CLIENT_ID, Topic, _ProducerConfig = []).

create_topic(Name, NumPartitions, NumReplicas) ->
  FMT = "/opt/kafka/bin/kafka-topics.sh --zookeeper localhost "
        " --create --partitions ~p --replication-factor ~p"
        " --topic ~s --config min.insync.replicas=1",
  CMD = lists:flatten(io_lib:format(FMT, [NumPartitions, NumReplicas, Name])),
  CMD1 = "docker exec kafka-1 bash -c '" ++ CMD ++ "'",
  Result = os:cmd(CMD1),
  ?log(notice, "Creating Kafka topic: ~s~nResult: ~p", [CMD1, Result]).

-spec get_acked_offsets(brod:group_id()) -> #{brod:partition() => brod:offset()}.
get_acked_offsets(GroupId) ->
  {ok, [#{partition_responses := Resp}]} =
    brod:fetch_committed_offsets(?CLIENT_ID, GroupId),
  Fun = fun(#{partition := P, offset := O}, Acc) ->
            Acc #{P => O}
        end,
  lists:foldl(Fun, #{}, Resp).

%% Validate offsets committed to Kafka:
check_committed_offsets(GroupId, Offsets) ->
  CommittedOffsets = get_acked_offsets(GroupId),
  lists:foreach( fun({TopicPartition, Offset}) ->
                     %% Explanation for + 1: brod's `roundrobin_v2'
                     %% protocol keeps _first unprocessed_ offset
                     %% rather than _last processed_. And this is
                     %% confusing.
                     ?assertEqual( Offset + 1
                                 , maps:get(TopicPartition, CommittedOffsets, undefined)
                                 )
                 end
               , Offsets
               ).

%% Wait until total number of messages processed by a consumer group
%% becomes equal to the expected value
wait_n_messages(TestGroupId, Expected, NRetries) ->
  ?retry(1000, NRetries,
         begin
           Offsets = get_acked_offsets(TestGroupId),
           NMessages = lists:sum(maps:values(Offsets)),
           ?log( notice
               , "Number of messages processed by consumer group: ~p; total: ~p/~p"
               , [Offsets, NMessages, Expected]
               ),
           ?assert(NMessages >= Expected)
         end).

wait_n_messages(TestGroupId, NMessages) ->
  wait_n_messages(TestGroupId, NMessages, 30).

-spec wait_for_port(string(), non_neg_integer()) -> ok.
wait_for_port(Host, Port) ->
  ?retry(1000, 10,
         begin
           {ok, Sock} = gen_tcp:connect(Host, Port, []),
           gen_tcp:close(Sock),
           ok
         end).

consumer_config() ->
  %% Makes brod restart faster, this will hopefully shave off some
  %% time from failure suites:
  [ {max_wait_time, 500}
  , {sleep_timeout, 100}
  , {max_bytes, 1}
  ].

bootstrap_hosts() ->
  [ {"localhost", 9092}
  ].
