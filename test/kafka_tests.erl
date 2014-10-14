%% @private
-module(kafka_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/brod_int.hrl").
-include("src/kafka.hrl").

-define(PORT, 1234).

-define(i8(I),   I:8/integer).
-define(i16(I),  I:16/integer).
-define(i32(I),  I:32/integer).
-define(i16s(I), I:16/signed-integer).
-define(i32s(I), I:32/signed-integer).
-define(i64(I),  I:64/integer).

api_key_test() ->
  ?assertMatch(?API_KEY_METADATA, kafka:api_key(#metadata_request{})),
  ?assertMatch(?API_KEY_PRODUCE, kafka:api_key(#produce_request{})),
  ?assertMatch(?API_KEY_OFFSET, kafka:api_key(#offset_request{})),
  ?assertMatch(?API_KEY_FETCH, kafka:api_key(#fetch_request{})),
  ?assertError(function_clause, kafka:api_key(foo)),
  ok.

parse_stream_test() ->
  D0 = dict:new(),
  ?assertMatch({<<>>, [], D0}, kafka:parse_stream(<<>>, D0)),
  ?assertMatch({<<"foo">>, [], D0}, kafka:parse_stream(<<"foo">>, D0)),
  D1 = dict:store(1, ?API_KEY_METADATA, D0),
  ?assertMatch({<<"foo">>, [], D1}, kafka:parse_stream(<<"foo">>, D1)),
  ok.

encode_metadata_test() ->
  ?assertMatch(<<?i32(0), ?i16s(-1)>>, kafka:encode(#metadata_request{})),
  R = #metadata_request{topics = [<<"FOO">>, <<"BARR">>]},
  ?assertMatch(<<?i32(2), ?i16(4), "BARR", ?i16(3), "FOO">>, kafka:encode(R)),
  ok.

decode_metadata_test() ->
  EmptyArray = empty_array(),
  Bin1 = <<EmptyArray/binary, EmptyArray/binary>>,
  ?assertMatch(#metadata_response{brokers = [], topics = []},
               kafka:decode(?API_KEY_METADATA, Bin1)),
  BrokerIds = [0, 1],
  Brokers = [mk_test_broker(N) || N <- BrokerIds],
  BrokersBin = mk_array(BrokerIds, fun mk_test_broker_bin/1),
  TopicNames = [<<"t1">>, <<"t2">>],
  Topics = [mk_test_topic(T) || T <- TopicNames],
  TopicsBin = mk_array(TopicNames, fun mk_test_topic_bin/1),
  Bin2 = <<BrokersBin/binary, TopicsBin/binary>>,
  ?assertMatch(#metadata_response{brokers = Brokers, topics = Topics},
               kafka:decode(?API_KEY_METADATA, Bin2)),
  ok.

%% make it print full binaries on tty when a test fails
%% to simplify debugging
-undef(assertEqual).
-define(assertEqual(Expect, Expr),
  ((fun (__X) ->
      case (Expr) of
    __X -> ok;
    __V -> .erlang:error({assertEqual_failed,
              [{module, ?MODULE},
               {line, ?LINE},
               {expression, (??Expr)},
               {expected, lists:flatten(io_lib:format("~1000p", [__X]))},
               {value, lists:flatten(io_lib:format("~1000p", [__V]))}]})
      end
    end)(Expect))).

encode_produce_test() ->
  R1 = #produce_request{acks = -1, timeout = 1, data = []},
  ?assertMatch(<<?i16s(-1), ?i32(1), ?i32(0)>>, kafka:encode(R1)),
  T1 = <<"t1">>,
  T2 = <<"t2">>,
  T3 = <<"topic3">>,
  Data = [ {{T1, 0}, []}
         , {{T1, 1}, [{<<>>, <<>>}]}
         , {{T2, 0}, [{<<?i32(1)>>, <<?i32(2)>>}]}
         , {{T1, 2}, []}
         , {{T1, 2}, [{<<"foo">>, <<"bar">>}]}
         , {{T2, 0}, []}
         , {{T1, 1}, [{<<>>, <<>>}, {<<>>, <<?i16(3)>>}]}
         , {{T2, 0}, [{<<>>, <<"foobar">>}]}
         , {{T3, 0}, []}],
  %% metadata: 16b acks, 32b timeout, 32b topics count, topics
  %% topic: 16b name size, name, 32b partitions count, partitions
  %% partition: 32b id, 32b msg set size, msg set
  %% message set: [message]
  %% message: 64b offset, 32b message size, CRC32,
  %%          8b magic byte, 8b compress mode,
  %%          32b key size, key, 32b value size, value
  R2 = #produce_request{acks = 0, timeout = 10, data = Data},
  Crc1 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32s(-1), ?i32s(-1)>>),
  Crc2 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32s(3), "foo", ?i32s(3), "bar">>),
  Crc3 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32s(4), ?i32(1), ?i32s(4), ?i32(2)>>),
  Crc4 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32s(-1), ?i32s(2), ?i16(3)>>),
  Crc5 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32s(-1), ?i32s(6), "foobar">>),
  ?assertEqual(<<?i16s(0), ?i32(10), ?i32(3),    % metadata
                 ?i16(2), T1/binary, ?i32(3),    % t1 start
                 ?i32(0), ?i32(0),               % p0 start/end
                 %% in kafka:group_by_topics/2 dict puts p2 before p0
                 ?i32(2), ?i32(32),              % p2 start
                                                 % message set start
                 ?i64(0), ?i32(20), ?i32(Crc2),  % msg1
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32s(3), "foo", ?i32s(3), "bar",
                                                 % message set end
                                                 % p2 end
                 ?i32(1), ?i32(80),              % p1 start
                                                 % message set start
                 ?i64(0), ?i32(14), ?i32(Crc1),  % msg1
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32s(-1), ?i32s(-1),
                 ?i64(0), ?i32(14), ?i32(Crc1),  % msg2
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32s(-1), ?i32s(-1),
                 ?i64(0), ?i32(16), ?i32(Crc4),  % msg3
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32s(-1), ?i32s(2), ?i16(3),
                                                 % message set end
                                                 % p1 end
                                                 % t1 end
                 ?i16(2), T2/binary, ?i32(1),    % t2 start
                 ?i32(0), ?i32(66),              % p0 start
                                                 % message set start
                 ?i64(0), ?i32(22), ?i32(Crc3),  % msg1
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32s(4), ?i32(1), ?i32s(4), ?i32(2),
                 ?i64(0), ?i32(20), ?i32(Crc5),  % msg2
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32s(-1), ?i32s(6), "foobar",
                                                 % message set end
                                                 % p0 end
                                                 % t2 end
                 ?i16(6), T3/binary, ?i32(1),    % t3 start
                 ?i32(0), ?i32(0),               % p0 start/end
                                                 % t3 end
                 <<>>/binary
               >>, kafka:encode(R2)),
  ok.

mk_test_broker(NodeId) ->
  #broker_metadata{node_id = NodeId, host = "localhost", port = ?PORT}.

mk_test_broker_bin(NodeId) ->
  <<NodeId:32/integer,
    (length("localhost")):16/integer,
    (list_to_binary("localhost"))/binary,
    ?i32(?PORT)>>.

empty_array() -> <<?i32(0)>>.

mk_array(L, F) ->
  Bin = mk_array(L, F, <<>>),
  <<?i32((length(L))), Bin/binary>>.

mk_array([], _F, Acc) ->
  Acc;
mk_array([H | T], F, Acc) ->
  mk_array(T, F, <<(F(H))/binary, Acc/binary>>).

mk_test_partition(Id) ->
  mk_test_partition(Id, 0, 0, [], []).

mk_test_partition(Id, ErrorCode, LeaderId, Replicas, Isrs) ->
  #partition_metadata{ id         = Id
                     , error_code = ErrorCode
                     , leader_id  = LeaderId
                     , replicas   = Replicas
                     , isrs       = Isrs}.

mk_test_partition_bin(Id) ->
  mk_test_partition_bin(Id, 0, 0, [], []).

mk_test_partition_bin(Id, ErrorCode, LeaderId, Replicas, Isrs) ->
  F = fun(I) -> <<I:32/integer>> end,
  <<ErrorCode:16/signed-integer,
    Id:32/integer,
    LeaderId:32/signed-integer,
    (mk_array(Replicas, F))/binary,
    (mk_array(Isrs, F))/binary>>.

mk_test_topic(Name) ->
  mk_test_topic(Name, []).

mk_test_topic(Name, Partitions) ->
  #topic_metadata{name = Name, error_code = 0,
                  partitions = [mk_test_partition(P) || P <- Partitions]}.

mk_test_topic_bin(Name) ->
  mk_test_topic_bin(Name, []).

mk_test_topic_bin(Name, Partitions) ->
  <<0:16/integer,
    (size(Name)):16/integer,
    Name/binary,
    (mk_array(Partitions, fun mk_test_partition_bin/1))/binary>>.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
