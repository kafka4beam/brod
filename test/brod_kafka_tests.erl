%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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
-module(brod_kafka_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/brod_int.hrl").
-include("src/brod_kafka.hrl").

-define(PORT, 1234).

-define(i8(I),  I:8/signed-integer).
-define(i16(I), I:16/signed-integer).
-define(i32(I), I:32/signed-integer).
-define(i64(I), I:64/signed-integer).

-define(l2b(L), erlang:list_to_binary(L)).

-define(max8,  (1 bsl 7 - 1)).
-define(max16, (1 bsl 15 - 1)).
-define(max32, (1 bsl 31 - 1)).

%% make it print full binaries on tty when a test fails
%% to simplify debugging
-undef(assertEqual).
-define(assertEqual(Expect, Expr),
  ((fun (__X) ->
      case (Expr) of
    __X -> ok;
    __V -> erlang:error({assertEqual_failed,
              [{expression, (??Expr)},
               {expected, lists:flatten(io_lib:format("~1000p", [__X]))},
               {value, lists:flatten(io_lib:format("~1000p", [__V]))}]})
      end
    end)(Expect))).

api_key_test() ->
  ?assertMatch(?API_KEY_METADATA, brod_kafka:api_key(#metadata_request{})),
  ?assertMatch(?API_KEY_PRODUCE, brod_kafka:api_key(#produce_request{})),
  ?assertMatch(?API_KEY_OFFSET, brod_kafka:api_key(#offset_request{})),
  ?assertMatch(?API_KEY_FETCH, brod_kafka:api_key(#fetch_request{})),
  ?assertError(function_clause, brod_kafka:api_key(foo)),
  ok.

parse_stream_test() ->
  R0 = brod_kafka_requests:new(),
  ?assertMatch({<<>>, []}, brod_kafka:parse_stream(<<>>, R0)),
  ?assertMatch({<<"foo">>, []}, brod_kafka:parse_stream(<<"foo">>, R0)),
  R1 = brod_kafka_requests:add(R0, self(), ?API_KEY_METADATA),
  ?assertMatch({<<"foo">>, []}, brod_kafka:parse_stream(<<"foo">>, R1)),
  ok.

encode_metadata_test() ->
  ?assertMatch(<<?i32(0), ?i16(-1)>>, brod_kafka:encode(#metadata_request{})),
  R = #metadata_request{topics = [<<"FOO">>, <<"BARR">>]},
  ?assertMatch(<<?i32(2), ?i16(4), "BARR", ?i16(3), "FOO">>,
               brod_kafka:encode(R)),
  ok.

decode_metadata_test() ->
  %% array: 32b length (number of items), [item]
  %% metadata response: array of brokers, array of topics
  %% broker: 32b node id, 16b host size, host, 32b port
  %% topic: 16b error code, 16b name size, name, array of partitions
  %% partition: 16b error code, 32b partition, 32b leader,
  %%            array of replicas, array of in-sync-replicas
  %% replica: 32b node id
  %% isr: 32b node id
  Bin1 = <<?i32(0), ?i32(0)>>,
  ?assertMatch(#metadata_response{brokers = [], topics = []},
               brod_kafka:decode(?API_KEY_METADATA, Bin1)),
  Host = "localhost",
  Node1 = 0,
  Node2 = ?max32,
  Leader1 = -1,
  Leader2 = ?max16,
  ErrorCode1 = -1,     ErrorCode1Decoded = brod_kafka_errors:decode(ErrorCode1),
  ErrorCode2 = ?max16, ErrorCode2Decoded = brod_kafka_errors:decode(ErrorCode2),
  Brokers = [ #broker_metadata{node_id = Node2, host = Host, port = ?PORT}
            , #broker_metadata{node_id = Node1, host = Host, port = ?PORT}],
  BrokersBin = <<?i32(2),
                 ?i32(Node1), ?i16((length(Host))),
                 (?l2b(Host))/binary, ?i32(?PORT),
                 ?i32(Node2), ?i16((length(Host))),
                 (?l2b(Host))/binary, ?i32(?PORT)>>,
  Partitions = [ #partition_metadata{ error_code = ErrorCode2Decoded
                                    , id = Node2
                                    , leader_id = Leader2
                                    , replicas = []
                                    , isrs = []}
               , #partition_metadata{ error_code = ErrorCode1Decoded
                                    , id = Node1
                                    , leader_id = Leader1
                                    , replicas = [1,2,3]
                                    , isrs = [1,2]}],
  T1 = <<"t1">>,
  T2 = <<"t2">>,
  Topics = [ #topic_metadata{name = T2, error_code = ErrorCode2Decoded,
                             partitions = Partitions}
           , #topic_metadata{name = T1, error_code = ErrorCode1Decoded,
                             partitions = []}],
  TopicsBin = <<?i32(2),
                ?i16(ErrorCode1), ?i16((size(T1))), T1/binary, ?i32(0),
                ?i16(ErrorCode2), ?i16((size(T2))), T2/binary, ?i32(2),
                ?i16(ErrorCode1), ?i32(Node1), ?i32(Leader1),
                ?i32(3), ?i32(3), ?i32(2), ?i32(1),
                ?i32(2), ?i32(2), ?i32(1),
                ?i16(ErrorCode2), ?i32(Node2), ?i32(Leader2),
                ?i32(0), ?i32(0)
              >>,
  Bin2 = <<BrokersBin/binary, TopicsBin/binary>>,
  ?assertEqual(#metadata_response{brokers = Brokers, topics = Topics},
               brod_kafka:decode(?API_KEY_METADATA, Bin2)),
  ok.

encode_produce_test() ->
  Acks1 = -1,
  Timeout1 = 0,
  R1 = #produce_request{acks = Acks1, timeout = Timeout1, data = []},
  ?assertMatch(<<?i16(Acks1), ?i32(Timeout1), ?i32(0)>>,
               brod_kafka:encode(R1)),
  T1 = <<"t1">>,
  T2 = <<"t2">>,
  T3 = <<"topic3">>,
  T1P0Data = [],
  T1P1Data = [{<<>>, <<>>}, {<<>>, <<>>}, {<<>>, <<?i16(3)>>}],
  T1P2Data = [{<<"foo">>, <<"bar">>}],
  T2P0Data = [{<<?i32(1)>>, <<?i32(2)>>}, {<<>>, <<"foobar">>}],
  T3P0Data = [],
  Data = [ {T1, [{0, T1P0Data}
                ,{1, T1P1Data}
                ,{2, T1P2Data}]}
         , {T2, [{0, T2P0Data}]}
         , {T3, [{0, T3P0Data}]}
         ],
  Acks2 = ?max8,
  Timeout2 = ?max32,
  R2 = #produce_request{acks = Acks2, timeout = Timeout2, data = Data},
  Crc1 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32(-1), ?i32(-1)>>),
  Crc2 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32(3), "foo", ?i32(3), "bar">>),
  Crc3 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32(4), ?i32(1), ?i32(4), ?i32(2)>>),
  Crc4 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32(-1), ?i32(2), ?i16(3)>>),
  Crc5 = erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                        ?i32(-1), ?i32(6), "foobar">>),
  %% metadata: 16b acks, 32b timeout, 32b topics count, topics
  %% topic: 16b name size, name, 32b partitions count, partitions
  %% partition: 32b id, 32b msg set size, msg set
  %% message set: [message]
  %% message: 64b offset, 32b message size, CRC32,
  %%          8b magic byte, 8b compress mode,
  %%          32b key size, key, 32b value size, value
  ?assertEqual(<<?i16(Acks2), ?i32(Timeout2), ?i32(3), % metadata
                 ?i16(2), T1/binary, ?i32(3),     % t1 start
                 ?i32(0), ?i32(0),                % p0 start/end

                 ?i32(1), ?i32(80),              % p1 start
                                                 % message set start
                 ?i64(0), ?i32(14), ?i32(Crc1),  % msg1
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32(-1), ?i32(-1),
                 ?i64(0), ?i32(14), ?i32(Crc1),  % msg2
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32(-1), ?i32(-1),
                 ?i64(0), ?i32(16), ?i32(Crc4),  % msg3
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32(-1), ?i32(2), ?i16(3),
                                                 % message set end
                                                 % p1 end

                 ?i32(2), ?i32(32),               % p2 start
                                                  % message set start
                 ?i64(0), ?i32(20), ?i32(Crc2),   % msg1
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32(3), "foo", ?i32(3), "bar",
                                                 % message set end
                                                 % p2 end

                                                 % t1 end
                 ?i16(2), T2/binary, ?i32(1),    % t2 start
                 ?i32(0), ?i32(66),              % p0 start
                                                 % message set start
                 ?i64(0), ?i32(22), ?i32(Crc3),  % msg1
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32(4), ?i32(1), ?i32(4), ?i32(2),
                 ?i64(0), ?i32(20), ?i32(Crc5),  % msg2
                 ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE),
                 ?i32(-1), ?i32(6), "foobar",
                                                 % message set end
                                                 % p0 end
                                                 % t2 end
                 ?i16(6), T3/binary, ?i32(1),    % t3 start
                 ?i32(0), ?i32(0),               % p0 start/end
                                                 % t3 end
                 <<>>/binary
               >>, brod_kafka:encode(R2)),
  ok.

decode_produce_test() ->
  %% array: 32b length (number of items), [item]
  %% produce response: array of topics
  %% topic: 16b name size, name, array of offsets
  %% offset: 32b partition, 16b error code, 64b offset
  ?assertEqual(#produce_response{topics = []},
               brod_kafka:decode(?API_KEY_PRODUCE, <<?i32(0)>>)),
  Topic1 = <<"t1">>,
  Offset1 = -1,
  ProduceOffset1 = #produce_offset{ partition = 0
                                  , error_code = ?EC_UNKNOWN
                                  , offset = Offset1},
  ProduceTopic1 = #produce_topic{ topic = Topic1
                                , offsets = [ProduceOffset1]},
  Bin1 = <<?i32(1), ?i16(2), Topic1/binary, ?i32(1),
           ?i32(0), ?i16(-1), ?i64(Offset1)>>,
  ?assertEqual(#produce_response{topics = [ProduceTopic1]},
              brod_kafka:decode(?API_KEY_PRODUCE, Bin1)),

  Topic2 = <<"t2">>,
  Topic3 = <<"t3">>,
  Offset2 = 0,
  Offset3 = 1,
  ProduceOffset2 = #produce_offset{ partition = 0
                                  , error_code = ?EC_OFFSET_OUT_OF_RANGE
                                  , offset = Offset2},
  ProduceOffset3 = #produce_offset{ partition = 2
                                  , error_code = ?EC_CORRUPT_MESSAGE
                                  , offset = Offset3},
  ProduceTopic2 = #produce_topic{ topic = Topic2
                                , offsets = [ ProduceOffset3
                                            , ProduceOffset2]},
  ProduceTopic3 = #produce_topic{ topic = Topic3
                                , offsets = []},
  Bin2 = <<?i32(2),
           ?i16(2), Topic2/binary, ?i32(2),
           ?i32(0), ?i16(1), ?i64(Offset2),
           ?i32(2), ?i16(2), ?i64(Offset3),
           ?i16(2), Topic3/binary, ?i32(0)
         >>,
  ?assertEqual(#produce_response{topics = [ProduceTopic3, ProduceTopic2]},
              brod_kafka:decode(?API_KEY_PRODUCE, Bin2)),
  ok.

encode_offset_test() ->
  Topic = <<"topic">>,
  Partition = 0,
  Time1 = -1,
  MaxNOffsets = 1,
  R1 = #offset_request{ topic = Topic
                      , partition = Partition
                      , time = Time1
                      , max_n_offsets = MaxNOffsets},
  Bin1 = <<?i32(?REPLICA_ID), ?i32(1), ?i16((size(Topic))), Topic/binary,
         ?i32(1), ?i32(Partition), ?i64(Time1), ?i32(MaxNOffsets)>>,
  ?assertEqual(Bin1, brod_kafka:encode(R1)),
  Time2 = -1,
  R2 = R1#offset_request{time = Time2},
  Bin2 = <<?i32(?REPLICA_ID), ?i32(1), ?i16((size(Topic))), Topic/binary,
         ?i32(1), ?i32(Partition), ?i64(Time2), ?i32(MaxNOffsets)>>,
  ?assertEqual(Bin2, brod_kafka:encode(R2)),
  ok.

decode_offset_test() ->
  %% array: 32b length (number of items), [item]
  %% offset response: array of topics
  %% topic: 16b name size, name, array of partitions
  %% partition: 32b partition, 16b error code, array of offsets
  %% offset: 64b int
  ?assertEqual(#offset_response{topics = []},
               brod_kafka:decode(?API_KEY_OFFSET, <<?i32(0)>>)),
  Topic = <<"t1">>,
  R1 = #offset_response{topics = [#offset_topic{ topic = Topic
                                               , partitions = []}]},
  Bin1 = <<?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(0)>>,
  ?assertEqual(R1, brod_kafka:decode(?API_KEY_OFFSET, Bin1)),
  Partition = 0,
  ErrorCode = -1,
  ErrorCodeDecoded = ?EC_UNKNOWN,
  Bin2 = <<?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(1),
           ?i32(Partition), ?i16(ErrorCode), ?i32(0)>>,
  Partitions2 = [#partition_offsets{ partition = Partition
                                   , error_code = ErrorCodeDecoded
                                   , offsets = []}],
  R2 = #offset_response{
                  topics = [#offset_topic{ topic = Topic
                                         , partitions = Partitions2}]},
  ?assertEqual(R2, brod_kafka:decode(?API_KEY_OFFSET, Bin2)),
  Offsets = [0, 1, -1, 3],
  OffsetsBin = << << ?i64(X) >> || X <- lists:reverse(Offsets) >>,
  Partitions3 = [#partition_offsets{ partition = Partition
                                   , error_code = ErrorCodeDecoded
                                   , offsets = Offsets}],
  R3 = #offset_response{topics = [#offset_topic{ topic = Topic
                                               , partitions = Partitions3}]},
  Bin3 = <<?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(1),
           ?i32(Partition), ?i16(ErrorCode), ?i32((length(Offsets))),
           OffsetsBin/binary>>,
  ?assertEqual(R3, brod_kafka:decode(?API_KEY_OFFSET, Bin3)),
  ok.

encode_fetch_test() ->
  MaxWaitTime = ?max32,
  MinBytes = ?max32,
  Topic = <<"topic">>,
  Partition = ?max32,
  Offset = -1,
  MaxBytes = ?max32,
  R = #fetch_request{ max_wait_time = MaxWaitTime
                    , min_bytes     = MinBytes
                    , topic         = Topic
                    , partition     = Partition
                    , offset        = Offset
                    , max_bytes     = MaxBytes},
  Bin = <<?i32(?REPLICA_ID), ?i32(MaxWaitTime), ?i32(MinBytes),
          ?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(1),
          ?i32(Partition), ?i64(Offset), ?i32(MaxBytes)>>,
  ?assertEqual(Bin, brod_kafka:encode(R)),
  ok.

decode_fetch_test() ->
  %% array: 32b length (number of items), [item]
  %% offset response: array of topics
  %% topic: 16b name size, name, array of partition message sets
  %% partition message set: 32b partition, 16b error code,
  %%   64b high watermark offset, 32b message set size, message set
  %% message set: [message]
  %% message: 64b offset, 32b message size, 32b crc, 8b magic byte,
  %%          8b attributes, 32b key size, key, 32b value size, value
  ?assertEqual(#fetch_response{topics = []},
               brod_kafka:decode(?API_KEY_FETCH, <<?i32(0)>>)),
  Topic = <<"t1">>,
  R1 = #fetch_response{topics = [#topic_fetch_data{ topic = Topic
                                                  , partitions = []}]},
  Bin1 = <<?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(0)>>,
  ?assertEqual(R1, brod_kafka:decode(?API_KEY_FETCH, Bin1)),

  R2 = #fetch_response{
    topics = [#topic_fetch_data{ topic = Topic
                               , partitions =
                                 [#partition_messages{ partition = 0
                                                     , error_code = ?EC_NONE
                                                     , high_wm_offset = 0
                                                     , last_offset = 0
                                                     , messages = []}]}]},
  Bin2 = <<?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(1),
           ?i32(0), ?i16(0), ?i64(0), ?i32(0)>>,
  ?assertEqual(R2, brod_kafka:decode(?API_KEY_FETCH, Bin2)),

  Bin3 = <<?i32(1), ?i16((size(Topic))), Topic/binary, ?i32(1),
           ?i32(0), ?i16(0), ?i64(0), ?i32(13), ?i64(0), ?i32(16), "x">>,
  ?assertThrow("max_bytes option is too small",
               brod_kafka:decode(?API_KEY_FETCH, Bin3)),

  Topic1 = <<"t1">>,
  Topic2 = random_bytes(),
  %% Topic2 = <<"t2">>,
  Topic3 = <<"t3">>,
  Partition1 = 1,
  ErrorCode1 = 0,
  ErrorCodeDecoded1 = ?EC_NONE,
  Partition2 = -1,
  ErrorCode2 = -1,
  ErrorCodeDecoded2 = ?EC_UNKNOWN,
  HighWmOffset1 = 1,
  HighWmOffset2 = -1,
  K2 = random_bytes(),
  V2 = random_bytes(),
  %% K2 = <<"a">>,
  %% V2 = <<"b">>,
  Message1 = #message{ offset = 0
                     , magic_byte = ?MAGIC_BYTE
                     , attributes = 0
                     , crc = msgcrc(<<"1">>, <<"2">>)
                     , key = <<"1">>
                     , value = <<"2">>},
  Message2 = #message{ offset = -1
                     , magic_byte = ?MAGIC_BYTE
                     , attributes = 0
                     , crc = msgcrc(K2, V2)
                     , key = K2
                     , value = V2},
  Message3 = #message{ offset = 0
                     , magic_byte = ?MAGIC_BYTE
                     , attributes = 0
                     , crc = msgcrc(<<>>, <<"0">>)
                     , key = <<>>
                     , value = <<"0">>},
  Message4 = #message{ offset = 0
                     , magic_byte = ?MAGIC_BYTE
                     , attributes = 0
                     , crc = msgcrc(<<>>, <<>>)
                     , key = <<>>
                     , value = <<>>},
  Partitions1 = [ #partition_messages{ partition = Partition2
                                     , error_code = ErrorCodeDecoded2
                                     , high_wm_offset = HighWmOffset2
                                     , last_offset = 0
                                     , messages = []}
                , #partition_messages{ partition = Partition1
                                     , error_code = ErrorCodeDecoded1
                                     , high_wm_offset = HighWmOffset1
                                     , last_offset = -1
                                     , messages = [Message1, Message2]}],
  Partitions3 = [ #partition_messages{ partition = Partition1
                                     , error_code = ErrorCodeDecoded1
                                     , high_wm_offset = HighWmOffset1
                                     , last_offset = 0
                                     , messages = [Message1, Message2,
                                                   Message3, Message4]}],
  R3 = #fetch_response{topics = [ #topic_fetch_data{ topic = Topic3
                                                   , partitions = Partitions3}
                                , #topic_fetch_data{ topic = Topic2
                                                   , partitions = []}
                                , #topic_fetch_data{ topic = Topic1
                                                   , partitions = Partitions1}
                                ]
                      },
  %% size of a message in a message set excluding key/value: 26 bytes
  Bin4 = <<?i32(3),                             % number of topics
           ?i16((size(Topic1))), Topic1/binary, % topic 1 start
           ?i32(2),                             % partitions in topic
                                                % partition 1 start
           ?i32(Partition1), ?i16(ErrorCode1), ?i64(HighWmOffset1),
           ?i32((26 * 2 + 2 + size(K2) + size(V2))), % message set start
           ?i64(0), ?i32(16), ?i32((msgcrc(<<"1">>, <<"2">>))), % msg1
           ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32(1),
           <<"1">>/binary, ?i32(1), <<"2">>/binary,
           ?i64(-1), ?i32((14 + size(K2) + size(V2))),          % msg2
           ?i32((msgcrc(K2, V2))),
           ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32((size(K2))),
           K2/binary, ?i32((size(V2))), V2/binary,
                                                % partition 1 end
                                                % partition 2 start
           ?i32(Partition2), ?i16(ErrorCode2), ?i64(HighWmOffset2), ?i32(0),
                                                % partition 2 end
                                                % topic 1 end
           ?i16((size(Topic2))), Topic2/binary, % topic 2 start
           ?i32(0),                             % partitions in topic
                                                % topic 2 end
           ?i16((size(Topic3))), Topic3/binary, % topic 3 start
           ?i32(1),                             % partitions in topic
                                                % partition 1 start
           ?i32(Partition1), ?i16(ErrorCode1), ?i64(HighWmOffset1),
           ?i32((26 * 4 + 2 + 1 + size(K2) + size(V2))), % message set start
           ?i64(0), ?i32(16), ?i32((msgcrc(<<"1">>, <<"2">>))), % msg1
           ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32(1),
           <<"1">>/binary, ?i32(1), <<"2">>/binary,
           ?i64(-1), ?i32((14 + size(K2) + size(V2))),   % msg2
           ?i32((msgcrc(K2, V2))),
           ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32((size(K2))),
           K2/binary, ?i32((size(V2))), V2/binary,
           ?i64(0), ?i32((14 + 1)),             % msg3
           ?i32((msgcrc(<<>>, <<"0">>))),
           ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32(-1),
           ?i32(1), <<"0">>/binary,
           ?i64(0), ?i32(14),                   % msg4
           ?i32((msgcrc(<<>>, <<>>))),
           ?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32(-1),
           ?i32(-1)
                                                % partition 1 end
                                                % topic 3 end
           >>,
  ?assertEqual(R3, brod_kafka:decode(?API_KEY_FETCH, Bin4)),
  ok.

msgcrc(<<>>, <<>>) ->
  erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32(-1),
                 ?i32(-1)>>);
msgcrc(<<>>, V) ->
  erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32(-1),
                 ?i32((size(V))), V/binary>>);
msgcrc(K, <<>>) ->
  erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32((size(K))),
                 K/binary, ?i32(-1)>>);
msgcrc(K, V) ->
  erlang:crc32(<<?i8(?MAGIC_BYTE), ?i8(?COMPRESS_NONE), ?i32((size(K))),
                 K/binary, ?i32((size(V))), V/binary>>).

random_bytes() -> crypto:rand_bytes(random:uniform(1 bsl 16 - 1)).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
