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
%%% @doc A kafka protocol implementation.
%%%      [https://cwiki.apache.org/confluence/display/KAFKA/
%%%       A+Guide+To+The+Kafka+Protocol].
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_kafka).

%% API
-export([ api_key/1
        , parse_stream/2
        , encode/1
        , encode/3
        , decode/2
        , is_error/1
        ]).

-include("brod_int.hrl").
-include("brod_kafka.hrl").

-define(INT, signed-integer).

%%%_* APIs =====================================================================

%% @doc Parse binary stream of kafka responses.
%%      Returns list of {CorrId, Response} tuples and remaining binary.
-spec parse_stream(binary(), brod_kakfa_requests:requests()) ->
        {binary(), [{corr_id(), term()}]}.
parse_stream(Bin, Requests) ->
  parse_stream(Bin, [], Requests).

parse_stream(<<Size:32/?INT,
               Bin0:Size/binary,
               Tail/binary>>, Acc, Requests) ->
  <<CorrId:32/?INT, Bin/binary>> = Bin0,
  ApiKey = brod_kafka_requests:get_api_key(Requests, CorrId),
  Response = decode(ApiKey, Bin),
  parse_stream(Tail, [{CorrId, Response} | Acc], Requests);
parse_stream(Bin, Acc, _Requests) ->
  {Bin, Acc}.

encode(ClientId, CorrId, Request) ->
  Header = header(api_key(Request), ClientId, CorrId),
  Body = encode(Request),
  Bin = <<Header/binary, Body/binary>>,
  Size = byte_size(Bin),
  <<Size:32/?INT, Bin/binary>>.

api_key(#metadata_request{}) -> ?API_KEY_METADATA;
api_key(#produce_request{})  -> ?API_KEY_PRODUCE;
api_key(#offset_request{})   -> ?API_KEY_OFFSET;
api_key(#fetch_request{})    -> ?API_KEY_FETCH.

decode(?API_KEY_METADATA, Bin) -> metadata_response(Bin);
decode(?API_KEY_PRODUCE, Bin)  -> produce_response(Bin);
decode(?API_KEY_OFFSET, Bin)   -> offset_response(Bin);
decode(?API_KEY_FETCH, Bin)    -> fetch_response(Bin).

is_error(X) -> brod_kafka_errors:is_error(X).

%%%_* Internal Functions =======================================================

header(ApiKey, ClientId, CorrId) ->
  ClientIdLength = size(ClientId),
  <<ApiKey:16/?INT,
    ?API_VERSION:16/?INT,
    CorrId:32/?INT,
    ClientIdLength:16/?INT,
    ClientId/binary>>.

encode(#metadata_request{} = Request) ->
  metadata_request_body(Request);
encode(#produce_request{} = Request)  ->
  produce_request_body(Request);
encode(#offset_request{} = Request)  ->
  offset_request_body(Request);
encode(#fetch_request{} = Request)  ->
  fetch_request_body(Request).

%%%_* metadata -----------------------------------------------------------------
metadata_request_body(#metadata_request{topics = []}) ->
  <<0:32/?INT, -1:16/?INT>>;
metadata_request_body(#metadata_request{topics = Topics}) ->
  Length = erlang:length(Topics),
  F = fun(Topic, Acc) ->
          Size = kafka_size(Topic),
          <<Size:16/?INT, Topic/binary, Acc/binary>>
      end,
  Bin = lists:foldl(F, <<>>, Topics),
  <<Length:32/?INT, Bin/binary>>.

metadata_response(Bin0) ->
  {Brokers, Bin} = parse_array(Bin0, fun parse_broker_metadata/1),
  {Topics, _} = parse_array(Bin, fun parse_topic_metadata/1),
  #metadata_response{brokers = Brokers, topics = Topics}.

parse_broker_metadata(<<NodeID:32/?INT,
                        HostSize:16/?INT,
                        Host:HostSize/binary,
                        Port:32/?INT,
                        Bin/binary>>) ->
  Broker = #broker_metadata{ node_id = NodeID
                           , host = binary_to_list(Host)
                           , port = Port},
  {Broker, Bin}.

parse_topic_metadata(<<ErrorCode:16/?INT,
                       Size:16/?INT,
                       Name:Size/binary,
                       Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition_metadata/1),
  Topic = #topic_metadata{ error_code = brod_kafka_errors:decode(ErrorCode)
                         , name = binary:copy(Name)
                         , partitions = Partitions},
  {Topic, Bin}.

%% isrs = "in sync replicas"
parse_partition_metadata(<<ErrorCode:16/?INT,
                           Id:32/?INT,
                           LeaderId:32/?INT,
                           Bin0/binary>>) ->
  {Replicas, Bin1} = parse_array(Bin0, fun parse_int32/1),
  {Isrs, Bin} = parse_array(Bin1, fun parse_int32/1),
  Partition =
    #partition_metadata{ error_code = brod_kafka_errors:decode(ErrorCode)
                       , id         = Id
                       , leader_id  = LeaderId
                       , replicas   = Replicas
                       , isrs       = Isrs},
  {Partition, Bin}.

%%%_* produce ------------------------------------------------------------------
produce_request_body(#produce_request{} = Produce) ->
  Acks = Produce#produce_request.acks,
  Timeout = Produce#produce_request.timeout,
  Topics = Produce#produce_request.data,
  TopicsCount = erlang:length(Topics),
  Head = <<Acks:16/?INT,
           Timeout:32/?INT,
           TopicsCount:32/?INT>>,
  encode_topics(Topics, Head).

encode_topics([], Acc) ->
  Acc;
encode_topics([{Topic, PartitionsDict} | T], Acc0) ->
  Size = erlang:size(Topic),
  Partitions = dict:to_list(PartitionsDict),
  PartitionsCount = erlang:length(Partitions),
  Acc1 = <<Acc0/binary, Size:16/?INT, Topic/binary,
           PartitionsCount:32/?INT>>,
  Acc = encode_partitions(Partitions, Acc1),
  encode_topics(T, Acc).

encode_partitions([], Acc) ->
  Acc;
encode_partitions([{Id, Messages} | T], Acc0) ->
  MessageSet = encode_message_set(Messages),
  MessageSetSize = erlang:size(MessageSet),
  Acc = <<Acc0/binary,
          Id:32/?INT,
          MessageSetSize:32/?INT,
          MessageSet/binary>>,
  encode_partitions(T, Acc).

encode_message_set(Messages) ->
  encode_message_set(Messages, <<>>).

encode_message_set([], Acc) ->
  Acc;
encode_message_set([Msg | Messages], Acc0) ->
  MsgBin = encode_message(Msg),
  Size = size(MsgBin),
  %% 0 is for offset which is unknown until message is handled by
  %% server, we can put any number here actually
  Acc = <<Acc0/binary, 0:64/?INT, Size:32/?INT, MsgBin/binary>>,
  encode_message_set(Messages, Acc).

encode_message({Key, Value}) ->
  KeySize = kafka_size(Key),
  ValSize = kafka_size(Value),
  Message = <<?MAGIC_BYTE:8/?INT,
              ?COMPRESS_NONE:8/?INT,
              KeySize:32/?INT,
              Key/binary,
              ValSize:32/?INT,
              Value/binary>>,
  Crc32 = erlang:crc32(Message),
  <<Crc32:32/integer, Message/binary>>.

produce_response(Bin) ->
  {Topics, _} = parse_array(Bin, fun parse_produce_topic/1),
  #produce_response{topics = Topics}.

parse_produce_topic(<<Size:16/?INT, Name:Size/binary, Bin0/binary>>) ->
  {Offsets, Bin} = parse_array(Bin0, fun parse_produce_offset/1),
  {#produce_topic{topic = binary:copy(Name), offsets = Offsets}, Bin}.

parse_produce_offset(<<Partition:32/?INT,
                       ErrorCode:16/?INT,
                       Offset:64/?INT,
                       Bin/binary>>) ->
  Res = #produce_offset{ partition = Partition
                       , error_code = brod_kafka_errors:decode(ErrorCode)
                       , offset = Offset},
  {Res, Bin}.

%%%_* offset -------------------------------------------------------------------
offset_request_body(#offset_request{} = Offset) ->
  Topic = Offset#offset_request.topic,
  Partition = Offset#offset_request.partition,
  Time = Offset#offset_request.time,
  MaxNumberOfOffsets = Offset#offset_request.max_n_offsets,
  TopicSize = erlang:size(Topic),
  PartitionsCount = 1,
  TopicsCount = 1,
  <<?REPLICA_ID:32/?INT,
    TopicsCount:32/?INT,
    TopicSize:16/?INT,
    Topic/binary,
    PartitionsCount:32/?INT,
    Partition:32/?INT,
    Time:64/?INT,
    MaxNumberOfOffsets:32/?INT>>.

offset_response(Bin) ->
  {Topics, _} = parse_array(Bin, fun parse_offset_topic/1),
  #offset_response{topics = Topics}.

parse_offset_topic(<<Size:16/?INT, Name:Size/binary, Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition_offsets/1),
  {#offset_topic{topic = binary:copy(Name), partitions = Partitions}, Bin}.

parse_partition_offsets(<<Partition:32/?INT,
                          ErrorCode:16/?INT,
                          Bin0/binary>>) ->
  {Offsets, Bin} = parse_array(Bin0, fun parse_int64/1),
  Res = #partition_offsets{ partition = Partition
                          , error_code = brod_kafka_errors:decode(ErrorCode)
                          , offsets = Offsets},
  {Res, Bin}.

%%%_* fetch --------------------------------------------------------------------
fetch_request_body(#fetch_request{} = Fetch) ->
  #fetch_request{ max_wait_time = MaxWaitTime
                , min_bytes     = MinBytes
                , topic         = Topic
                , partition     = Partition
                , offset        = Offset
                , max_bytes     = MaxBytes} = Fetch,
  PartitionsCount = 1,
  TopicsCount = 1,
  TopicSize = erlang:size(Topic),
  <<?REPLICA_ID:32/?INT,
    MaxWaitTime:32/?INT,
    MinBytes:32/?INT,
    TopicsCount:32/?INT,
    TopicSize:16/?INT,
    Topic/binary,
    PartitionsCount:32/?INT,
    Partition:32/?INT,
    Offset:64/?INT,
    MaxBytes:32/?INT>>.

fetch_response(Bin) ->
  {Topics, _} = parse_array(Bin, fun parse_topic_fetch_data/1),
  #fetch_response{topics = Topics}.

parse_topic_fetch_data(<<Size:16/?INT, Name:Size/binary, Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition_messages/1),
  {#topic_fetch_data{topic = binary:copy(Name), partitions = Partitions}, Bin}.

parse_partition_messages(<<Partition:32/?INT,
                           ErrorCode:16/?INT,
                           HighWmOffset:64/?INT,
                           MessageSetSize:32/?INT,
                           MessageSetBin:MessageSetSize/binary,
                           Bin/binary>>) ->
  {LastOffset, Messages} = parse_message_set(MessageSetBin),
  Res = #partition_messages{ partition = Partition
                           , error_code = brod_kafka_errors:decode(ErrorCode)
                           , high_wm_offset = HighWmOffset
                           , last_offset = LastOffset
                           , messages = Messages},
  {Res, Bin}.

parse_message_set(Bin) ->
  parse_message_set(Bin, []).

parse_message_set(<<>>, []) ->
  {0, []};
parse_message_set(<<>>, [Msg | _] = Acc) ->
  {Msg#message.offset, lists:reverse(Acc)};
parse_message_set(<<Offset:64/?INT,
                    MessageSize:32/?INT,
                    MessageBin:MessageSize/binary,
                    Bin/binary>>, Acc) ->
  <<Crc:32/integer,
    MagicByte:8/?INT,
    Attributes:8/?INT,
    KeySize:32/?INT,
    KV/binary>> = MessageBin,
  {Key, ValueBin} = parse_bytes(KeySize, KV),
  <<ValueSize:32/?INT, Value0/binary>> = ValueBin,
  {Value, <<>>} = parse_bytes(ValueSize, Value0),
  Msg = #message{ offset     = Offset
                , crc        = Crc
                , magic_byte = MagicByte
                , attributes = Attributes
                , key        = Key
                , value      = Value},
  parse_message_set(Bin, [Msg | Acc]);
parse_message_set(_Bin, [Msg | _] = Acc) ->
  %% the last message in response was sent only partially, dropping
  {Msg#message.offset, lists:reverse(Acc)};
parse_message_set(_Bin, []) ->
  %% The only case when I managed to get there is when max_bytes option
  %% is too small to for a whole message.
  %% For some reason kafka does not report error.
  throw("max_bytes option is too small").

parse_array(<<Length:32/?INT, Bin/binary>>, Fun) ->
  parse_array(Length, Bin, [], Fun).

parse_array(0, Bin, Acc, _Fun) ->
  {Acc, Bin};
parse_array(Length, Bin0, Acc, Fun) ->
  {Item, Bin} = Fun(Bin0),
  parse_array(Length - 1, Bin, [Item | Acc], Fun).

parse_int32(<<X:32/?INT, Bin/binary>>) -> {X, Bin}.

parse_int64(<<X:64/?INT, Bin/binary>>) -> {X, Bin}.

kafka_size(<<"">>) -> -1;
kafka_size(Bin)    -> size(Bin).

parse_bytes(-1, Bin) ->
  {<<>>, Bin};
parse_bytes(Size, Bin0) ->
  <<Bytes:Size/binary, Bin/binary>> = Bin0,
  {binary:copy(Bytes), Bin}.

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

parse_array_test() ->
  F = fun(<<Size:32/?INT, X:Size/binary, Bin/binary>>) ->
          {binary_to_list(X), Bin}
      end,
  ?assertMatch({["BARR", "FOO"], <<>>},
               parse_array(<<2:32/?INT, 3:32/?INT, "FOO",
                           4:32/?INT, "BARR">>, F)),
  ?assertMatch({["FOO"], <<4:32/?INT, "BARR">>},
               parse_array(<<1:32/?INT, 3:32/?INT, "FOO",
                                           4:32/?INT, "BARR">>, F)),
  ?assertMatch({[], <<>>}, parse_array(<<0:32/?INT>>, F)),
  ?assertError(function_clause, parse_array(<<-1:32/?INT>>, F)),
  ?assertError(function_clause, parse_array(<<1:32/?INT>>, F)),
  ok.

parse_int32_test() ->
  ?assertMatch({0, <<"123">>}, parse_int32(<<0:32/?INT, "123">>)),
  ?assertMatch({0, <<"">>}, parse_int32(<<0:32/?INT>>)),
  ?assertError(function_clause, parse_int32(<<0:16/?INT>>)),
  ok.

parse_int64_test() ->
  ?assertMatch({0, <<"123">>}, parse_int64(<<0:64/?INT, "123">>)),
  ?assertMatch({0, <<"">>}, parse_int64(<<0:64/?INT>>)),
  ?assertError(function_clause, parse_int64(<<0:32/?INT>>)),
  ok.

parse_bytes_test() ->
  ?assertMatch({<<"1234">>, <<"5678">>}, parse_bytes(4, <<"12345678">>)),
  ?assertMatch({<<"1234">>, <<"">>}, parse_bytes(4, <<"1234">>)),
  ?assertMatch({<<"">>, <<"1234">>}, parse_bytes(-1, <<"1234">>)),
  ?assertError({badmatch, <<"123">>}, parse_bytes(4, <<"123">>)),
  ok.

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
