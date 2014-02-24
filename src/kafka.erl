%%%=============================================================================
%%% @doc Kafka protocol
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(kafka).

%% API
-export([ api_key/1
        , parse_stream/2
        , encode/2
        , decode/2
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* Macros -------------------------------------------------------------------

-define(CLIENT_ID,      <<"brod">>).
-define(CLIENT_ID_SIZE, 4).

-define(API_VERSION, 0).
-define(MAGIC_BYTE, 0).

-define(REPLICA_ID, -1).

%% Api keys
-define(API_KEY_PRODUCE,        0).
-define(API_KEY_FETCH,          1).
-define(API_KEY_OFFSET,         2).
-define(API_KEY_METADATA,       3).
-define(API_KEY_LEADER_AND_ISR, 4).
-define(API_KEY_STOP_REPLICA,   5).
-define(API_KEY_OFFSET_COMMIT,  6).
-define(API_KEY_OFFSET_FETCH,   7).

%% Compression
-define(COMPRESS_NONE, 0).
-define(COMPRESS_GZIP, 1).
-define(COMPRESS_SNAPPY, 2).

%%%_* API ----------------------------------------------------------------------
%% @doc Parse binary stream of kafka responses.
%%      Returns list of {CorrId, Response} tuples and remaining binary.
%%      CorrIdDict: dict(CorrId -> ApiKey)
parse_stream(Bin, CorrIdDict) ->
  parse_stream(Bin, [], CorrIdDict).

parse_stream(<<Size:32/integer,
               Bin0:Size/binary,
               Tail/binary>>, Acc, CorrIdDict0) ->
  <<CorrId:32/integer, Bin/binary>> = Bin0,
  ApiKey = dict:fetch(CorrId, CorrIdDict0),
  Response = decode(ApiKey, Bin),
  CorrIdDict = dict:erase(CorrId, CorrIdDict0),
  parse_stream(Tail, [{CorrId, Response} | Acc], CorrIdDict);
parse_stream(Bin, Acc, CorrIdDict) ->
  {Bin, lists:reverse(Acc), CorrIdDict}.

encode(CorrId, Request) ->
  Header = header(api_key(Request), CorrId),
  Body = do_encode_request(Request),
  Bin = <<Header/binary, Body/binary>>,
  Size = byte_size(Bin),
  <<Size:32/integer, Bin/binary>>.

api_key(#metadata_request{}) -> ?API_KEY_METADATA;
api_key(#produce_request{})  -> ?API_KEY_PRODUCE;
api_key(#offset_request{})   -> ?API_KEY_OFFSET;
api_key(#fetch_request{})    -> ?API_KEY_FETCH.

decode(?API_KEY_METADATA, Bin) -> metadata_response(Bin);
decode(?API_KEY_PRODUCE, Bin)  -> produce_response(Bin);
decode(?API_KEY_OFFSET, Bin)   -> offset_response(Bin);
decode(?API_KEY_FETCH, Bin)    -> fetch_response(Bin).

%%%_* Internal functions -------------------------------------------------------
header(ApiKey, CorrId) ->
  <<ApiKey:16/integer,
    ?API_VERSION:16/integer,
    CorrId:32/integer,
    ?CLIENT_ID_SIZE:16/integer,
    ?CLIENT_ID/binary>>.

do_encode_request(#metadata_request{} = Request) ->
  metadata_request_body(Request);
do_encode_request(#produce_request{} = Request)  ->
  produce_request_body(Request);
do_encode_request(#offset_request{} = Request)  ->
  offset_request_body(Request);
do_encode_request(#fetch_request{} = Request)  ->
  fetch_request_body(Request).

parse_array(<<Length:32/integer, Bin/binary>>, Fun) ->
  parse_array(Length, Bin, [], Fun).

parse_array(0, Bin, Acc, _Fun) ->
  {lists:reverse(Acc), Bin};
parse_array(Length, Bin0, Acc, Fun) ->
  {Item, Bin} = Fun(Bin0),
  parse_array(Length - 1, Bin, [Item | Acc], Fun).

parse_int32(<<X:32/integer, Bin/binary>>) -> {X, Bin}.

parse_int64(<<X:64/integer, Bin/binary>>) -> {X, Bin}.

kafka_size(<<"">>) -> -1;
kafka_size(Bin)    -> size(Bin).

%%%_* metadata -----------------------------------------------------------------
metadata_request_body(#metadata_request{topics = []}) ->
  <<0:32/integer, -1:16/signed-integer>>;
metadata_request_body(#metadata_request{topics = Topics}) ->
  Length = erlang:length(Topics),
  F = fun(Topic, Acc) ->
          Size = kafka_size(Topic),
          <<Size:16/signed-integer, Topic/binary, Acc/binary>>
      end,
  Bin = lists:foldl(F, <<>>, Topics),
  <<Length:32/integer, Bin/binary>>.

metadata_response(Bin0) ->
  {Brokers, Bin} = parse_array(Bin0, fun parse_broker_metadata/1),
  {Topics, _} = parse_array(Bin, fun parse_topic_metadata/1),
  #metadata_response{brokers = Brokers, topics = Topics}.

parse_broker_metadata(<<NodeID:32/integer,
                        HostSize:16/integer,
                        Host:HostSize/binary,
                        Port:32/integer,
                        Bin/binary>>) ->
  Broker = #broker_metadata{ node_id = NodeID
                           , host = binary_to_list(Host)
                           , port = Port},
  {Broker, Bin}.

parse_topic_metadata(<<ErrorCode:16/integer,
                       Size:16/integer,
                       Name:Size/binary,
                       Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition_metadata/1),
  Topic = #topic_metadata{ error_code = ErrorCode
                         , name = Name
                         , partitions = Partitions},
  {Topic, Bin}.

parse_partition_metadata(<<ErrorCode:16/signed-integer,
                           Id:32/integer,
                           LeaderId:32/signed-integer,
                           Bin0/binary>>) ->
  {Replicas, Bin1} = parse_array(Bin0, fun parse_int32/1),
  {Isrs, Bin} = parse_array(Bin1, fun parse_int32/1),
  Partition = #partition_metadata{ error_code = ErrorCode
                                 , id = Id
                                 , leader_id = LeaderId
                                 , replicas = Replicas
                                 , isrs = Isrs},
  {Partition, Bin}.

%%%_* produce ------------------------------------------------------------------
produce_request_body(#produce_request{} = Produce) ->
  Acks = Produce#produce_request.acks,
  Timeout = Produce#produce_request.timeout,
  Topics = dict:to_list(Produce#produce_request.topics),
  TopicsCount = erlang:length(Topics),
  TopicsBin = encode_topics(Topics, <<>>),
  <<Acks:16/signed-integer, Timeout:32/integer,
    TopicsCount:32/integer, TopicsBin/binary>>.

encode_topics([], Acc) ->
  Acc;
encode_topics([{Topic, PartitionsDict} | T], Acc0) ->
  Size = erlang:size(Topic),
  Partitions = dict:to_list(PartitionsDict),
  PartitionsCount = erlang:length(Partitions),
  PartitionsBin = encode_partitions(Partitions, <<>>),
  Acc = <<Size:16/integer, Topic/binary,
          PartitionsCount:32/integer, PartitionsBin/binary, Acc0/binary>>,
  encode_topics(T, Acc).

encode_partitions([], Acc) ->
  Acc;
encode_partitions([{Id, Messages} | T], Acc0) ->
  MessageSet = encode_message_set(Messages),
  MessageSetSize = erlang:size(MessageSet),
  Acc = <<Id:32/integer,
          MessageSetSize:32/integer,
          MessageSet/binary,
          Acc0/binary>>,
  encode_partitions(T, Acc).

encode_message_set(Messages) ->
  F = fun(X, Acc) ->
          Message = encode_message(X),
          Size = size(Message),
          %% 0 is for offset which is unknown until message is handled by
          %% server, we can put any number here actually
          <<0:64/integer, Size:32/integer, Message/binary, Acc/binary>>
      end,
  MessageSet = lists:foldr(F, <<>>, Messages),
  <<MessageSet/binary>>.

encode_message({Key, Value}) ->
  KeySize = kafka_size(Key),
  ValSize = kafka_size(Value),
  Payload = <<KeySize:32/signed-integer,
              Key/binary,
              ValSize:32/signed-integer,
              Value/binary>>,
  Message = <<?MAGIC_BYTE:8/integer, ?COMPRESS_NONE:8/integer, Payload/binary>>,
  Crc32 = erlang:crc32(Message),
  <<Crc32:32/integer, Message/binary>>.

produce_response(Bin) ->
  {Topics, _} = parse_array(Bin, fun parse_produce_topic/1),
  #produce_response{topics = Topics}.

parse_produce_topic(<<Size:16/integer, Name:Size/binary, Bin0/binary>>) ->
  {Offsets, Bin} = parse_array(Bin0, fun parse_produce_offset/1),
  {#produce_topic{topic = Name, offsets = Offsets}, Bin}.

parse_produce_offset(<<Partition:32/integer,
                       ErrorCode:16/signed-integer,
                       Offset:64/integer,
                       Bin/binary>>) ->
  Res = #produce_offset{ partition = Partition
                       , error_code = ErrorCode
                       , offset = Offset},
  {Res, Bin}.

%%%_* offset -------------------------------------------------------------------
offset_request_body(#offset_request{} = Offset) ->
  Topic = Offset#offset_request.topic,
  Partition = Offset#offset_request.partition,
  Time = Offset#offset_request.time,
  MaxNumberOfOffsets = 1,
  PartitionsBin = <<Partition:32/integer,
                    Time:64/signed-integer,
                    MaxNumberOfOffsets:32/integer>>,
  PartitionsCount = 1,
  TopicSize = erlang:size(Topic),
  TopicsBin = <<TopicSize:16/integer,
                Topic/binary,
                PartitionsCount:32/integer,
                PartitionsBin/binary>>,
  TopicsCount = 1,
  <<?REPLICA_ID:32/signed-integer, TopicsCount:32/integer, TopicsBin/binary>>.

offset_response(Bin) ->
  {Topics, _} = parse_array(Bin, fun parse_offset_topic/1),
  #offset_response{topics = Topics}.

parse_offset_topic(<<Size:16/integer, Name:Size/binary, Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition_offsets/1),
  {#offset_topic{topic = Name, partitions = Partitions}, Bin}.

parse_partition_offsets(<<Partition:32/integer,
                          ErrorCode:16/signed-integer,
                          Bin0/binary>>) ->
  {Offsets, Bin} = parse_array(Bin0, fun parse_int64/1),
  Res = #partition_offsets{ partition = Partition
                          , error_code = ErrorCode
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
  PartitionsBin = <<Partition:32/integer,
                    Offset:64/integer,
                    MaxBytes:32/integer>>,
  PartitionsCount = 1,
  TopicSize = erlang:size(Topic),
  TopicsBin = <<TopicSize:16/integer,
                Topic/binary,
                PartitionsCount:32/integer,
                PartitionsBin/binary>>,
  TopicsCount = 1,
  <<?REPLICA_ID:32/signed-integer,
    MaxWaitTime:32/integer,
    MinBytes:32/integer,
    TopicsCount:32/integer,
    TopicsBin/binary>>.

fetch_response(Bin) ->
  {Topics, _} = parse_array(Bin, fun parse_topic_fetch_data/1),
  #fetch_response{topics = Topics}.

parse_topic_fetch_data(<<Size:16/integer, Name:Size/binary, Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition_messages/1),
  {#topic_fetch_data{topic = Name, partitions = Partitions}, Bin}.

parse_partition_messages(<<Partition:32/integer,
                           ErrorCode:16/integer,
                           HighWmOffset:64/integer,
                           MessageSetSize:32/integer,
                           MessageSetBin:MessageSetSize/binary,
                           Bin/binary>>) ->
  {LastOffset, Messages} = parse_message_set(MessageSetBin),
  Res = #partition_messages{ partition = Partition
                           , error_code = ErrorCode
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
parse_message_set(<<Offset:64/integer,
                    MessageSize:32/integer,
                    MessageBin:MessageSize/binary,
                    Bin/binary>>, Acc) ->
  <<Crc:32/integer,
    MagicByte:8/integer,
    Attributes:8/integer,
    KeySize:32/signed-integer,
    KV/binary>> = MessageBin,
  {Key, ValueBin} = parse_bytes(KeySize, KV),
  <<ValueSize:32/signed-integer, Value0/binary>> = ValueBin,
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
  {Msg#message.offset, lists:reverse(Acc)}.

parse_bytes(-1, Bin) ->
  {<<>>, Bin};
parse_bytes(Size, Bin0) ->
  <<Bytes:Size/binary, Bin/binary>> = Bin0,
  {Bytes, Bin}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
