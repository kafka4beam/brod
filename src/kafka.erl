%%%=============================================================================
%%% @doc Kafka protocol
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(kafka).

%% API
-export([ pre_parse_stream/1
        , metadata_request/2
        , metadata_response/1
        , produce_request/4
        , produce_response/1
        ]).

%%%_* Includes -----------------------------------------------------------------
-include_lib("brod/include/brod.hrl").

%%%_* Macros -------------------------------------------------------------------

-define(CLIENT_ID,      <<"brod">>).
-define(CLIENT_ID_SIZE, 4).

-define(API_VERSION, 0).
-define(MAGIC_BYTE, 0).

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
%% @doc Split binary stream in list of tuples {CorrellationId, Response}.
%%      Returns resulting list and remaining binary.
pre_parse_stream(Bin) ->
  pre_parse_stream(Bin, []).

pre_parse_stream(<<Length:32/integer, Bin0:Length/binary, Tail/binary>>, Acc) ->
  <<CorrId:32/integer, Bin/binary>> = Bin0,
  pre_parse_stream(Tail, [{CorrId, Bin} | Acc]);
pre_parse_stream(Bin, Acc) ->
  {Acc, Bin}.

metadata_request(CorrId, Topics) ->
  Header = header(?API_KEY_METADATA, CorrId),
  Body = metadata_request_body(Topics),
  Request = <<Header/binary, Body/binary>>,
  Size = byte_size(Request),
  <<Size:32/integer, Request/binary>>.

metadata_response(Bin0) ->
  {Brokers, Bin} = parse_array(Bin0, fun parse_broker_metadata/1),
  {Topics, _} = parse_array(Bin, fun parse_topic_metadata/1),
  #metadata{brokers = Brokers, topics = Topics}.

produce_request(CorrId, Data, Acks, Timeout) ->
  Header = header(?API_KEY_PRODUCE, CorrId),
  Body = produce_request_body(Acks, Timeout, Data),
  Request = <<Header/binary, Body/binary>>,
  Size = byte_size(Request),
  <<Size:32/integer, Request/binary>>.

produce_response(Bin) ->
  {TopicOffsets, _} = parse_array(Bin, fun parse_topic_offsets/1),
  TopicOffsets.

%%%_* Internal functions -------------------------------------------------------
header(ApiKey, CorrId) ->
  <<ApiKey:16/integer,
    ?API_VERSION:16/integer,
    CorrId:32/integer,
    ?CLIENT_ID_SIZE:16/integer,
    ?CLIENT_ID/binary>>.

metadata_request_body([]) ->
  <<0:32/integer, -1:16/integer>>;
metadata_request_body(Topics) ->
  Length = erlang:length(Topics),
  F = fun(Topic, Acc) ->
          Size = kafka_size(Topic),
          <<Size:16/integer, Topic/binary, Acc/binary>>
      end,
  Bin = lists:foldl(F, <<>>, Topics),
  <<Length:32/integer, Bin/binary>>.

produce_request_body(Acks, Timeout, Data) ->
  TopicsCount = erlang:length(Data),
  TopicsBin = encode_topics(Data, <<>>),
  <<Acks:16/integer, Timeout:32/integer,
    TopicsCount:32/integer, TopicsBin/binary>>.

encode_topics([], Acc) ->
  Acc;
encode_topics([#data{topic = Topic, partitions = Partitions} | T], Acc0) ->
  Size = erlang:size(Topic),
  PartitionsCount = erlang:length(Partitions),
  PartitionsBin = encode_partitions(Partitions, <<>>),
  Acc = <<Size:16/integer, Topic/binary,
          PartitionsCount:32/integer, PartitionsBin/binary, Acc0/binary>>,
  encode_topics(T, Acc).

encode_partitions([], Acc) ->
  Acc;
encode_partitions([#partition_messages{id = Id} = P | T], Acc0) ->
  MessageSet = encode_message_set(P#partition_messages.messages),
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

encode_message(#message{key = Key, value = Value}) ->
  KeySize = kafka_size(Key),
  ValSize = kafka_size(Value),
  Payload = <<KeySize:32/integer,
              Key/binary,
              ValSize:32/integer,
              Value/binary>>,
  Message = <<?MAGIC_BYTE:8/integer, ?COMPRESS_NONE:8/integer, Payload/binary>>,
  Crc32 = erlang:crc32(Message),
  <<Crc32:32/integer, Message/binary>>.

parse_array(<<Length:32/integer, Bin/binary>>, Fun) ->
  parse_array(Length, Bin, [], Fun).

parse_array(0, Bin, Acc, _Fun) ->
  {Acc, Bin};
parse_array(Length, Bin0, Acc, Fun) ->
  {Item, Bin} = Fun(Bin0),
  parse_array(Length - 1, Bin, [Item | Acc], Fun).

parse_broker_metadata(<<NodeID:32/integer,
                        HostSize:16/integer,
                        Host:HostSize/binary,
                        Port:32/integer,
                        Bin/binary>>) ->
  Broker = #broker_metadata{ node_id = NodeID
                           , host = Host
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

parse_partition_metadata(<<ErrorCode:16/integer,
                           Id:32/integer,
                           LeaderId:32/integer,
                           Bin0/binary>>) ->
  {Replicas, Bin1} = parse_array(Bin0, fun parse_int32/1),
  {Isrs, Bin} = parse_array(Bin1, fun parse_int32/1),
  Partition = #partition_metadata{ error_code = ErrorCode
                                 , id = Id
                                 , leader_id = LeaderId
                                 , replicas = Replicas
                                 , isrs = Isrs},
  {Partition, Bin}.

parse_int32(<<X:32/integer, Bin/binary>>) -> {X, Bin}.

parse_topic_offsets(<<Size:16/integer, Name:Size/binary, Bin0/binary>>) ->
  {Offsets, Bin} = parse_array(Bin0, fun parse_offset/1),
  {#topic_offsets{topic = Name, offsets = Offsets}, Bin}.

parse_offset(<<PartitionId:32/integer,
               ErrorCode:16/integer,
               Offset:64/integer,
               Bin/binary>>) ->
  Res = #offset{ partition_id = PartitionId
               , error_code = ErrorCode
               , offset = Offset},
  {Res, Bin}.

kafka_size(<<"">>) -> -1;
kafka_size(Bin)    -> size(Bin).

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
