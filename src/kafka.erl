%%%=============================================================================
%%% @doc Kafka protocol
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(kafka).

%% API
-export([ metadata_request/1
        , metadata_response/1
        ]).

%%%_* Macros -------------------------------------------------------------------

-define(CLIENT_ID,      <<"brod">>).
-define(CLIENT_ID_SIZE, 4).

-define(API_VERSION, 0).

%% Api keys
-define(API_KEY_PRODUCE,        0).
-define(API_KEY_FETCH,          1).
-define(API_KEY_OFFSET,         2).
-define(API_KEY_METADATA,       3).
-define(API_KEY_LEADER_AND_ISR, 4).
-define(API_KEY_STOP_REPLICA,   5).
-define(API_KEY_OFFSET_COMMIT,  6).
-define(API_KEY_OFFSET_FETCH,   7).

%%%_* API ----------------------------------------------------------------------
metadata_request(CorrId) ->
  Header = header(?API_KEY_METADATA, CorrId),
  Body = <<0:32/integer, -1:16/integer>>,
  Request = <<Header/binary, Body/binary>>,
  Size = byte_size(Request),
  <<Size:32/integer, Request/binary>>.

metadata_response(Bin0) ->
  {Brokers, Bin} = parse_array(Bin0, fun parse_broker/1),
  {Topics, _} = parse_array(Bin, fun parse_topic/1),
  {Brokers, Topics}.

%%%_* Internal functions -------------------------------------------------------
header(ApiKey, CorrId) ->
  <<ApiKey:16/integer,
    ?API_VERSION:16/integer,
    CorrId:32/integer,
    ?CLIENT_ID_SIZE:16/integer,
    ?CLIENT_ID/binary>>.

parse_array(<<Size:32/integer, Bin/binary>>, Fun) ->
  parse_array(Size, Bin, [], Fun).

parse_array(0, Bin, Acc, _Fun) ->
  {Acc, Bin};
parse_array(Size, Bin0, Acc, Fun) ->
  {Item, Bin} = Fun(Bin0),
  parse_array(Size - 1, Bin, [Item | Acc], Fun).

parse_broker(<<NodeID:32/integer,
               HostSize:16/integer,
               Host:HostSize/binary,
               Port:32/integer,
               Bin/binary>>) ->
  Broker = [{node_id, NodeID}, {host, binary_to_list(Host)}, {port, Port}],
  {Broker, Bin}.

parse_topic(<<Size:32/integer, Name:Size/binary, Bin0/binary>>) ->
  {Partitions, Bin} = parse_array(Bin0, fun parse_partition/1),
  {[{name, binary_to_list(Name)}, {partitions, Partitions}], Bin}.

parse_partition(<<ErrorCode:16/integer,
                  Id:32/integer,
                  LeaderId:32/integer,
                  Bin0/binary>>) ->
  {Replicas, Bin1} = parse_array(Bin0, fun parse_replica/1),
  {Isrs, Bin} = parse_array(Bin1, fun parse_isr/1),
  Partition = [ {error_code, ErrorCode}
              , {id, Id}
              , {leader_id, LeaderId}
              , {replicas, Replicas}
              , {isrs, Isrs}],
  {Partition, Bin}.

parse_replica(<<Replica:32/integer, Bin/binary>>) -> {Replica, Bin}.

parse_isr(<<Isr:32/integer, Bin/binary>>) -> {Isr, Bin}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
