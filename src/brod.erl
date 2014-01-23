%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).

%% API
-export([ connect/1
        , close_connection/1
        , get_metadata/1
        , get_metadata/2
        , prepare_data/2
        , prepare_data/3
        , prepare_data/4
        , produce/4
        ]).

%%%_* Includes -----------------------------------------------------------------
-include_lib("brod/include/brod.hrl").

%%%_* Types --------------------------------------------------------------------
-type connection() :: pid().

%%%_* API ----------------------------------------------------------------------
%% @doc Open connection to kafka broker
-spec connect([{string(), integer()}]) -> {ok, connection()} | {error, any()}.
connect(Hosts) ->
  brod_srv:start_link(Hosts).

%% @doc Close connection to kafka broker
-spec close_connection(connection()) -> ok.
close_connection(Conn) ->
  brod_srv:stop(Conn).

%% @doc Get metadata for all topics.
-spec get_metadata(connection()) -> #metadata{} | {error, any()}.
get_metadata(Conn) ->
  get_metadata(Conn, []).

%% @doc Get metadata for specified topics.
-spec get_metadata(connection(), [binary()]) -> #metadata{} | {error, any()}.
get_metadata(Conn, Topics) ->
  brod_srv:get_metadata(Conn, Topics).

%% @equiv prepare_data(Topic, 0, <<>>, Value)
-spec prepare_data(binary(), binary()) -> #data{}.
prepare_data(Topic, Value) ->
  prepare_data(Topic, 0, <<>>, Value).

%% @equiv prepare_data(Topic, PartitionId, <<>>, Value)
-spec prepare_data(binary(), integer(), binary()) -> #data{}.
prepare_data(Topic, PartitionId, Value) ->
  prepare_data(Topic, PartitionId, <<>>, Value).

%% @doc Helper to build a proper #data{} record for single message
%%      produce requests.
-spec prepare_data(binary(), integer(), binary(), binary()) -> #data{}.
prepare_data(Topic, PartitionId, Key, Value) ->
  Msg = #message{key = Key, value = Value},
  Partitions = [#partition_messages{id = PartitionId, messages = [Msg]}],
  [#data{topic = Topic, partitions = Partitions}].

%% @doc Send messages to the server.
-spec produce(connection(), #data{}, integer(), integer()) ->
                 [#topic_offsets{}] | {error, any()}.
produce(Conn, Data, Acks, Timeout) ->
  brod_srv:produce(Conn, Data, Acks, Timeout).

%%%_* Internal functions -------------------------------------------------------

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
