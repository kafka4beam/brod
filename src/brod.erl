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
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).
-behaviour(application).

%% Application
-export([ start/0
        , start/2
        , stop/0
        , stop/1
        ]).

%% Client API
-export([ get_partitions/2
        , get_producer/3
        , start_link_client/3
        , start_link_client/5
        , stop_client/1
        ]).

%% Producer API
-export([ produce/2
        , produce/3
        , produce/5
        , produce_sync/2
        , produce_sync/3
        , produce_sync/5
        , sync_produce_request/1
        ]).

%% Consumer API
-export([ start_link_consumer/3
        , start_link_consumer/4
        , start_consumer/3
        , stop_consumer/1
        ]).

%% Management and testing API
-export([ get_metadata/1
        , get_metadata/2
        , get_offsets/3
        , get_offsets/5
        , fetch/4
        , fetch/7
        , console_consumer/4
        , file_consumer/5
        , simple_consumer/5
        ]).

%% escript
-export([main/1]).

-include("brod_int.hrl").

%%%_* APIs =====================================================================

%% @doc Start brod application.
start() -> application:start(brod).

%% @doc Stop brod application.
stop() -> application:stop(brod).

%% @doc Application behaviour callback
start(_StartType, _StartArgs) -> brod_sup:start_link().

%% @doc Application behaviour callback
stop(_State) -> ok.

%% @doc Simple version of start_link_client/4.
%% Deafult client ID and default configs are used.
%% For more details: @see start_link_client/4
%% @end
-spec start_link_client( [endpoint()]
                       , [{topic(), producer_config()}]
                       , [{topic(), consumer_config()}]) ->
                           {ok, pid()} | {error, any()}.
start_link_client(Endpoints, Producers, Consumers) ->
  start_link_client(?BROD_DEFAULT_CLIENT_ID, Endpoints,
                    Producers, Consumers, _Config = []).

%5 @doc Start a client.
%% ClientId:
%%   Atom to identify the client process
%% Endpoints:
%%   Kafka cluster endpoints, can be any of the brokers in the cluster
%%   which does not necessarily have to be a leader of any partition,
%%   e.g. a load-balanced entrypoint to the remote kakfa cluster.
%% Producers:
%%   A list of {Topic, ProducerConfig} where ProducerConfig is a
%%   proplist, @see brod_producers_sup:start_link/2 for more details
%% Consumers:
%%   A list of {Topic, ConsumerConfig} where ConsumerConfig is a
%%   proplist, @see brod_consumers_sup:start_link/2 for more details
%% Config:
%%   Proplist, possible values:
%%     get_metadata_timout_seconds(optional, default=5)
%%       Return timeout error from brod_client:get_metadata/2 in case the
%%       respons is not received from kafka in this configured time.
%%     reconnect_cool_down_seconds(optional, default=1)
%%       Delay this configured number of seconds before retrying to
%%       estabilish a new connection to the kafka partition leader.
% @end
-spec start_link_client( client_id()
                       , [endpoint()]
                       , [{topic(), producer_config()}]
                       , [{topic(), consumer_config()}]
                       , client_config()) ->
                           {ok, pid()} | {error, any()}.
start_link_client(ClientId, Endpoints, Producers, Consumers, Config) ->
  brod_client:start_link(ClientId, Endpoints, Producers, Consumers, Config).

%% @doc Stop a client.
-spec stop_client(client_id()) -> ok.
stop_client(ClientId) ->
  brod_client:stop(ClientId).

%% @doc Get all partition numbers of a given topic.
%% The higher level producers may need the partition numbers to
%% find the partition producer pid --- if the number of partitions
%% is not statically configured for them.
%% It is up to the callers how they want to distribute their data
%% (e.g. random, roundrobin or consistent-hashing) to the partitions.
%% @end
-spec get_partitions(client(), topic()) ->
        {ok, [partition()]} | {error, any()}.
get_partitions(Client, Topic) ->
  brod_client:get_partitions(Client, Topic).

%% @equiv brod_client:get_producer/3
-spec get_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, Reason}
          when Reason :: client_down
                       | {producer_down, noproc}
                       | {producer_not_found, topic()}
                       | {producer_not_found, topic(), partition()}.
get_producer(Client, Topic, Partition) ->
  brod_client:get_producer(Client, Topic, Partition).

%% @equiv produce(Pid, 0, <<>>, Value)
-spec produce(pid(), binary()) -> {ok, reference()} | {error, any()}.
produce(Pid, Value) ->
  produce(Pid, _Key = <<>>, Value).

%% @doc Produce one message. The pid should be a producer pid.
-spec produce(pid(), binary(), binary()) ->
        {ok, brod_call_ref()} | {error, any()}.
produce(ProducerPid, Key, Value) ->
  brod_producer:produce(ProducerPid, Key, Value).

%% @doc Produce one message. This function first lookup the producer
%% pid, then call produce/3 to do the actual job.
%% @end
-spec produce(client(), topic(), partition(), binary(), binary()) ->
        {ok, brod_call_ref()} | {error, any()}.
produce(Client, Topic, Partition, Key, Value) ->
  case get_producer(Client, Topic, Partition) of
    {ok, Pid}       -> produce(Pid, Key, Value);
    {error, Reason} -> {error, Reason}
  end.

%% @equiv produce_sync(Pid, 0, <<>>, Value)
-spec produce_sync(pid(), binary()) -> ok.
produce_sync(Pid, Value) ->
  produce_sync(Pid, _Key = <<>>, Value).

%% @doc Produce one message and wait for ack from kafka.
%% The pid should be a partition producer pid, NOT client pid.
%% @end
-spec produce_sync(pid(), binary(), binary()) ->
        ok | {error, any()}.
produce_sync(Pid, Key, Value) ->
  case produce(Pid, Key, Value) of
    {ok, CallRef} ->
      %% Wait until the request is acked by kafka
      sync_produce_request(CallRef);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Produce one message and wait for ack from kafka.
-spec produce_sync(client(), topic(), partition(), binary(), binary()) ->
        ok | {error, any()}.
produce_sync(Client, Topic, Partition, Key, Value) ->
  case produce(Client, Topic, Partition, Key, Value) of
    {ok, CallRef} ->
      sync_produce_request(CallRef);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Block wait for sent produced request to be acked by kafka.
-spec sync_produce_request(brod_call_ref()) ->
        ok | {error, Reason::any()}.
sync_produce_request(CallRef) ->
  Expect = #brod_produce_reply{ call_ref = CallRef
                              , result   = brod_produce_req_acked
                              },
  brod_producer:sync_produce_request(Expect).

%% @deprecated
%% @equiv start_link_consumer(Hosts, Topic, Partition)
-spec start_consumer([endpoint()], binary(), integer()) ->
                   {ok, pid()} | {error, any()}.
start_consumer(Hosts, Topic, Partition) ->
  start_link_consumer(Hosts, Topic, Partition).

%% @equiv start_link_consumer(Hosts, Topic, Partition, 1000)
-spec start_link_consumer([endpoint()], binary(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_link_consumer(Hosts, Topic, Partition) ->
  start_link_consumer(Hosts, Topic, Partition, 1000).

%% @doc Start consumer process
%%      SleepTimeout: how much time to wait before sending next fetch
%%      request when we got no messages in the last one
-spec start_link_consumer([endpoint()], binary(), integer(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_link_consumer(Hosts, Topic, Partition, SleepTimeout) ->
  brod_consumer:start_link(Hosts, Topic, Partition, SleepTimeout).

%% @doc Stop consumer process
-spec stop_consumer(pid()) -> ok.
stop_consumer(Pid) ->
  brod_consumer:stop(Pid).

%% @doc Fetch broker metadata
-spec get_metadata([endpoint()]) -> {ok, #metadata_response{}} | {error, any()}.
get_metadata(Hosts) ->
  brod_utils:get_metadata(Hosts).

%% @doc Fetch broker metadata
-spec get_metadata([endpoint()], [binary()]) ->
                      {ok, #metadata_response{}} | {error, any()}.
get_metadata(Hosts, Topics) ->
  brod_utils:get_metadata(Hosts, Topics).

%% @equiv get_offsets(Hosts, Topic, Partition, -1, 1)
-spec get_offsets([endpoint()], binary(), non_neg_integer()) ->
                     {ok, #offset_response{}} | {error, any()}.
get_offsets(Hosts, Topic, Partition) ->
  get_offsets(Hosts, Topic, Partition, -1, 1).

%% @doc Get valid offsets for a specified topic/partition
-spec get_offsets([endpoint()], binary(), non_neg_integer(),
                  integer(), non_neg_integer()) ->
                     {ok, #offset_response{}} | {error, any()}.
get_offsets(Hosts, Topic, Partition, Time, MaxNOffsets) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  Request = #offset_request{ topic = Topic
                           , partition = Partition
                           , time = Time
                           , max_n_offsets = MaxNOffsets},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  ok = brod_sock:stop(Pid),
  Response.

%% @equiv fetch(Hosts, Topic, Partition, Offset, 1000, 0, 100000)
-spec fetch([endpoint()], binary(), non_neg_integer(), integer()) ->
               {ok, [#message{}]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset) ->
  fetch(Hosts, Topic, Partition, Offset, 1000, 0, 100000).

%% @doc Fetch a single message set from a specified topic/partition
-spec fetch([endpoint()], binary(), non_neg_integer(),
            integer(), non_neg_integer(), non_neg_integer(),
            pos_integer()) ->
               {ok, [#message{}]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  Request = #fetch_request{ topic = Topic
                          , partition = Partition
                          , offset = Offset
                          , max_wait_time = MaxWaitTime
                          , min_bytes = MinBytes
                          , max_bytes = MaxBytes},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  #fetch_response{topics = [TopicFetchData]} = Response,
  #topic_fetch_data{ topic = Topic
                   , partitions = [PM]} = TopicFetchData,
  #partition_messages{ error_code = ErrorCode
                     , messages = Messages} = PM,
  ok = brod_sock:stop(Pid),
  case brod_kafka:is_error(ErrorCode) of
    true ->
      {error, brod_kafka_errors:desc(ErrorCode)};
    false ->
      {ok, Messages}
  end.

console_consumer(Hosts, Topic, Partition, Offset) ->
  simple_consumer(Hosts, Topic, Partition, Offset, user).

file_consumer(Hosts, Topic, Partition, Offset, Filename) ->
  {ok, File} = file:open(Filename, [write, append]),
  C = simple_consumer(Hosts, Topic, Partition, Offset, File),
  MonitorFun = fun() ->
                   Mref = erlang:monitor(process, C),
                   receive
                     {'DOWN', Mref, process, C, _} ->
                       io:format("~nClosing ~s~n", [Filename]),
                       file:close(File)
                   end
               end,
  proc_lib:spawn(fun() -> MonitorFun() end),
  C.

simple_consumer(Hosts, Topic, Partition, Offset, Io) ->
  {ok, C} = brod:start_link_consumer(Hosts, Topic, Partition),
  Pid = proc_lib:spawn_link(fun() -> simple_consumer_loop(C, Io) end),
  Callback = fun(MsgOffset, K, V) -> print_message(Io, MsgOffset, K, V) end,
  ok = brod:consume(C, Callback, Offset),
  Pid.

%% escript entry point
main([]) ->
  show_help();
main(["help"]) ->
  show_help();
main(Args) ->
  case length(Args) < 2 of
    true  -> erlang:exit("I expect at least 2 arguments. Please see ./brod help.");
    false -> ok
  end,
  [F | Tail] = Args,
  io:format("~p~n", [call_api(list_to_atom(F), Tail)]).

show_help() ->
  io:format(user, "Command line interface for brod.\n", []),
  io:format(user, "General patterns:\n", []),
  io:format(user, "  ./brod <comma-separated list of kafka host:port pairs> <function name> <function arguments>\n", []),
  io:format(user, "  ./brod <kafka host:port pair> <function name> <function arguments>\n", []),
  io:format(user, "  ./brod <kafka host name (9092 is used by default)> <function name> <function arguments>\n", []),
  io:format(user, "\n", []),
  io:format(user, "Examples:\n", []),
  io:format(user, "Get metadata:\n", []),
  io:format(user, "  ./brod get_metadata Hosts Topic1[,Topic2]\n", []),
  io:format(user, "  ./brod get_metadata kafka-1:9092,kafka-2:9092,kafka-3:9092\n", []),
  io:format(user, "  ./brod get_metadata kafka-1:9092\n", []),
  io:format(user, "  ./brod get_metadata kafka-1:9092  topic1,topic2\n", []),
  io:format(user, "  ./brod get_metadata kafka-1 topic1\n", []),
  io:format(user, "Produce:\n", []),
  io:format(user, "  ./brod produce Hosts Topic Partition Key:Value\n", []),
  io:format(user, "  ./brod produce kafka-1 topic1 0 key:value\n", []),
  io:format(user, "  ./brod produce kafka-1 topic1 0 :value\n", []),
  io:format(user, "This one can be used to generate a delete marker for compacted topic:\n", []),
  io:format(user, "  ./brod produce kafka-1 topic1 0 key:\n", []),
  io:format(user, "Get offsets:\n", []),
  io:format(user, "  ./brod get_offsets Hosts Topic Partition Time MaxNOffsets\n", []),
  io:format(user, "  ./brod get_offsets kafka-1 topic1 0 -1 1\n", []),
  io:format(user, "Fetch:\n", []),
  io:format(user, "  ./brod fetch Hosts Topic Partition Offset\n", []),
  io:format(user, "  ./brod fetch Hosts Topic Partition Offset MaxWaitTime MinBytes MaxBytes\n", []),
  io:format(user, "  ./brod fetch kafka-1 topic1 0 -1\n", []),
  io:format(user, "  ./brod fetch kafka-1 topic1 0 -1 1000 0 100000\n", []),
  io:format(user, "Start a console consumer:\n", []),
  io:format(user, "  ./brod console_consumer Hosts Topic Partition Offset\n", []),
  io:format(user, "  ./brod console_consumer kafka-1 topic1 0 -1\n", []),
  io:format(user, "Start a file consumer:\n", []),
  io:format(user, "  ./brod file_consumer Hosts Topic Partition Offset Filepath\n", []),
  io:format(user, "  ./brod file_consumer kafka-1 topic1 0 -1 /tmp/kafka-data\n", []),
  ok.

call_api(get_metadata, [HostsStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  brod:get_metadata(Hosts);
call_api(get_metadata, [HostsStr, TopicsStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  Topics = [list_to_binary(T) || T <- string:tokens(TopicsStr, ",")],
  brod:get_metadata(Hosts, Topics);
call_api(produce, [HostsStr, TopicStr, PartitionStr, KVStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  Topic = list_to_binary(TopicStr),
  Partition = list_to_integer(PartitionStr),
  Pos = string:chr(KVStr, $:),
  Key = iolist_to_binary(string:left(KVStr, Pos - 1)),
  Value = iolist_to_binary(string:right(KVStr, length(KVStr) - Pos)),
  {ok, Client} = brod:start_link_client(Hosts, [{Topic, []}]),
  {ok, ProducerPid} = brod:get_producer(Client, Topic, Partition),
  Res = brod:produce_sync(ProducerPid, Key, Value),
  brod:stop_client(Client),
  Res;
call_api(get_offsets, [HostsStr, TopicStr, PartitionStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  brod:get_offsets(Hosts, list_to_binary(TopicStr), list_to_integer(PartitionStr));
call_api(get_offsets, [HostsStr, TopicStr, PartitionStr, TimeStr, MaxOffsetStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  brod:get_offsets(Hosts,
                   list_to_binary(TopicStr),
                   list_to_integer(PartitionStr),
                   list_to_integer(TimeStr),
                   list_to_integer(MaxOffsetStr));
call_api(fetch, [HostsStr, TopicStr, PartitionStr, OffsetStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  brod:fetch(Hosts,
             list_to_binary(TopicStr),
             list_to_integer(PartitionStr),
             list_to_integer(OffsetStr));
call_api(fetch, [HostsStr, TopicStr, PartitionStr, OffsetStr,
                 MaxWaitTimeStr, MinBytesStr, MaxBytesStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  brod:fetch(Hosts,
             list_to_binary(TopicStr),
             list_to_integer(PartitionStr),
             list_to_integer(OffsetStr),
             list_to_integer(MaxWaitTimeStr),
             list_to_integer(MinBytesStr),
             list_to_integer(MaxBytesStr));
call_api(console_consumer, [HostsStr, TopicStr, PartitionStr, OffsetStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  Pid = brod:console_consumer(Hosts,
                              list_to_binary(TopicStr),
                              list_to_integer(PartitionStr),
                              list_to_integer(OffsetStr)),
  MRef = erlang:monitor(process, Pid),
  receive
    {'DOWN', MRef, process, Pid, _} -> ok
  end;
call_api(file_consumer, [HostsStr, TopicStr, PartitionStr, OffsetStr, Filename]) ->
  Hosts = parse_hosts_str(HostsStr),
  Pid = brod:file_consumer(Hosts,
                           list_to_binary(TopicStr),
                           list_to_integer(PartitionStr),
                           list_to_integer(OffsetStr),
                           Filename),
  MRef = erlang:monitor(process, Pid),
  receive
    {'DOWN', MRef, process, Pid, _} -> ok
  end.

%%%_* Internal functions =======================================================

parse_hosts_str(HostsStr) ->
  F = fun(HostPortStr) ->
          Pair = string:tokens(HostPortStr, ":"),
          case Pair of
            [Host, PortStr] -> {Host, list_to_integer(PortStr)};
            [Host]          -> {Host, 9092}
          end
      end,
  lists:map(F, string:tokens(HostsStr, ",")).

connect_leader(Hosts, Topic, Partition) ->
  {ok, Metadata} = get_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  #topic_metadata{partitions = Partitions} =
    lists:keyfind(Topic, #topic_metadata.name, Topics),
  #partition_metadata{leader_id = Id} =
    lists:keyfind(Partition, #partition_metadata.id, Partitions),
  Broker = lists:keyfind(Id, #broker_metadata.node_id, Brokers),
  Host = Broker#broker_metadata.host,
  Port = Broker#broker_metadata.port,
  %% client id matters only for producer clients
  brod_sock:start_link(self(), Host, Port, ?BROD_DEFAULT_CLIENT_ID, []).

print_message(Io, Offset, K, V) ->
  io:format(Io, "[~p] ~s:~s\n", [Offset, K, V]).

simple_consumer_loop(C, Io) ->
  receive
    stop ->
      brod:stop_consumer(C),
      io:format(Io, "\Simple consumer ~p terminating\n", [self()]),
      ok;
    Other ->
      io:format(Io, "\nStrange msg: ~p\n", [Other]),
      simple_consumer_loop(C, Io)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
