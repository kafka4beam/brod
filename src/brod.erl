%%%
%%%   Copyright (c) 2014-2016, Klarna AB
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
%%% @copyright 2014-2016 Klarna AB
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
-export([ get_partitions_count/2
        , start_client/1
        , start_client/2
        , start_client/3
        , start_consumer/3
        , start_producer/3
        , stop_client/1
        ]).

%% Client API (deprecated)
-export([ start_link_client/1
        , start_link_client/2
        , start_link_client/3
        ]).

%% Producer API
-export([ get_producer/3
        , produce/2
        , produce/3
        , produce/5
        , produce_sync/2
        , produce_sync/3
        , produce_sync/5
        , sync_produce_request/1
        ]).

%% Simple Consumer API
-export([ consume_ack/2
        , consume_ack/4
        , get_consumer/3
        , subscribe/3
        , subscribe/5
        , unsubscribe/1
        , unsubscribe/2
        , unsubscribe/3
        , unsubscribe/4
        ]).

%% Subscriber API
-export([ start_link_group_subscriber/7
        , start_link_topic_subscriber/5
        , start_link_topic_subscriber/6
        ]).

%% Management and testing API
-export([ get_metadata/1
        , get_metadata/2
        , get_offsets/3
        , get_offsets/5
        , fetch/4
        , fetch/7
        ]).

%% escript
-export([main/1]).

-deprecated([{start_link_client, '_', next_version}]).

-export_type([ brod_call_ref/0
             , brod_client_id/0
             , brod_partition_fun/0
             , client/0
             , client_config/0
             , consumer_config/0
             , consumer_options/0
             , endpoint/0
             , group_config/0
             , group_id/0
             , key/0
             , kpro_MetadataResponse/0
             , kv_list/0
             , offset/0
             , offset_time/0
             , producer_config/0
             , topic/0
             , value/0
             ]).

-include("brod_int.hrl").

%%%_* APIs =====================================================================

%% @doc Start brod application.
-spec start() -> ok | no_return().
start() ->
  {ok, _Apps} = application:ensure_all_started(brod),
  ok.

%% @doc Stop brod application.
-spec stop() -> ok.
stop() ->
  application:stop(brod).

%% @doc Application behaviour callback
start(_StartType, _StartArgs) -> brod_sup:start_link().

%% @doc Application behaviour callback
stop(_State) -> ok.

%% @equiv stat_client(BootstrapEndpoints, brod_default_client)
-spec start_client([endpoint()]) -> ok | {error, any()}.
start_client(BootstrapEndpoints) ->
  start_client(BootstrapEndpoints, ?BROD_DEFAULT_CLIENT_ID).

%% @equiv stat_client(BootstrapEndpoints, ClientId, [])
-spec start_client([endpoint()], brod_client_id()) -> ok | {error, any()}.
start_client(BootstrapEndpoints, ClientId) ->
  start_client(BootstrapEndpoints, ClientId, []).

%% @doc Start a client.
%% BootstrapEndpoints:
%%   Kafka cluster endpoints, can be any of the brokers in the cluster
%%   which does not necessarily have to be a leader of any partition,
%%   e.g. a load-balanced entrypoint to the remote kakfa cluster.
%% ClientId:
%%   Atom to identify the client process
%% Config:
%%   Proplist, possible values:
%%     restart_delay_seconds (optional, default=10)
%%       How much time to wait between attempts to restart brod_client
%%       process when it crashes
%%     max_metadata_sock_retry (optional, default=1)
%%       Number of retries if failed fetching metadata due to socket error
%%     get_metadata_timeout_seconds(optional, default=5)
%%       Return timeout error from brod_client:get_metadata/2 in case the
%%       respons is not received from kafka in this configured time.
%%     reconnect_cool_down_seconds (optional, default=1)
%%       Delay this configured number of seconds before retrying to
%%       estabilish a new connection to the kafka partition leader.
%%     allow_topic_auto_creation (optional, default=true)
%%       By default, brod respects what is configured in broker about
%%       topic auto-creation. i.e. whatever auto.create.topics.enable
%%       is set in borker configuration.
%%       However if 'allow_topic_auto_creation' is set to 'false' in client
%%       config, brod will avoid sending metadata requests that may cause an
%%       auto-creation of the topic regardless of what the broker config is.
%%     auto_start_producers (optional, default=false)
%%       If true, brod client will spawn a producer automatically when
%%       user is trying to call 'produce' but did not call
%%       brod:start_producer explicitly. Can be useful for applications
%%       which don't know beforehand which topics they will be working with.
%%     default_producer_config (optional, default=[])
%%       Producer configuration to use when auto_start_producers is true.
%%       @see brod_client:start_producer/3. for more details.
%%     ssl (optional, default=false)
%%       true | false | [{certfile, ...},{keyfile, ...},{cacertfile, ...}]
%%       When true, brod will try to upgrade tcp connection to ssl using default
%%       ssl options. List of ssl options implies ssl=true.
%%     connect_timeout (optional, default=5000)
%%       Timeout when trying to connect to one endpoint.
%%     request_timeout (optional, default=120000, constraint: >= 1000)
%%       Timeout when waiting for a response, socket restart when timedout.
%% @end
-spec start_client([endpoint()], brod_client_id(), client_config()) ->
                      ok | {error, any()}.
start_client(BootstrapEndpoints, ClientId, Config) ->
  case brod_sup:start_client(BootstrapEndpoints, ClientId, Config) of
    ok                               -> ok;
    {error, {already_started, _Pid}} -> ok;
    {error, Reason}                  -> {error, Reason}
  end.

%% @equiv stat_link_client(BootstrapEndpoints, brod_default_client)
-spec start_link_client([endpoint()]) -> {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints) ->
  start_link_client(BootstrapEndpoints, ?BROD_DEFAULT_CLIENT_ID).

%% @equiv stat_link_client(BootstrapEndpoints, ClientId, [])
-spec start_link_client([endpoint()], brod_client_id()) ->
                           {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints, ClientId) ->
  start_link_client(BootstrapEndpoints, ClientId, []).

-spec start_link_client([endpoint()], brod_client_id(), client_config()) ->
                           {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints, ClientId, Config) ->
  brod_client:start_link(BootstrapEndpoints, ClientId, Config).

%% @doc Stop a client.
-spec stop_client(client()) -> ok.
stop_client(Client) when is_atom(Client) ->
  case brod_sup:find_client(Client) of
    [_Pid] -> brod_sup:stop_client(Client);
    []     -> brod_client:stop(Client)
  end;
stop_client(Client) when is_pid(Client) ->
  brod_client:stop(Client).

%% @doc Dynamically start a per-topic producer.
%% @see brod_client:start_producer/3. for more details.
%% @end
-spec start_producer(client(), topic(), producer_config()) ->
                        ok | {error, any()}.
start_producer(Client, TopicName, ProducerConfig) ->
  brod_client:start_producer(Client, TopicName, ProducerConfig).

%% @doc Dynamically start a topic consumer.
%% @see brod_client:start_consumer/3. for more details.
%% @end
-spec start_consumer(client(), topic(), consumer_config()) ->
                        ok | {error, any()}.
start_consumer(Client, TopicName, ConsumerConfig) ->
  brod_client:start_consumer(Client, TopicName, ConsumerConfig).

%% @doc Get number of partitions for a given topic.
%% The higher level producers may need the partition numbers to
%% find the partition producer pid --- if the number of partitions
%% is not statically configured for them.
%% It is up to the callers how they want to distribute their data
%% (e.g. random, roundrobin or consistent-hashing) to the partitions.
%% @end
-spec get_partitions_count(client(), topic()) ->
        {ok, pos_integer()} | {error, any()}.
get_partitions_count(Client, Topic) ->
  brod_client:get_partitions_count(Client, Topic).

-spec get_consumer(client(), topic(), partition()) ->
        {ok, pid()} | {error, Reason}
          when Reason :: client_down
                       | {consumer_down, noproc}
                       | {consumer_not_found, topic()}
                       | {consumer_not_found, topic(), partition()}.
get_consumer(Client, Topic, Partition) ->
  brod_client:get_consumer(Client, Topic, Partition).

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
-spec produce(pid(), value()) ->
                 {ok, brod_call_ref()} | {error, any()}.
produce(Pid, Value) ->
  produce(Pid, _Key = <<>>, Value).

%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is discarded (only the keys in kv-list are sent to kafka).
%% The pid should be a partition producer pid, NOT client pid.
%% @end
-spec produce(pid(), key(), value()) ->
        {ok, brod_call_ref()} | {error, any()}.
produce(ProducerPid, Key, Value) ->
  brod_producer:produce(ProducerPid, Key, Value).

%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is used only for partitioning (or discarded if Partition is used
%% instead of PartFun).
%% This function first lookup the producer pid,
%% then call produce/3 to do the real work.
%% @end
-spec produce(client(), topic(), partition() | brod_partition_fun(),
              key(), value()) -> {ok, brod_call_ref()} | {error, any()}.
produce(Client, Topic, PartFun, Key, Value) when is_function(PartFun) ->
  case brod_client:get_partitions_count(Client, Topic) of
    {ok, PartitionsCnt} ->
      {ok, Partition} = PartFun(Topic, PartitionsCnt, Key, Value),
      produce(Client, Topic, Partition, Key, Value);
    {error, Reason} ->
      {error, Reason}
  end;
produce(Client, Topic, Partition, Key, Value) when is_integer(Partition) ->
  case get_producer(Client, Topic, Partition) of
    {ok, Pid}       -> produce(Pid, Key, Value);
    {error, Reason} -> {error, Reason}
  end.

%% @equiv produce_sync(Pid, 0, <<>>, Value)
-spec produce_sync(pid(), value()) -> ok.
produce_sync(Pid, Value) ->
  produce_sync(Pid, _Key = <<>>, Value).

%% @doc Sync version of produce/3
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return onece the messages is buffered in the producer process.
%% @end
-spec produce_sync(pid(), key(), value()) ->
        ok | {error, any()}.
produce_sync(Pid, Key, Value) ->
  case produce(Pid, Key, Value) of
    {ok, CallRef} ->
      %% Wait until the request is acked by kafka
      sync_produce_request(CallRef);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Sync version of produce/5
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages are buffered in the producer process.
%% @end
-spec produce_sync(client(), topic(), partition() | brod_partition_fun(),
                   key(), value()) -> ok | {error, any()}.
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

%% @doc Subscribe data stream from the given topic-partition.
%% If {error, Reason} is returned, the caller should perhaps retry later.
%% {ok, ConsumerPid} is returned if success, the caller may want to monitor
%% the consumer pid to trigger a re-subscribe in case it crashes.
%%
%% If subscribed successfully, the subscriber process should expect messages
%% of pattern:
%% {ConsumerPid, #kafka_message_set{}} and
%% {ConsumerPid, #kafka_fetch_error{}},
%% -include_lib(brod/include/brod.hrl) to access the records.
%% In case #kafka_fetch_error{} is received the subscriber should re-subscribe
%% itself to resume the data stream.
%% @end
-spec subscribe(client(), pid(), topic(), partition(),
                consumer_options()) -> {ok, pid()} | {error, any()}.
subscribe(Client, SubscriberPid, Topic, Partition, Options) ->
  case brod_client:get_consumer(Client, Topic, Partition) of
    {ok, ConsumerPid} ->
      case subscribe(ConsumerPid, SubscriberPid, Options) of
        ok    -> {ok, ConsumerPid};
        Error -> Error
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec subscribe(pid(), pid(), consumer_options()) -> ok | {error, any()}.
subscribe(ConsumerPid, SubscriberPid, Options) ->
  brod_consumer:subscribe(ConsumerPid, SubscriberPid, Options).

%% @doc Unsubscribe the current subscriber. Assuming the subscriber is self().
-spec unsubscribe(client(), topic(), partition()) -> ok | {error, any()}.
unsubscribe(Client, Topic, Partition) ->
  unsubscribe(Client, Topic, Partition, self()).

%% @doc Unsubscribe the current subscriber.
-spec unsubscribe(client(), topic(), partition(), pid()) -> ok | {error, any()}.
unsubscribe(Client, Topic, Partition, SubscriberPid) ->
  case brod_client:get_consumer(Client, Topic, Partition) of
    {ok, ConsumerPid} -> unsubscribe(ConsumerPid, SubscriberPid);
    Error             -> Error
  end.

%% @doc Unsubscribe the current subscriber. Assuming the subscriber is self().
-spec unsubscribe(pid()) -> ok | {error, any()}.
unsubscribe(ConsumerPid) ->
  unsubscribe(ConsumerPid, self()).

%% @doc Unsubscribe the current subscriber.
-spec unsubscribe(pid(), pid()) -> ok | {error, any()}.
unsubscribe(ConsumerPid, SubscriberPid) ->
  brod_consumer:unsubscribe(ConsumerPid, SubscriberPid).

-spec consume_ack(client(), topic(), partition(), offset()) ->
        ok | {error, any()}.
consume_ack(Client, Topic, Partition, Offset) ->
  case brod_client:get_consumer(Client, Topic, Partition) of
    {ok, ConsumerPid} -> consume_ack(ConsumerPid, Offset);
    {error, Reason}   -> {error, Reason}
  end.

-spec consume_ack(pid(), offset()) -> ok | {error, any()}.
consume_ack(ConsumerPid, Offset) ->
  brod_consumer:ack(ConsumerPid, Offset).

%% @equiv brod_group_subscriber:start_link/7
-spec start_link_group_subscriber(
        client(), group_id(), [topic()],
        group_config(), consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_group_subscriber(Client, GroupId, Topics, GroupConfig,
                            ConsumerConfig, CbModule, CbInitArg) ->
  brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
                                   ConsumerConfig, CbModule, CbInitArg).

%% @equiv start_link_topic_subscriber(Client, Topic, 'all', ConsumerConfig,
%%                                    CbModule, CbInitArg)
-spec start_link_topic_subscriber(
        client(), topic(), consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, ConsumerConfig,
                            CbModule, CbInitArg) ->
  start_link_topic_subscriber(Client, Topic, all, ConsumerConfig,
                              CbModule, CbInitArg).

%% @equiv brod_topic_subscriber:start_link/6
-spec start_link_topic_subscriber(
        client(), topic(), all | [partition()],
        consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, Partitions,
                            ConsumerConfig, CbModule, CbInitArg) ->
  brod_topic_subscriber:start_link(Client, Topic, Partitions,
                                   ConsumerConfig, CbModule, CbInitArg).

%% @doc Fetch broker metadata
-spec get_metadata([endpoint()]) ->
                      {ok, kpro_MetadataResponse()} | {error, any()}.
get_metadata(Hosts) ->
  brod_utils:get_metadata(Hosts).

%% @doc Fetch broker metadata
-spec get_metadata([endpoint()], [topic()]) ->
                      {ok, kpro_MetadataResponse()} | {error, any()}.
get_metadata(Hosts, Topics) ->
  brod_utils:get_metadata(Hosts, Topics).

%% @equiv get_offsets(Hosts, Topic, Partition, latest, 1)
-spec get_offsets([endpoint()], topic(), non_neg_integer()) ->
                     {ok, [offset()]} | {error, any()}.
get_offsets(Hosts, Topic, Partition) ->
  get_offsets(Hosts, Topic, Partition, ?OFFSET_LATEST, 1).

%% @doc Get valid offsets for a specified topic/partition
-spec get_offsets([endpoint()], topic(), partition(),
                  offset_time(), pos_integer()) ->
                     {ok, [offset()]} | {error, any()}.
get_offsets(Hosts, Topic, Partition, TimeOrSemanticOffset, MaxNoOffsets) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  try
    brod_utils:fetch_offsets(Pid, Topic, Partition,
                             TimeOrSemanticOffset, MaxNoOffsets)
  after
    ok = brod_sock:stop(Pid)
  end.

%% @equiv fetch(Hosts, Topic, Partition, Offset, 1000, 0, 100000)
-spec fetch([endpoint()], topic(), partition(), integer()) ->
               {ok, [#kafka_message{} | ?incomplete_message]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset) ->
  fetch(Hosts, Topic, Partition, Offset, 1000, 0, 100000).

%% @doc Fetch a single message set from a specified topic/partition
-spec fetch([endpoint()], topic(), partition(), offset_time(),
            non_neg_integer(), non_neg_integer(), pos_integer()) ->
               {ok, [#kafka_message{} | ?incomplete_message]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  Request = kpro:fetch_request(Topic, Partition, Offset,
                               MaxWaitTime, MinBytes, MaxBytes),
  {ok, Response} = brod_sock:request_sync(Pid, Request, 10000),
  #kpro_FetchResponse{fetchResponseTopic_L = [TopicFetchData]} = Response,
  #kpro_FetchResponseTopic{fetchResponsePartition_L = [PM]} = TopicFetchData,
  #kpro_FetchResponsePartition{ errorCode  = ErrorCode
                              , message_L  = Messages
                              } = PM,
  ok = brod_sock:stop(Pid),
  case kpro_ErrorCode:is_error(ErrorCode) of
    true ->
      {error, kpro_ErrorCode:desc(ErrorCode)};
    false ->
      {ok, lists:map(fun brod_utils:kafka_message/1, Messages)}
  end.

%% escript entry point
main([]) ->
  show_help();
main(["help"]) ->
  show_help();
main(Args) ->
  case length(Args) < 2 of
    true ->
      erlang:exit("I expect at least 2 arguments. Please see ./brod help.");
    false ->
      ok
  end,
  [F | Tail] = Args,
  io:format("~p~n", [call_api(list_to_atom(F), Tail)]).

show_help() ->
  io:format(user, "Command line interface for brod.\n", []),
  io:format(user, "General patterns:\n", []),
  io:format(user, "  ./brod <comma-separated list of kafka host:port pairs> "
           "<function name> <function arguments>\n", []),
  io:format(user, "  ./brod <kafka host:port pair> <function name> "
           "<function arguments>\n", []),
  io:format(user, "  ./brod <kafka host name (9092 is used by default)> "
           "<function name> <function arguments>\n", []),
  io:format(user, "\n", []),
  io:format(user, "Examples:\n", []),
  io:format(user, "Get metadata:\n", []),
  io:format(user, "  ./brod get_metadata Hosts Topic1[,Topic2]\n", []),
  io:format(user, "  ./brod get_metadata kafka-1:9092,kafka-2:9092,"
           "kafka-3:9092\n", []),
  io:format(user, "  ./brod get_metadata kafka-1:9092\n", []),
  io:format(user, "  ./brod get_metadata kafka-1:9092  topic1,topic2\n", []),
  io:format(user, "  ./brod get_metadata kafka-1 topic1\n", []),
  io:format(user, "Produce:\n", []),
  io:format(user, "  ./brod produce Hosts Topic Partition Key:Value\n", []),
  io:format(user, "  ./brod produce kafka-1 topic1 0 key:value\n", []),
  io:format(user, "  ./brod produce kafka-1 topic1 0 :value\n", []),
  io:format(user, "This one can be used to generate a delete marker "
           "for compacted topic:\n", []),
  io:format(user, "  ./brod produce kafka-1 topic1 0 key:\n", []),
  io:format(user, "Get offsets:\n", []),
  io:format(user, "  ./brod get_offsets Hosts Topic Partition "
           "Time MaxNOffsets\n", []),
  io:format(user, "  ./brod get_offsets kafka-1 topic1 0 -1 1\n", []),
  io:format(user, "Fetch:\n", []),
  io:format(user, "  ./brod fetch Hosts Topic Partition Offset\n", []),
  io:format(user, "  ./brod fetch Hosts Topic Partition Offset MaxWaitTime "
           "MinBytes MaxBytes\n", []),
  io:format(user, "  ./brod fetch kafka-1 topic1 0 -1\n", []),
  io:format(user, "  ./brod fetch kafka-1 topic1 0 -1 1000 0 100000\n", []),
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
  Client = brod_cli,
  {ok, _Pid} = brod_client:start_link(Hosts, Client, []),
  ok = brod:start_producer(Client, Topic, []),
  Res = brod:produce_sync(Client, Topic, Partition, Key, Value),
  brod_client:stop(Client),
  Res;
call_api(get_offsets, [HostsStr, TopicStr, PartitionStr]) ->
  Hosts = parse_hosts_str(HostsStr),
  brod:get_offsets(Hosts, list_to_binary(TopicStr),
                   list_to_integer(PartitionStr));
call_api(get_offsets, [HostsStr, TopicStr, PartitionStr,
                       TimeStr, MaxOffsetStr]) ->
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
             list_to_integer(MaxBytesStr)).

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

-spec connect_leader([endpoint()], topic(), partition()) -> {ok, pid()}.
connect_leader(Hosts, Topic, Partition) ->
  {ok, Metadata} = get_metadata(Hosts, [Topic]),
  {ok, {Host, Port}} =
    brod_utils:find_leader_in_metadata(Metadata, Topic, Partition),
  %% client id matters only for producer clients
  brod_sock:start_link(self(), Host, Port, ?BROD_DEFAULT_CLIENT_ID, []).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
