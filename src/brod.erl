%%%
%%%   Copyright (c) 2014-2017, Klarna AB
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
        , sync_produce_request/2
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
        , start_link_group_subscriber/8
        , start_link_topic_subscriber/5
        , start_link_topic_subscriber/6
        , start_link_topic_subscriber/7
        ]).

%% APIs for quick metadata or message inspection and brod_cli
-export([ get_metadata/1
        , get_metadata/2
        , get_metadata/3
        , resolve_offset/3
        , resolve_offset/4
        , resolve_offset/5
        , fetch/4
        , fetch/7
        , fetch/8
        , connect_leader/4
        , list_all_groups/2
        , list_groups/2
        , describe_groups/3
        , connect_group_coordinator/3
        , fetch_committed_offsets/2
        , fetch_committed_offsets/3
        ]).

%% escript
-ifdef(BROD_CLI).
-export([main/1]).
-endif.

-export_type([ call_ref/0
             , cg/0
             , cg_protocol_type/0
             , client/0
             , client_config/0
             , client_id/0
             , compression/0
             , consumer_config/0
             , consumer_option/0
             , consumer_options/0
             , corr_id/0
             , endpoint/0
             , error_code/0
             , group_config/0
             , group_generation_id/0
             , group_id/0
             , group_member/0
             , group_member_id/0
             , hostname/0
             , key/0
             , kv_list/0
             , message/0
             , offset/0
             , offset_time/0
             , partition/0
             , partition_assignment/0
             , partition_fun/0
             , portnum/0
             , producer_config/0
             , produce_reply/0
             , produce_result/0
             , received_assignments/0
             , sock_opts/0
             , topic/0
             , value/0
             ]).

-include("brod_int.hrl").

%%%_* Types ====================================================================

%% basics
-type hostname() :: string().
-type portnum() :: pos_integer().
-type endpoint() :: {hostname(), portnum()}.
-type topic() :: kpro:topic().
-type partition() :: kpro:partition().
-type offset() :: kpro:offset().
-type key() :: kpro:key().
-type value() :: kpro:value().
-type kv_list() :: kpro:kv_list().
-type client_id() :: atom().
-type client() :: client_id() | pid().
-type client_config() :: brod_client:config().
-type offset_time() :: integer()
                     | ?OFFSET_EARLIEST
                     | ?OFFSET_LATEST.
-type message() :: kpro:message().
-type error_code() :: kpro:error_code().

%% producers
-type produce_reply() :: #brod_produce_reply{}.
-type producer_config() :: brod_producer:config().
-type partition_fun() :: fun((topic(), partition(), key(), value()) ->
                                {ok, partition()}).
-type compression() :: no_compression | gzip | snappy.
-type call_ref() :: #brod_call_ref{}.
-type produce_result() :: brod_produce_req_buffered
                        | brod_produce_req_acked.


%% consumers
-type consumer_option() :: begin_offset
                         | min_bytes
                         | max_bytes
                         | max_wait_time
                         | sleep_timeout
                         | prefetch_count.
-type consumer_options() :: [{consumer_option(), integer()}].
-type consumer_config() :: brod_consumer:config().

%% consumer groups
-type group_id() :: binary().
-type group_member_id() :: binary().
-type group_member() :: {group_member_id(), #kafka_group_member_metadata{}}.
-type group_generation_id() :: non_neg_integer().
-type group_config() :: proplists:proplist().
-type partition_assignment() :: {topic() , [partition()]}.
-type received_assignments() :: [#brod_received_assignment{}].
-type cg() :: #brod_cg{}.
-type cg_protocol_type() :: binary().

%% internals
-type corr_id() :: kpro:corr_id().
-type sock_opts() :: brod_sock:options().

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
-spec start_client([endpoint()], client_id()) -> ok | {error, any()}.
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
%%       @see brod_producer:start_link/4. for details about producer config
%%     ssl (optional, default=false)
%%       true | false | [{certfile, ...},{keyfile, ...},{cacertfile, ...}]
%%       When true, brod will try to upgrade tcp connection to ssl using default
%%       ssl options. List of ssl options implies ssl=true.
%%     sasl (optional, default=undefined)
%%       Credentials for SASL/Plain authentication.
%%       {plain, "username", "password"}
%%     connect_timeout (optional, default=5000)
%%       Timeout when trying to connect to one endpoint.
%%     request_timeout (optional, default=240000, constraint: >= 1000)
%%       Timeout when waiting for a response, socket restart when timedout.
%% @end
-spec start_client([endpoint()], client_id(), client_config()) ->
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
-spec start_link_client([endpoint()], client_id()) ->
        {ok, pid()} | {error, any()}.
start_link_client(BootstrapEndpoints, ClientId) ->
  start_link_client(BootstrapEndpoints, ClientId, []).

-spec start_link_client([endpoint()], client_id(), client_config()) ->
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
%% @see brod_producer:start_link/4. for details about producer config.
%% @end
-spec start_producer(client(), topic(), producer_config()) ->
                        ok | {error, any()}.
start_producer(Client, TopicName, ProducerConfig) ->
  brod_client:start_producer(Client, TopicName, ProducerConfig).

%% @doc Dynamically start a topic consumer.
%% @see brod_consumer:start_link/5. for details about consumer config.
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
-spec produce(pid(), value()) -> {ok, call_ref()} | {error, any()}.
produce(Pid, Value) ->
  produce(Pid, _Key = <<>>, Value).

%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is discarded (only the keys in kv-list are sent to kafka).
%% The pid should be a partition producer pid, NOT client pid.
%% @end
-spec produce(pid(), key(), value()) ->
        {ok, call_ref()} | {error, any()}.
produce(ProducerPid, Key, Value) ->
  brod_producer:produce(ProducerPid, Key, Value).

%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is used only for partitioning (or discarded if Partition is used
%% instead of PartFun).
%% This function first lookup the producer pid,
%% then call produce/3 to do the real work.
%% @end
-spec produce(client(), topic(), partition() | partition_fun(),
              key(), value()) -> {ok, call_ref()} | {error, any()}.
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
-spec produce_sync(client(), topic(), partition() | partition_fun(),
                   key(), value()) -> ok | {error, any()}.
produce_sync(Client, Topic, Partition, Key, Value) ->
  case produce(Client, Topic, Partition, Key, Value) of
    {ok, CallRef} ->
      sync_produce_request(CallRef);
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Block wait for sent produced request to be acked by kafka.
-spec sync_produce_request(call_ref()) ->
        ok | {error, Reason :: any()}.
sync_produce_request(CallRef) ->
  sync_produce_request(CallRef, infinity).

-spec sync_produce_request(call_ref(), infinity | timeout()) ->
        ok | {error, Reason :: any()}.
sync_produce_request(CallRef, Timeout) ->
  Expect = #brod_produce_reply{ call_ref = CallRef
                              , result   = brod_produce_req_acked
                              },
  brod_producer:sync_produce_request(Expect, Timeout).

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

%% @equiv brod_group_subscriber:start_link/8
-spec start_link_group_subscriber(
        client(), group_id(), [topic()], group_config(),
        consumer_config(), message | message_type,
        module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_group_subscriber(Client, GroupId, Topics, GroupConfig,
                            ConsumerConfig, MessageType,
                            CbModule, CbInitArg) ->
  brod_group_subscriber:start_link(Client, GroupId, Topics, GroupConfig,
                                   ConsumerConfig, MessageType,
                                   CbModule, CbInitArg).

%% @equiv start_link_topic_subscriber(Client, Topic, 'all', ConsumerConfig,
%%                                    CbModule, CbInitArg)
-spec start_link_topic_subscriber(
        client(), topic(), consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, ConsumerConfig,
                            CbModule, CbInitArg) ->
  start_link_topic_subscriber(Client, Topic, all, ConsumerConfig,
                              CbModule, CbInitArg).

%% @equiv start_link_topic_subscriber(Client, Topic, Partitions,
%%                                    ConsumerConfig, message,
%%                                    CbModule, CbInitArg)
-spec start_link_topic_subscriber(
        client(), topic(), all | [partition()],
        consumer_config(), module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, Partitions,
                            ConsumerConfig, CbModule, CbInitArg) ->
  start_link_topic_subscriber(Client, Topic, Partitions,
                              ConsumerConfig, message, CbModule, CbInitArg).

%% @equiv brod_topic_subscriber:start_link/7
-spec start_link_topic_subscriber(
        client(), topic(), all | [partition()],
        consumer_config(), message | message_set,
        module(), term()) ->
          {ok, pid()} | {error, any()}.
start_link_topic_subscriber(Client, Topic, Partitions,
                            ConsumerConfig, MessageType, CbModule, CbInitArg) ->
  brod_topic_subscriber:start_link(Client, Topic, Partitions,
                                   ConsumerConfig, MessageType,
                                   CbModule, CbInitArg).

%% @doc Fetch broker metadata
%% Return the message body of metadata_response.
%% See kpro_schema.erl for details
%% @end
-spec get_metadata([endpoint()]) -> {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts) ->
  brod_utils:get_metadata(Hosts).

%% @doc Fetch broker metadata
%% Return the message body of metadata_response.
%% See `kpro_schema.erl' for struct details
%% @end
-spec get_metadata([endpoint()], [topic()]) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics) ->
  brod_utils:get_metadata(Hosts, Topics).

%% @doc Fetch broker metadata
%% Return the message body of metadata_response.
%% See `kpro_schema.erl' for struct details
%% @end
-spec get_metadata([endpoint()], [topic()], sock_opts()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics, Options) ->
  brod_utils:get_metadata(Hosts, Topics, Options).

%% @equiv resolve_offset(Hosts, Topic, Partition, latest, 1)
-spec resolve_offset([endpoint()], topic(), partition()) ->
        {ok, [offset()]} | {error, any()}.
resolve_offset(Hosts, Topic, Partition) ->
  resolve_offset(Hosts, Topic, Partition, ?OFFSET_LATEST).

%% @doc Resolve semantic offset or timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(), offset_time()) ->
        {ok, [offset()]} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time) ->
  resolve_offset(Hosts, Topic, Partition, Time, []).

%% @doc Resolve semantic offset or timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(),
                     offset_time(), sock_opts()) ->
        {ok, [offset()]} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time, Options) when is_list(Options) ->
  brod_utils:resolve_offset(Hosts, Topic, Partition, Time, Options).

%% @equiv fetch(Hosts, Topic, Partition, Offset, 1000, 0, 100000)
-spec fetch([endpoint()], topic(), partition(), integer()) ->
               {ok, [#kafka_message{}]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset) ->
  fetch(Hosts, Topic, Partition, Offset,
        _MaxWaitTime = 1000, _MinBytes = 0, _MaxBytes = 100000).

%% @equiv fetch(Hosts, Topic, Partition, Offset, Wait, MinBytes, MaxBytes, [])
-spec fetch([endpoint()], topic(), partition(), offset(),
            non_neg_integer(), non_neg_integer(), pos_integer()) ->
               {ok, [#kafka_message{}]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes, []).

%% @doc Fetch a single message set from the given topic-partition.
-spec fetch([endpoint()], topic(), partition(), offset(),
            non_neg_integer(), non_neg_integer(), pos_integer(),
            sock_opts()) -> {ok, [#kafka_message{}]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset,
      MaxWaitTime, MinBytes, MaxBytes, Options) ->
  brod_utils:fetch(Hosts, Topic, Partition, Offset,
                   MaxWaitTime, MinBytes, MaxBytes, Options).

%% @doc Connect partition leader.
-spec connect_leader([endpoint()], topic(), partition(),
                     sock_opts()) -> {ok, pid()}.
connect_leader(Hosts, Topic, Partition, Options) ->
  {ok, Metadata} = get_metadata(Hosts, [Topic], Options),
  {ok, {Host, Port}} =
    brod_utils:find_leader_in_metadata(Metadata, Topic, Partition),
  %% client id matters only for producer clients
  brod_sock:start_link(self(), Host, Port, ?BROD_DEFAULT_CLIENT_ID, Options).

%% @doc List ALL consumer groups in the given kafka cluster.
%% NOTE: Exception if failed against any of the coordinator brokers.
%% @end
-spec list_all_groups([endpoint()], sock_opts()) ->
        [{endpoint(), [cg()] | {error, any()}}].
list_all_groups(Hosts, Options) ->
  brod_utils:list_all_groups(Hosts, Options).

%% @doc List consumer groups in the given group coordinator broker.
-spec list_groups(endpoint(), sock_opts()) -> {ok, [cg()]} | {error, any()}.
list_groups(Hosts, Options) ->
  brod_utils:list_groups(Hosts, Options).

%% @doc Describe consumer groups. The given consumer group IDs should be all
%% managed by the coordinator-broker running at the given endpoint.
%% Otherwise error codes will be returned in the result structs.
%% Return `describe_groups_response' response body field named `groups'.
%% See `kpro_schema.erl' for struct details
%% @end
-spec describe_groups(endpoint(), sock_opts(), [group_id()]) ->
        {ok, [kpro:struct()]} | {error, any()}.
describe_groups(CoordinatorEndpoint, SockOpts, IDs) ->
  brod_utils:describe_groups(CoordinatorEndpoint, SockOpts, IDs).

%% @doc Connect to consumer group coordinator broker.
%% Done in steps: 1) connect to any of the given bootstrap ednpoints;
%% 2) send group_coordinator_request to resolve group coordinator endpoint;;
%% 3) connect to the resolved endpoint and return the brod_sock pid
%% @end
-spec connect_group_coordinator([endpoint()], sock_opts(), group_id()) ->
        {ok, pid()} | {error, any()}.
connect_group_coordinator(BootstrapEndpoints, SockOpts, GroupId) ->
  brod_utils:connect_group_coordinator(BootstrapEndpoints, SockOpts, GroupId).

%% @doc Fetch committed offsets for ALL topics in the given consumer group.
%% Return the `responses' field of the `offset_fetch_response' response.
%% See `kpro_schema.erl' for struct details.
%% @end
-spec fetch_committed_offsets([endpoint()], sock_opts(), group_id()) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(BootstrapEndpoints, SockOpts, GroupId) ->
  brod_utils:fetch_committed_offsets(BootstrapEndpoints, SockOpts, GroupId).

%% @doc Same as `fetch_committed_offsets/3', only work on the socket
%% connected to the group coordinator broker.
%% @end
-spec fetch_committed_offsets(pid(), group_id()) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(SockPid, GroupId) ->
  brod_utils:fetch_committed_offsets(SockPid, GroupId).

-ifdef(BROD_CLI).
main(Args) -> brod_cli:main(Args, halt).
-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
