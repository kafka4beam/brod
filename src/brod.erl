%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).

%% Producer API
-export([ start_producer/1
        , start_producer/2
        , stop_producer/1
        , produce/3
        , produce/4
        , produce/5
        ]).

%% Consumer API
-export([ start_consumer/3
        , stop_consumer/1
        , consume/6
        ]).

%% Management and testing API
-export([ get_metadata/1
        , get_metadata/2
        , get_offsets/5
        , fetch/7
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* API ----------------------------------------------------------------------
%% @equiv start_producer(Hosts, [])
-spec start_producer([{string(), integer()}]) ->
                        {ok, pid()} | {error, any()}.
start_producer(Hosts) ->
  start_producer(Hosts, []).

%% @doc Start a process to publish messages to kafka.
%%      <br/>
%%      Hosts: list of "bootstrap" kafka nodes, {"hostname", 1234}
%%      <br/>
%%      Options:
%%      <dl>
%%        <dt>``{required_acks, integer()}''</dt>
%%          <dd>How many acknowledgements the servers should receive
%%              before responding to the request. If it is 0 the
%%              server will not send any response (this is the only
%%              case where the server will not reply to a request). If
%%              it is 1, the server will wait the data is written to
%%              the local log before sending a response. If it is -1
%%              the server will block until the message is committed
%%              by all in sync replicas before sending a response. For
%%              any number > 1 the server will block waiting for this
%%              number of acknowledgements to occur (but the server
%%              will never wait for more acknowledgements than there
%%              are in-sync replicas).<br/>Default: 1</dd>
%%        <dt>``{timeout, integer()}''</dt>
%%          <dd>Maximum time in milliseconds the server can await the
%%              receipt of the number of acknowledgements in
%%              required_acks. The timeout is not an exact limit on
%%              the request time for a few reasons: (1) it does not
%%              include network latency, (2) the timer begins at the
%%              beginning of the processing of this request so if many
%%              requests are queued due to server overload that wait
%%              time will not be included, (3) we will not terminate a
%%              local write so if the local write time exceeds this
%%              timeout it will not be respected.<br/>Default:
%%              1000.</dd>
%%        <dt>``{flush_threshold, integer()}''</dt>
%%          <dd>Producer keeps incoming messages in a buffer until
%%              buffer size reaches flush_threshold limit. Then
%%              producer sends everything to kafka.<br/>Default:
%%              100000.</dd>
%%        <dt>``{flush_timeout, integer()}''</dt>
%%          <dd>If no message comes in during this time (in ms),
%%              producer will try to flush buffer anyway, regardless
%%              of flush_threshold setting.<br/>Default: 100.</dd>
%%      </dl>
-spec start_producer([{string(), integer()}], [{atom(), integer()}]) ->
                        {ok, pid()} | {error, any()}.
start_producer(Hosts, Options) ->
  brod_producer:start_link(Hosts, Options).

%% @doc Stop producer process
-spec stop_producer(pid()) -> ok.
stop_producer(Pid) ->
  brod_producer:stop(Pid).

%% @equiv produce(Pid, Topic, 0, <<>>, Value)
-spec produce(pid(), binary(), binary()) -> ok | {error, any()}.
produce(Pid, Topic, Value) ->
  produce(Pid, Topic, 0, Value).

%% @equiv produce(Pid, Topic, Partition, <<>>, Value)
-spec produce(pid(), binary(), integer(), binary()) -> ok | {error, any()}.
produce(Pid, Topic, Partition, Value) ->
  produce(Pid, Topic, Partition, <<>>, Value).

%% @doc Send message to a broker.
%%      Internally it's a gen_server:call to brod_srv process. Returns
%%      when the message is handled by brod_srv.
-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 ok | {error, any()}.
produce(Pid, Topic, Partition, Key, Value) ->
  brod_producer:produce(Pid, Topic, Partition, Key, Value).

%% @doc
-spec start_consumer([{string(), integer()}], binary(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_consumer(Hosts, Topic, Partition) ->
  brod_consumer:start_link(Hosts, Topic, Partition).

%% @doc Stop consumer process
-spec stop_consumer(pid()) -> ok.
stop_consumer(Pid) ->
  brod_consumer:stop(Pid).

%% @doc Start consuming message from a partition.
%% Subscriber: a process which will receive messages
%% Offset: Where to start to fetch data from.
%%                  -1: start from the latest available offset
%%                  -2: start from the earliest available offset
%%               N > 0: valid offset for the given partition
%% MaxWaitTime: The max wait time is the maximum amount of time in
%%              milliseconds to block waiting if insufficient data is
%%              available at the time the request is issued.
%% MinBytes: This is the minimum number of bytes of messages that must
%%           be available to give a response. If the client sets this
%%           to 0 the server will always respond immediately, however
%%           if there is no new data since their last request they
%%           will just get back empty message sets. If this is set to
%%           1, the server will respond as soon as at least one
%%           partition has at least 1 byte of data or the specified
%%           timeout occurs. By setting higher values in combination
%%           with the timeout the consumer can tune for throughput and
%%           trade a little additional latency for reading only large
%%           chunks of data (e.g. setting MaxWaitTime to 100 ms and
%%           setting MinBytes to 64k would allow the server to wait up
%%           to 100ms to try to accumulate 64k of data before
%%           responding).
%% MaxBytes: The maximum bytes to include in the message set for this
%%           partition. This helps bound the size of the response.
-spec consume(pid(), pid(), integer(), integer(), integer(), integer()) ->
                 ok | {error, any()}.
consume(Pid, Subscriber, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  brod_consumer:consume(Pid, Subscriber, Offset,
                        MaxWaitTime, MinBytes, MaxBytes).

get_metadata(Hosts) ->
  get_metadata(Hosts, []).

get_metadata(Hosts, Topics) ->
  {ok, Pid} = brod_utils:try_connect(Hosts),
  Request = #metadata_request{topics = Topics},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  brod_sock:stop(Pid),
  Response.

get_offsets(Hosts, Topic, Partition, Time, MaxNOffsets) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  Request = #offset_request{ topic = Topic
                           , partition = Partition
                           , time = Time
                           , max_n_offsets = MaxNOffsets},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  brod_sock:stop(Pid),
  Response.

fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  Request = #fetch_request{ topic = Topic
                          , partition = Partition
                          , offset = Offset
                          , max_wait_time = MaxWaitTime
                          , min_bytes = MinBytes
                          , max_bytes = MaxBytes},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  brod_sock:stop(Pid),
  Response.

%%%_* Internal functions -------------------------------------------------------
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
  brod_sock:start_link(self(), Host, Port, []).

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
