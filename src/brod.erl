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

%% Producer API
-export([ start_link_producer/1
        , start_link_producer/2
        , stop_producer/1
        , produce/3
        , produce/4
        , produce/5
        , produce_sync/3
        , produce_sync/4
        , produce_sync/5
        ]).

%% Consumer API
-export([ start_link_consumer/3
        , start_link_consumer/4
        , start_consumer/3
        , stop_consumer/1
        , consume/2
        , consume/3
        , consume/6
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

-export_type([host/0]).

%%%_* Includes -----------------------------------------------------------------
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Types --------------------------------------------------------------------
-type host() :: {string(), integer()}.

%%%_* API ----------------------------------------------------------------------
%% @equiv start_link_producer(Hosts, [])
-spec start_link_producer([host()]) -> {ok, pid()} | {error, any()}.
start_link_producer(Hosts) ->
  start_link_producer(Hosts, []).

%% @doc Start a process to publish messages to kafka.
%%      Hosts:
%%        list of "bootstrap" kafka nodes, {"hostname", 9092}
%%      Options:
%%        list of tuples {atom(), any()} where atom can be:
%%      producer_id (optional)
%%        The producer PID is regisgred using this name if given.
%%        This is mandatory for permanent producers configured in app env.
%%        See brod_sup.erl for more info.
%%      client_id (optional, default = ?DEFAULT_CLIENT_ID):
%%        Atom or binary string (preferably unique) identifier of the client,
%%        This ID is used by kafka broker for per-client statistic data
%%        collection.
%%      required_acks (optional, default = -1):
%%        How many acknowledgements the kafka broker should receive
%%        from the clustered replicas before responding to the
%%        producer.
%%        If it is 0 the broker will not send any response (this is
%%        the only case where the broker will not reply to a
%%        request). If it is 1, the broker will wait the data is
%%        written to the local log before sending a response. If it is
%%        -1 the broker will block until the message is committed by
%%        all in sync replicas before sending a response. For any
%%        number > 1 the broker will block waiting for this number of
%%        acknowledgements to occur (but the broker will never wait
%%        for more acknowledgements than there are in-sync replicas).
%%      ack_timeout (optional, default = 1000 ms):
%%        Maximum time in milliseconds the broker can await the
%%        receipt of the number of acknowledgements in
%%        RequiredAcks. The timeout is not an exact limit on
%%        the request time for a few reasons: (1) it does not
%%        include network latency, (2) the timer begins at the
%%        beginning of the processing of this request so if many
%%        requests are queued due to broker overload that wait
%%        time will not be included, (3) we will not terminate a
%%        local write so if the local write time exceeds this
%%        timeout it will not be respected.
%%     max_leader_queue_len (optional, default = 32):
%%        How many requests sent to a kafka broker can stay
%%        unacknowledged by a broker before blocking producing thread.
%%     max_requests_in_flight (optional, default = 5):
%%        Worker process will be sending produce reqeusts to kafka
%%        without waiting for acknowledgements of a previous one.
%%        This is to control how many such requests can be sent.
-spec start_link_producer([host()], proplists:proplist()) ->
                             {ok, pid()} | {error, any()}.
start_link_producer(Hosts, Options) ->
  brod_producer:start_link(Hosts, Options).

%% @doc Stop producer process
-spec stop_producer(pid()) -> ok.
stop_producer(Pid) ->
  brod_producer:stop(Pid).

%% @equiv produce(Pid, Topic, 0, <<>>, Value)
-spec produce(pid(), binary(), binary()) -> {ok, reference()} | ok.
produce(Pid, Topic, Value) ->
  produce(Pid, Topic, 0, <<>>, Value).

%% @equiv produce(Pid, Topic, Partition, [{Key, Value}])
-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 {ok, reference()} | ok.
produce(Pid, Topic, Partition, Key, Value) ->
  produce(Pid, Topic, Partition, [{Key, Value}]).

%% @doc Send one or more {key, value} messages to a broker.
%%      If required_acks was set to 0, returns ok.
%%      Otherwise returns a reference which can later be used to match
%%      on an 'ack' message from producer acknowledging that the
%%      payload has been handled by kafka cluster.
%%      'ack' message has format {{Reference, ProducerPid}, ack}.
-spec produce(pid(), binary(), integer(), [{binary(), binary()}]) ->
                 {ok, reference()} | ok.
produce(Pid, Topic, Partition, KVList) when is_list(KVList) ->
  brod_producer:produce(Pid, Topic, Partition, KVList).

%% @equiv produce_sync(Pid, Topic, 0, <<>>, Value)
-spec produce_sync(pid(), binary(), binary()) -> ok.
produce_sync(Pid, Topic, Value) ->
  produce_sync(Pid, Topic, 0, <<>>, Value).

%% @equiv produce_sync(Pid, Topic, Partition, [{Key, Value}])
-spec produce_sync(pid(), binary(), integer(), binary(), binary()) -> ok.
produce_sync(Pid, Topic, Partition, Key, Value) ->
  produce_sync(Pid, Topic, Partition, [{Key, Value}]).

%% @doc Send one or more {key, value} messages to a broker and block
%%      until producer acknowledges the payload.
%%      Does not block if required_acks was set to 0.
-spec produce_sync(pid(), binary(), integer(), [{binary(), binary()}]) ->
        ok | {error, any()}.
produce_sync(Pid, Topic, Partition, KVList) when is_list(KVList) ->
  case produce(Pid, Topic, Partition, KVList) of
    ok ->
      ok;
    {ok, Ref} ->
      MonitorRef = erlang:monitor(process, Pid),
      receive
        {'DOWN', MonitorRef, _, _, Info} ->
          {error, Info};
        {{Ref, Pid}, ack} ->
          erlang:demonitor(MonitorRef, [flush]),
          ok
      end
  end.

%% @deprecated
%% @equiv start_link_consumer(Hosts, Topic, Partition)
-spec start_consumer([host()], binary(), integer()) ->
                   {ok, pid()} | {error, any()}.
start_consumer(Hosts, Topic, Partition) ->
  start_link_consumer(Hosts, Topic, Partition).

%% @equiv start_link_consumer(Hosts, Topic, Partition, 1000)
-spec start_link_consumer([host()], binary(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_link_consumer(Hosts, Topic, Partition) ->
  start_link_consumer(Hosts, Topic, Partition, 1000).

%% @doc Start consumer process
%%      SleepTimeout: how much time to wait before sending next fetch
%%      request when we got no messages in the last one
-spec start_link_consumer([host()], binary(), integer(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_link_consumer(Hosts, Topic, Partition, SleepTimeout) ->
  brod_consumer:start_link(Hosts, Topic, Partition, SleepTimeout).

%% @doc Stop consumer process
-spec stop_consumer(pid()) -> ok.
stop_consumer(Pid) ->
  brod_consumer:stop(Pid).

-spec consume(pid(), integer()) -> ok | {error, any()}.
%% @equiv consume(Pid, fun(M) -> self() ! M end, Offset)
%% @doc A simple alternative for consume/6 with predefined defaults.
%%      Calling process will receive messages from consumer process.
consume(Pid, Offset) ->
  Self = self(),
  Callback = fun(MsgSet) -> Self ! MsgSet end,
  consume(Pid, Callback, Offset).

-spec consume(pid(), callback_fun(), integer()) -> ok | {error, any()}.
%% @equiv consume(Pid, Callback, Offset, 1000, 0, 100000)
%% @doc A simple alternative for consume/6 with predefined defaults.
%%      Callback() will be called to handle incoming messages.
consume(Pid, Callback, Offset) ->
  consume(Pid, Callback, Offset, 1000, 0, 100000).

%% @doc Start consuming data from a partition.
%%      Messages are delivered as #message_set{}.
%% Pid: brod consumer pid, @see start_link_consumer/3
%% Callback: a function which will be called by brod_consumer to
%%           handle incoming messages
%% Offset: Where to start to fetch data from.
%%                  -1: start from the latest available offset
%%                  -2: start from the earliest available offset
%%               N > 0: valid offset for the given partition
%% MaxWaitTime: The max wait time is the maximum amount of time in
%%              milliseconds to block waiting if insufficient data is
%%              available at the time the request is issued.
%% MinBytes: This is the minimum number of bytes of messages that must
%%           be available to give a response. If the client sets this
%%           to 0, the server will always respond immediately. If
%%           there is no new data since their last request, clients
%%           will get back empty message sets. If this is set to 1,
%%           the server will respond as soon as at least one partition
%%           has at least 1 byte of data or the specified timeout
%%           occurs. By setting higher values in combination with the
%%           timeout the consumer can tune for throughput and trade a
%%           little additional latency for reading only large chunks
%%           of data (e.g. setting MaxWaitTime to 100 ms and setting
%%           MinBytes to 64k would allow the server to wait up to
%%           100ms to try to accumulate 64k of data before
%%           responding).
%% MaxBytes: The maximum bytes to include in the message set for this
%%           partition. This helps bound the size of the response.
-spec consume(pid(), callback_fun(), integer(),
             integer(), integer(), integer()) -> ok | {error, any()}.
consume(Pid, Callback, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  brod_consumer:consume(Pid, Callback, Offset, MaxWaitTime, MinBytes, MaxBytes).

%% @doc Fetch broker metadata
-spec get_metadata([host()]) -> {ok, #metadata_response{}} | {error, any()}.
get_metadata(Hosts) ->
  brod_utils:get_metadata(Hosts).

%% @doc Fetch broker metadata
-spec get_metadata([host()], [binary()]) ->
                      {ok, #metadata_response{}} | {error, any()}.
get_metadata(Hosts, Topics) ->
  brod_utils:get_metadata(Hosts, Topics).

%% @equiv get_offsets(Hosts, Topic, Partition, -1, 1)
-spec get_offsets([host()], binary(), non_neg_integer()) ->
                     {ok, #offset_response{}} | {error, any()}.
get_offsets(Hosts, Topic, Partition) ->
  get_offsets(Hosts, Topic, Partition, -1, 1).

%% @doc Get valid offsets for a specified topic/partition
-spec get_offsets([host()], binary(), non_neg_integer(),
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
-spec fetch([host()], binary(), non_neg_integer(), integer()) ->
               {ok, #message_set{}} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset) ->
  fetch(Hosts, Topic, Partition, Offset, 1000, 0, 100000).

%% @doc Fetch a single message set from a specified topic/partition
-spec fetch([host()], binary(), non_neg_integer(),
            integer(), non_neg_integer(), non_neg_integer(),
            pos_integer()) ->
               {ok, #message_set{}} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  {ok, Pid} = connect_leader(Hosts, Topic, Partition),
  Request = #fetch_request{ topic = Topic
                          , partition = Partition
                          , offset = Offset
                          , max_wait_time = MaxWaitTime
                          , min_bytes = MinBytes
                          , max_bytes = MaxBytes},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  ok = brod_sock:stop(Pid),
  case Response of
    {ok, FetchResponse} ->
      {ok, brod_utils:fetch_response_to_message_set(FetchResponse)};
    _ ->
      Response
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
  {ok, Pid} = brod:start_link_producer(Hosts),
  Res = brod:produce_sync(Pid, Topic, Partition, Key, Value),
  brod:stop_producer(Pid),
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

%%%_* Internal functions -------------------------------------------------------
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
  brod_sock:start_link(self(), Host, Port, ?DEFAULT_CLIENT_ID, []).

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

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
