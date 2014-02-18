%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).

%% API
-export([ start_producer/3
        , stop_producer/1
        , produce/3
        , produce/4
        , produce/5
        ]).

-export([ start_consumer/3
        , stop_consumer/1
        , consume/6
        ]).

%%%_* Includes -----------------------------------------------------------------

%%%_* Types --------------------------------------------------------------------

%%%_* API ----------------------------------------------------------------------
%% @doc Start a process which manages connections to kafka brokers and
%%      specialized in publishing messages.
%%      Hosts: list of "bootstrap" kafka nodes, {"hostname", 1234}
%%      RequiredAcks: how many kafka nodes must acknowledge a message
%%      before sending a response
%%      Timeout: maximum time in milliseconds the server can await the
%%      receipt of the number of acknowledgements in RequiredAcks
-spec start_producer([{string(), integer()}], integer(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_producer(Hosts, RequiredAcks, Timeout) ->
  brod_producer:start_link(Hosts, RequiredAcks, Timeout).

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

%%%_* Internal functions -------------------------------------------------------

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
