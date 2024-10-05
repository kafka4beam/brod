# NOTICE

This product includes software developed by
[Klarna Bank AB (publ)](https://www.klarna.com)

# Brod - Apache Kafka Client for Erlang/Elixir

![brod](https://github.com/kafka4beam/brod/workflows/brod/badge.svg?branch=master)

Brod is an Erlang implementation of the Apache Kafka protocol, providing support for both producers and consumers.

Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

## Features

- Supports Apache Kafka v0.8+
- Robust producer implementation supporting in-flight requests and asynchronous acknowledgements
- Both consumer and producer handle leader re-election and other cluster disturbances internally
- Opens max 1 tcp connection to a broker per `brod_client`, one can create more clients if needed
- Producer: will start to batch automatically when number of unacknowledged (in flight) requests exceeds configurable maximum
- Producer: will try to re-send buffered messages on common errors like "Not a leader for partition", errors are resolved automatically by refreshing metadata
- Simple consumer: The poller, has a configurable "prefetch count" - it will continue sending fetch requests as long as total number of unprocessed messages (not message-sets) is less than "prefetch count"
- Group subscriber: Support for consumer groups with options to have Kafka as offset storage or a custom one
- Topic subscriber: Subscribe on messages from all or selected topic partitions without using consumer groups
- Pick latest supported version when sending requests to kafka.
- Direct APIs for message send/fetch and cluster inspection/management without having to start clients/producers/consumers.
- A escriptized command-line tool for message send/fetch and cluster inspection/management.
- Configurable compression library. No compression is supported by default for both producers and consumers.
  For more compression options, see [kafka_protocol/README](https://github.com/kafka4beam/kafka_protocol/blob/master/README.md#compression-support)

## Building and testing

NOTE: Min Erlang/OTP version 22

```sh
make compile
make test-env t # requires docker-compose in place
```

## Working With Kafka 0.9.x or Earlier

Make sure `{query_api_versions, false}` exists in client config.
This is because `ApiVersionRequest` was introduced in kafka 0.10,
sending such request to older version brokers will cause connection failure.

e.g. in `sys.config`:

```erlang
[{brod,
   [ { clients
     , [ { brod_client_1 %% registered name
         , [ { endpoints, [{"localhost", 9092}]}
           , { query_api_versions, false} %% <---------- here
           ]}]}]}]
```

## Quick Demo

Assuming kafka is running at `localhost:9092` and there is a topic named `test-topic`.

Start Erlang shell by `make compile; erl -pa _build/default/lib/*/ebin`, then paste lines below into shell:

```erlang
rr(brod),
{ok, _} = application:ensure_all_started(brod),
KafkaBootstrapEndpoints = [{"localhost", 9092}],
Topic = <<"test-topic">>,
Partition = 0,
ok = brod:start_client(KafkaBootstrapEndpoints, client1),
ok = brod:start_producer(client1, Topic, _ProducerConfig = []),
{ok, FirstOffset} = brod:produce_sync_offset(client1, Topic, Partition, <<"key1">>, <<"value1">>),
ok = brod:produce_sync(client1, Topic, Partition, <<"key2">>, <<"value2">>),
SubscriberCallbackFun = fun(Partition, Msg, ShellPid = CallbackState) -> ShellPid ! Msg, {ok, ack, CallbackState} end,
Receive = fun() -> receive Msg -> Msg after 1000 -> timeout end end,
brod_topic_subscriber:start_link(client1, Topic, Partitions=[Partition],
                                 _ConsumerConfig=[{begin_offset, FirstOffset}],
                                 _CommittedOffsets=[], message, SubscriberCallbackFun,
                                 _CallbackState=self()),
AckCb = fun(Partition, BaseOffset) -> io:format(user, "\nProduced to partition ~p at base-offset ~p\n", [Partition, BaseOffset]) end,
ok = brod:produce_cb(client1, Topic, Partition, <<>>, [{<<"key3">>, <<"value3">>}], AckCb).
Receive().
Receive().
{ok, {_, [Msg]}} = brod:fetch(KafkaBootstrapEndpoints, Topic, Partition, FirstOffset + 2), Msg.
```

Example outputs:

```erlang
#kafka_message{offset = 0,key = <<"key1">>,
               value = <<"value1">>,ts_type = create,ts = 1531995555085,
               headers = []}
#kafka_message{offset = 1,key = <<"key2">>,
               value = <<"value2">>,ts_type = create,ts = 1531995555107,
               headers = []}
Produced to partition 0 at base-offset 406
#kafka_message{offset = 2,key = <<"key3">>,
               value = <<"value3">>,ts_type = create,ts = 1531995555129,
               headers = []}
```

## Overview

Brod supervision (and process link) tree.

![brod supervision architecture](https://cloud.githubusercontent.com/assets/164324/19621338/0b53ccbe-9890-11e6-9142-432a3a87bcc7.jpg)

## Clients

A `brod_client` in brod is a `gen_server` responsible for establishing and
maintaining tcp sockets connecting to kafka brokers.
It also manages per-topic-partition producer and consumer processes under
two-level supervision trees.

To use producers or consumers, you have to start at least one client that
will manage them.

### Compression

Brod does not dependent on any compression/decompression implementation by default.
To enable them, you must add the compression application as dependency in your project's rebar.config.

For example:

```erlang
{deps, [
    {snappyer, "1.2.9"}
]}.
```

### Start clients by default

You may include client configs in `sys.config` have them started by default
(by application controller)

Example of configuration (for `sys.config`):

```erlang
[{brod,
   [ { clients
     , [ { brod_client_1 %% registered name
         , [ { endpoints, [{"localhost", 9092}]}
           , { reconnect_cool_down_seconds, 10} %% socket error recovery
           ]
         }
       ]
     }
     %% start another client for another kafka cluster
     %% or if you think it's necessary to start another set of tcp connections
   ]
}]
```

Example of configuration in Elixir (for `config/dev.exs` or `config/prod.exs`, etc.):

```elixir
config :brod,
  clients: [
    # :brod_client_1 is the registered name of the client
    brod_client_1: [
      endpoints: [{"localhost", 9092}],
      reconnect_cool_down_seconds: 10
    ]
  ]
```

### Start brod client on demand

You may also call `brod:start_client/1,2,3` to start a client on demand,
which will be added to brod supervision tree.

```erlang
ClientConfig = [{reconnect_cool_down_seconds, 10}],
ok = brod:start_client([{"localhost", 9092}], brod_client_1, ClientConfig).
```

Extra [socket options](http://erlang.org/doc/man/gen_tcp.html#type-option)
could be passed as `{extra_sock_opts, ExtraSockOpts}`, e.g.

```erlang
ExtraSockOpts = [{sndbuf, 1024*1024}],
ok = brod:start_client([{"localhost", 9092}], brod_client_1, [{extra_sock_opts, ExtraSockOpts}]).
```

## Producers

A `brod_producer` is a `gen_server` that is responsible for producing messages to a given
partition of a given topic.

Producers may be started either manually or automatically in the moment you call `brod:produce`
but did not call `brod:start_producer` beforehand.

### Auto start producer with default producer config

Put below configs to client config in `sys.config` or app env if you start client statically:

```erlang
{auto_start_producers, true}
{default_producer_config, []}
```

Or pass the `{auto_start_producers, true}` option to `brod:start_client` if you start the client
dynamically.

### Start a Producer on Demand

```erlang
brod:start_producer(_Client         = brod_client_1,
                    _Topic          = <<"brod-test-topic-1">>,
                    _ProducerConfig = []).
```

### Supported Message Input Format

Brod supports below produce APIs:

- [`brod:produce`](https://hexdocs.pm/brod/brod.html#produce/5): Async produce with ack message sent back to caller.
- [`brod:produce_cb`](https://hexdocs.pm/brod/brod.html#produce_cb/6): Async produce with a callback evaluated when ack is received.
- [`brod:produce_sync`](https://hexdocs.pm/brod/brod.html#produce_sync/5): Sync produce that returns `ok`.
- [`brod:produce_sync_offset`](https://hexdocs.pm/brod/brod.html#produce_sync_offset/5): Sync produce that returns `{ok, BaseOffset}`.
- [`brod:produce_no_ack`](https://hexdocs.pm/brod/brod.html#produce_no_ack/5): Async produce without backpressure (use with care!).

The `Value` arg in these APIs can be:

- `binary()`: One single message
- `{brod:msg_ts(), binary()}`: One single message with its create-time timestamp
- `#{ts => brod:msg_ts(), value => binary(), headers => [{_, _}]}`:
  One single message. If this map does not have a `key` field, the `Key` argument is used.
- `[{K, V} | {T, K, V}]`: A batch, where `V` could be a nested list of such representation.
- `[#{key => K, value => V, ts => T, headers => [{_, _}]}]`: A batch.

When `Value` is a batch, the `Key` argument is only used as partitioner input and all messages are written on the same partition.
All messages are unified into a batch format of below spec:
`[#{key => K, value => V, ts => T, headers => [{_, _}]}]`.
`ts` field is dropped for kafka prior to version `0.10` (produce API version 0, magic version 0).
`headers` field is dropped for kafka prior to version `0.11` (produce API version 0-2, magic version 0-1).

### Synchronized Produce API

```erlang
brod:produce_sync(_Client    = brod_client_1,
                  _Topic     = <<"brod-test-topic-1">>,
                  _Partition = 0,
                  _Key       = <<"some-key">>,
                  _Value     = <<"some-value">>).
```

Or block calling process until Kafka confirmed the message:

```erlang
{ok, CallRef} =
  brod:produce(_Client    = brod_client_1,
               _Topic     = <<"brod-test-topic-1">>,
               _Partition = 0,
               _Key       = <<"some-key">>,
               _Value     = <<"some-value">>),
brod:sync_produce_request(CallRef).
```

### Produce One Message and Receive Its Offset in Kafka

```erlang
Client = brod_client_1,
Topic  = <<"brod-test-topic-1">>,
{ok, Offset} = brod:produce_sync_offset(Client, Topic, 0, <<>>, <<"value">>).
```

### Produce with Random Partitioner

```erlang
Client = brod_client_1,
Topic  = <<"brod-test-topic-1">>,
ok = brod:produce_sync(Client, Topic, random, Key, Value).
```

### Produce a Batch

```erlang
brod:produce(_Client    = brod_client_1,
             _Topic     = <<"brod-test-topic-1">>,
             _Partition = MyPartitionerFun,
             _Key       = KeyUsedForPartitioning,
             _Value     = [ #{key => "k1", value => "v1", headers => [{"foo", "bar"}]}
                          , #{key => "k2", value => "v2"}
                          ]).
```

### Handle Acks from Kafka as Messages

For async produce APIs `brod:produce/3` and `brod:produce/5`,
the caller should expect a message of below pattern for each produce call.

```erlang
#brod_produce_reply{ call_ref = CallRef %% returned from brod:produce
                   , result   = brod_produce_req_acked
                   }
```

Add `-include_lib("brod/include/brod.hrl").` to use the record.

In case the `brod:produce` caller is a process like `gen_server` which
receives ALL messages, the callers should keep the call references in its
looping state and match the replies against them when received.
Otherwise `brod:sync_produce_request/1` can be used to block-wait for acks.

NOTE: If `required_acks` is set to `none` in producer config,
kafka will NOT ack the requests, and the reply message is sent back
to caller immediately after the message has been sent to the socket process.

NOTE: The replies are only strictly ordered per-partition.
i.e. if the caller is producing to two or more partitions,
it may receive replies ordered differently than in which order
`brod:produce` API was called.

### Handle Acks from Kafka in Callback Function

Async APIs `brod:produce_cb/4` and `brod:produce_cb/6` allow callers to
provided a callback function to handle acknowledgements from kafka.
In this case, the caller may want to monitor the producer process because
then they know that the callbacks will not be evaluated if the producer is 'DOWN',
and there is perhaps a need for retry.

## Consumers

Kafka consumers work in poll mode. In brod, `brod_consumer` is the poller,
which is constantly asking for more data from the kafka node which is a leader
for the given partition.

By subscribing to `brod_consumer` a process should receive the polled message
sets (not individual messages) into its mailbox.

In brod, we have so far implemented two different subscribers
(`brod_topic_subscriber` and `brod_group_subscriber`),
hopefully covered most of the common use cases.

For maximum flexibility, applications may implement their own
per-partition subscriber.

Below diagrams illustrate 3 examples of how subscriber processes may work
with `brod_consumer`.

### Partition subscriber

![partition subscriber architecture](https://cloud.githubusercontent.com/assets/164324/19621677/5e469350-9897-11e6-8c8e-8a6a4f723f73.jpg)

This gives the best flexibility as the per-partition subscribers work
directly with per-partition pollers (`brod_consumer`s).

The messages are delivered to subscribers in message sets (batches),
not individual messages, (however the subscribers are allowed to
ack individual offsets).

Example:
```erlang
ok = brod:start_client([{"localhost", 9092}], my_client). % one client per application is enough
ok = brod:start_consumer(my_client, <<"my_topic">>, []).

% Now in a separate process for each partition of my_topic call:
{ok, ConsumerPid} = brod:subscribe(my_client, self(), <<"my_topic">>, Partition, []).
% The process should now receive messages sets as regular messages
```

### Topic subscriber (`brod_topic_subscriber`)

![topic subscribe flow](https://cloud.githubusercontent.com/assets/164324/19621951/41e1d75e-989e-11e6-9bc2-49fe814d3020.jpg)

A topic subscriber provides the easiest way to receive and process messages from
ALL partitions of a given topic. See
[brod_demo_cg_collector](https://github.com/kafka4beam/brod/blob/master/test/brod_demo_cg_collector.erl) and
[brod_demo_topic_subscriber](https://github.com/kafka4beam/brod/blob/master/test/brod_demo_topic_subscriber.erl) for example.

Users may choose to implement the `brod_topic_subscriber` behaviour callbacks
in a module, or simply provide an anonymous callback function to have the
individual messages processed.

### Group subscriber (`brod_group_subscriber`)

![group subscriber flow](https://cloud.githubusercontent.com/assets/164324/19621956/59d76a9a-989e-11e6-9633-a0bc677e06f3.jpg)

Similar to topic subscriber, the `brod_group_subscriber` behaviour callbacks are
to be implemented to process individual messages. See
[brod_demo_group_subscriber_koc](https://github.com/kafka4beam/brod/blob/master/test/brod_demo_group_subscriber_koc.erl) and
[brod_demo_group_subscriber_loc](https://github.com/kafka4beam/brod/blob/master/test/brod_demo_group_subscriber_loc.erl) for
example.

A group subscriber is started by giving a set of topics, some
(maybe none, or maybe all) of the partitions in the topic set will be
assigned to it, then the subscriber should subscribe to ALL the assigned
partitions.

Users may also choose to implement the `brod_group_member` behaviour (callbacks
for `brod_group_coordinator`) for a different group subscriber (e.g. spawn
one subscriber per partition), see [brucke](https://github.com/klarna/brucke)
for example.

#### Example of group consumer which commits offsets to Kafka

```erlang
-module(my_subscriber).
-include_lib("brod/include/brod.hrl"). %% needed for the #kafka_message record definition

-export([start/1]).
-export([init/2, handle_message/4]). %% callback api

%% brod_group_subscriber behaviour callback
init(_GroupId, _Arg) -> {ok, []}.

%% brod_group_subscriber behaviour callback
handle_message(_Topic, Partition, Message, State) ->
  #kafka_message{ offset = Offset
                , key   = Key
                , value = Value
                } = Message,
  error_logger:info_msg("~p ~p: offset:~w key:~s value:~s\n",
                        [self(), Partition, Offset, Key, Value]),
  {ok, ack, State}.

%% @doc The brod client identified ClientId should have been started
%% either by configured in sys.config and started as a part of brod application
%% or started by brod:start_client/3
%% @end
-spec start(brod:client_id()) -> {ok, pid()}.
start(ClientId) ->
  Topic  = <<"brod-test-topic-1">>,
  %% commit offsets to kafka every 5 seconds
  GroupConfig = [{offset_commit_policy, commit_to_kafka_v2},
                 {offset_commit_interval_seconds, 5}
                ],
  GroupId = <<"my-unique-group-id-shared-by-all-members">>,
  ConsumerConfig = [{begin_offset, earliest}],
  brod:start_link_group_subscriber(ClientId, GroupId, [Topic],
                                   GroupConfig, ConsumerConfig,
                                   _CallbackModule  = ?MODULE,
                                   _CallbackInitArg = []).
```

## Authentication support

brod supports SASL `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512` authentication mechanisms out of the box.
To use it, add `{sasl, {Mechanism, Username, Password}}` or `{sasl, {Mechanism, File}}` to client config.
Where `Mechanism` is `plain | scram_sha_256 | scram_sha_512`, and `File` is the path to a text file
which contains two lines, first line for username and second line for password

Also, brod has authentication plugins support with `{sasl, {callback, Module, Opts}}` in client config.
Authentication callback module should implement `kpro_auth_backend` behaviour.
Auth function spec:

```erlang
auth(Host :: string(), Sock :: gen_tcp:socket() | ssl:sslsocket(),
     Mod :: gen_tcp | ssl, ClientId :: binary(),
     Timeout :: pos_integer(), SaslOpts :: term()) ->
        ok | {error, Reason :: term()}
```

If authentication is successful - callback function should return an atom `ok`, otherwise - error tuple with reason description.
For example, you can use [`brod_gssapi` plugin](https://github.com/kafka4beam/brod_gssapi) for SASL GSSAPI authentication.
To use it - add it as dependency to your top level project that uses brod.
Then add `{sasl, {callback, brod_gssapi, {gssapi, Keytab, Principal}}}` to client config.
Keytab should be the keytab file path, and Principal should be a byte-list or binary string.

See also: <https://github.com/klarna/brod/wiki/SASL-gssapi-(kerberos)-authentication>

### Additional Auth plugins

- [brod_oauth](https://github.com/HCA-Healthcare/brod_oauth)  - OAuth Bearer support

## Other API to play with/inspect kafka

These functions open a connection to kafka cluster, send a request,
await response and then close the connection.

```erlang
Hosts = [{"localhost", 9092}].
Topic = <<"topic">>.
Partition = 0.
Timeout = 1000.
TopicConfigs = [
  #{
    configs => [ #{name  => <<"cleanup.policy">>, value => "compact"}],
    num_partitions => 1,
    assignments => [],
    replication_factor => 1,
    name => Topic
  }
].
brod:get_metadata(Hosts).
brod:create_topics(Hosts, TopicConfigs, #{timeout => Timeout}).
brod:get_metadata(Hosts, [Topic]).
brod:resolve_offset(Hosts, Topic, Partition).
brod:delete_topics(Hosts, [Topic], Timeout).
```

Caution the above delete_topics can fail if you do not have `delete.topic.enable` set to true in your kafka config
