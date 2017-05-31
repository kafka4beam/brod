# Brod - Apache Kafka Erlang client library

Brod is an erlang implementation of the Apache Kafka protocol, providing support for both producers and consumers.

[![Build Status](https://travis-ci.org/klarna/brod.svg)](https://travis-ci.org/klarna/brod)

Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

# Features

* Supports Apache Kafka v0.8+
* Robust producer implementation supporting in-flight requests and asynchronous acknowledgements
* Both consumer and producer handle leader re-election and other cluster disturbances internally
* Opens max 1 tcp connection to a broker per "brod_client", one can create more clients if needed
* Producer: will start to batch automatically when number of unacknowledged (in flight) requests exceeds configurable maximum
* Producer: will try to re-send buffered messages on common errors like "Not a leader for partition", errors are resolved automatically by refreshing metadata
* Simple consumer: The poller, has a configurable "prefetch count" - it will continue sending fetch requests as long as total number of unprocessed messages (not message-sets) is less than "prefetch count"
* Group subscriber: Support for consumer groups with options to have Kafka as offset storage or a custom one
* Topic subscriber: Subscribe on messages from all or selected topic partitions without using consumer groups

# Missing features

* lz4 compression & decompression
* new 0.10 on-wire message format
* new 0.10.1.0 create/delete topic api

# Building and testing

    make
    make test-env t # requires docker-composer in place

# Quick Demo

Assuming kafka is running at `localhost:9092` 
and there is a topic named `brod-test`.

Below code snippet is copied from Erlang shell 
with some non-important printouts trimmed.

```erlang
> rr(brod).
> {ok, _} = application:ensure_all_started(brod).
> KafkaBootstrapEndpoints = [{"localhost", 9092}].
> Topic = <<"brod-test">>.
> Partition = 0.
> ok = brod:start_client(KafkaBootstrapEndpoints, client1).
> ok = brod:start_producer(client1, Topic, _ProducerConfig = []).
> ok = brod:produce_sync(client1, Topic, Partition, <<"key1">>, <<"value1">>).
> ok = brod:produce_sync(client1, Topic, Partition, <<"key2">>, <<"value2">>).
> SubscriberCallbackFun =
    fun(Partition, Msg, ShellPid = CallbackState) ->
      ShellPid ! Msg,
      {ok, ack, CallbackState}
    end.
> Receive = fun() -> receive Msg -> Msg after 1000 -> timeout end end.
> brod_topic_subscriber:start_link(client1, Topic, Partitions=[Partition],
                                   _ConsumerConfig=[{begin_offset, earliest}],
                                   _CommittdOffsets=[], SubscriberCallbackFun,
                                   _CallbackState=self()).
> Receive().
#kafka_message{offset = 0,magic_byte = 0,attributes = 0,
               key = <<"key1">>,value = <<"value1">>,crc = 1978725405}
> Receive().
#kafka_message{offset = 1,magic_byte = 0,attributes = 0,
               key = <<"key2">>,value = <<"value2">>,crc = 1964753830}
> {ok, CallRef} = brod:produce(client1, Topic, Partition, <<"key3">>, <<"value3">>).
> #brod_produce_reply{ call_ref = CallRef,
                       result   = brod_produce_req_acked
                     } = Receive().
> Receive().
#kafka_message{offset = 2,magic_byte = 0,attributes = 0,
               key = <<"key3">>,value = <<"value3">>,crc = -1013830416}
```

# Overview

Brod supervision (and process link) tree.

![](https://cloud.githubusercontent.com/assets/164324/19621338/0b53ccbe-9890-11e6-9142-432a3a87bcc7.jpg)

# Clients

A `brod_client` in brod is a `gen_server` responsible for establishing and 
maintaining tcp sockets connecting to kafka brokers. 
It also manages per-topic-partition producer and consumer processes under 
two-level supervision trees.

## Start clients by default

You may include client configs in sys.config have them started by default 
(by application controller)

Example of configuration (for sys.config):

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

## Start brod client on demand

You may also call `brod:start_client/1,2,3` to start a client on demand, 
which will be added to brod supervision tree.

```erlang
ClientConfig = [{reconnect_cool_down_seconds, 10}],
ok = brod:start_client([{"localhost", 9092}], brod_client_1, ClientConfig).
```

# Producers

## Auto start producer with default producer config

Put below configs to client config in sys.config or app env:

```erlang
{auto_start_producers, true}
{default_producer_config, []}
```

## Start a producer on demand

```erlang
brod:start_producer(_Client         = brod_client_1,
                    _Topic          = <<"brod-test-topic-1">>,
                    _ProducerConfig = []).
```

## Produce to a known topic-partition:

```erlang
{ok, CallRef} =
  brod:produce(_Client    = brod_client_1,
               _Topic     = <<"brod-test-topic-1">>,
               _Partition = 0
               _Key       = <<"some-key">>
               _Value     = <<"some-value">>),

%% just to illustrate what message to expect
receive
  #brod_produce_reply{ call_ref = CallRef
                     , result   = brod_produce_req_acked
                     } ->
    ok
after 5000 ->
  erlang:exit(timeout)
end.
```

## Synchronized produce request

Block calling process until Kafka confirmed the message:

```erlang
{ok, CallRef} =
  brod:produce(_Client    = brod_client_1,
               _Topic     = <<"brod-test-topic-1">>,
               _Partition = 0
               _Key       = <<"some-key">>
               _Value     = <<"some-value">>),
brod:sync_produce_request(CallRef).
```

or the same in one call:

```erlang
brod:produce_sync(_Client    = brod_client_1,
                  _Topic     = <<"brod-test-topic-1">>,
                  _Partition = 0
                  _Key       = <<"some-key">>
                  _Value     = <<"some-value">>).
```

## Produce with random partitioner

```erlang
Client = brod_client_1,
Topic  = <<"brod-test-topic-1">>,
PartitionFun = fun(_Topic, PartitionsCount, _Key, _Value) ->
                   {ok, crypto:rand_uniform(0, PartitionsCount)}
               end,
{ok, CallRef} = brod:produce(Client, Topic, PartitionFun, Key, Value).
```

## Produce a batch of (maybe nested) Key-Value list

```erlang
%% The top-level key is used for partitioning
%% and nested keys are discarded.
%% Nested messages are serialized into a message set to the same partition.
brod:produce(_Client    = brod_client_1,
             _Topic     = <<"brod-test-topic-1">>,
             _Partition = MyPartitionerFun
             _Key       = KeyUsedForPartitioning
             _Value     = [ {<<"k1", <<"v1">>}
                          , {<<"k2", <<"v2">>}
                          , { _KeyDiscarded = <<>>
                            , [ {<<"k3">>, <<"v3">>}
                              , {<<"k4">>, <<"v4">>}
                              ]}
                          ]).
```

## Handle acks from kafka

Unless brod:produce_sync was called, callers of brod:produce should 
expect a message of below pattern for each produce call. 
Add `-include_lib("brod/include/brod.hrl").` to use the record.

```erlang
#brod_produce_reply{ call_ref = CallRef %% returned from brod:produce
                   , result   = brod_produce_req_acked
                   }
```

NOTE: If required_acks is set to 0 in producer config, 
kafka will NOT ack the requests, and the reply message is sent back 
to caller immediately after the message has been sent to the socket process.

In case the brod:produce caller is a process like gen_server which 
receives ALL messages, the callers should keep the call references in its 
looping state and match the replies against them when received. 
Otherwise brod:sync_produce_request/1 can be used to block-wait for acks.

NOTE: The replies are only strictly ordered per-partition. 
i.e. if the caller is producing to two or more partitions, 
it may receive replies ordered differently than in which order 
brod:produce API was called.

# Consumers

Kafka consumers work in poll mode. In brod, `brod_consumer` is the poller, 
which is constantly asking for more data from the kafka node which is a leader 
for the given partition.

By subscribing to `brod_consumer` a process should receive the polled message 
sets (not individual messages) into its mailbox.

In brod, we have so far implemented two different subscribers 
(`brod_topic_subscriber` and `brod_group_subscriber`), 
hopefully covered most of the common use cases.

For maximum flexibility, an applications may implement their own 
per-partition subscriber.

Below diagrams illustrate 3 examples of how subscriber processes may work 
with `brod_consumer`.

## Partition subscriber
![](https://cloud.githubusercontent.com/assets/164324/19621677/5e469350-9897-11e6-8c8e-8a6a4f723f73.jpg)

This gives the best flexibility as the per-partition subscribers work 
directly with per-partition pollers.

The messages are delivered to subscribers in message sets (batches), 
not individual messages, (however the subscribers are allowed to 
ack individual offsets).

## Topic subscriber (`brod_topic_subscriber`)
![](https://cloud.githubusercontent.com/assets/164324/19621951/41e1d75e-989e-11e6-9bc2-49fe814d3020.jpg)

A topic subscriber provides the easiest way to receive and process 
messages from ALL partitions of a given topic.  See `brod_demo_cg_collector` 
and `brod_demo_topic_subscriber` for example.

Users may choose to implement the `brod_topic_subscriber` behaviour callbacks 
in a module, or simply provide an anonymous callback function to have the 
individual messages processed.

## Group subscriber (`brod_group_subscriber`)
![](https://cloud.githubusercontent.com/assets/164324/19621956/59d76a9a-989e-11e6-9633-a0bc677e06f3.jpg)

Similar to topic subscriber, the `brod_group_subscriber` behaviour callbacks 
are to be implemented to process individual messages. See 
`brod_demo_group_subscriber_koc` and `brod_demo_group_subscriber_loc` 
for example.

A group subscriber is started by giving a set of topics, some 
(maybe none, or maybe all) of the partitions in the topic set will be 
assigned to it, then the subscriber should subscribe to ALL the assigned 
partitions.

Users may also choose to implement the `brod_group_member` behaviour (callbacks 
for `brod_group_coordinator`) for a different group subscriber (e.g. spawn 
one subscriber per partition), see [brucke](https://github.com/klarna/brucke) 
for example.

### Example of group consumer which commits offsets to Kafka

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

# Authentication support

brod supports SASL PLAIN authentication mechanism out of the box. To use it
add `{sasl, {plain, Username, Password}}` to client config. Also, brod has authentication
plugins support. Authentication callback module should implement `brod_auth_backend` behaviour.
Auth function spec:

```erlang
auth(Host :: string(), Sock :: gen_tcp:socket() | ssl:sslsocket(),
     Mod :: gen_tcp | ssl, ClientId :: binary(),
     Timeout :: pos_integer(), SaslOpts :: term()) ->
        ok | {error, Reason :: term()}
```

If authentication is successful - callback function should return an atom `ok`, otherwise - error tuple with reason description.
For example, you can use `brod_gssapi' plugin (https://github.com/ElMaxo/brod_gssapi) for SASL GSSAPI authentication.
To use it - add it as dependency to your top level project that uses brod.
Then add `{sasl, {callback, brod_gssapi, {gssapi, Keytab, Principal}}}` to client config.
Keytab should be the keytab file path, and Principal should be a byte-list or binary string.

See also: https://github.com/klarna/brod/wiki/SASL-gssapi-(kerberos)-authentication

# Other API to play with/inspect kafka

These functions open a connetion to kafka cluster, send a request,
await response and then close the connection.

```erlang
Hosts = [{"localhost", 9092}].
Topic = <<"topic">>.
Partition = 0.
brod:get_metadata(Hosts).
brod:get_metadata(Hosts, [Topic]).
brod:get_offsets(Hosts, Topic, Partition).
brod:fetch(Hosts, Topic, Partition, 1).
```

# brod-cli: A command line tool to interact with Kafka

This will build a self-contained binary with brod application

```bash
make rel
./_rel/brod/bin/brod --help
```

Disclaimer: This script is NOT designed for use cases where fault-tolerance is a hard requirement.
As it may crash when e.g. kafka cluster is temporarily unreachable,
or (for fetch command) when the parition leader migrates to another broker in the cluster.

Start an Erlang shell with brod started

```bash
./_rel/brod/bin/brod-i
```

