# Brod - Apache Kafka Erlang client library

Brod is an erlang implementation of the Apache Kafka protocol, providing support for both producers and consumers.

[![Build Status](https://travis-ci.org/klarna/brod.svg)](https://travis-ci.org/klarna/brod)

Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

# Features

* Supports Apache Kafka v0.8.\*, 0.9.\*
* Robust producer implementation supporting in-flight requests and asynchronous acknowledgements
* Both consumer and producer handle leader re-election and other cluster disturbances internally
* Opens max 1 tcp connection to a broker per "brod_client", one can create more clients if needed
* Producer: will start to batch automatically when number of unacknowledged (in flight) requests exceeds configurable maximum
* Producer: will try to re-send buffered messages on common errors like "Not a leader for partition", errors are resolved automatically by refreshing metadata
* Simple consumer: The poller, has a configurable "prefetch count" - it will continue sending fetch requests as long as total number of unprocessed messages (not message-sets) is less than "prefetch count"
* Group subscriber: Support for consumer groups with options to have Kafka as offset storage or a custom one
* Topic subscriber: Subscribe on messages from all or selected topic partitions without using consumer groups

# Missing features

* snappy/lz4 compression & decompression

# Building and testing

    make
    make test-env t # requires docker-composer in place

# Quick start

"client" in brod is a process responsible for establishing and maintaining
connections to kafka cluster. It also manages producer and consumer processes.

You can use brod:start_client/1,2,3 to start a client on demand,
or include its configuration in sys.config.

A required parameter for client is kafka endpoint(s).

Example of configuration (for sys.config):

```
[{brod,
   [ { clients
     , [ { brod_client_1 %% registered name
         , [ { endpoints, [{"localhost", 9092}]}
           , { reconnect_cool_down_seconds, 10} %% connection error
           ]
         }
       ]
     }
     %% start another client if producing to / consuming from another kafka cluster
     %% or if you think it's necessary to start another set of tcp connections
   ]
}]
```

## Start brod client on demand

    ClientConfig = [{reconnect_cool_down_seconds, 10}],
    ok = brod:start_client([{"localhost", 9092}], brod_client_1, ClientConfig).

## Producer

### Auto start producer with default producer config

Put below configs to client config in sys.config or app env:

    {auto_start_producers, true}
    {default_producer_config, []}


### Start a producer on demand

    brod:start_producer(_Client         = brod_client_1,
                        _Topic          = <<"brod-test-topic-1">>,
                        _ProducerConfig = []).

### Produce to a known topic-partition:

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

### Synchronized produce request

Block calling process until Kafka confirmed the message:

    {ok, CallRef} =
      brod:produce(_Client    = brod_client_1,
                   _Topic     = <<"brod-test-topic-1">>,
                   _Partition = 0
                   _Key       = <<"some-key">>
                   _Value     = <<"some-value">>),
    brod:sync_produce_request(CallRef).

or the same in one call:

    brod:produce_sync(_Client    = brod_client_1,
                      _Topic     = <<"brod-test-topic-1">>,
                      _Partition = 0
                      _Key       = <<"some-key">>
                      _Value     = <<"some-value">>).

### Produce with random partitioner

    Client = brod_client_1,
    Topic  = <<"brod-test-topic-1">>,
    PartitionFun = fun(_Topic, PartitionsCount, _Key, _Value) ->
                       {ok, crypto:rand_uniform(0, PartitionsCount)}
                   end,
    {ok, CallRef} = brod:produce(Client, Topic, PartitionFun, Key, Value).

### Handle acks from kafka

Unless brod:produce_sync was called, callers of brod:produce should 
expect a message of below pattern for each produce call. 
Add `-include_lib("brod/include/brod.hrl").` to use the record.

    #brod_produce_reply{ call_ref = CallRef %% returned from brod:produce
                       , result   = brod_produce_req_acked
                       }

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
bord:produce API was called.

## Consumer
Find more examples in test/ (brod\_demo\_*).

### Group consumer commiting offsets to Kafka

    -module(my_consumer).

    init(_GroupId, _Arg) -> {ok, []}.

    handle_message(_Topic, Partition, Message, State) ->
      #kafka_message{ offset = Offset
                    , key    = Key
                    , value  = Value
                    } = Message,
      error_logger:info_msg("~p ~p: offset:~w key:~s value:~s\n",
                            [self(), Partition, Offset, Key, Value]),
      {ok, ack, State}.

    init() ->
      Client = brod_client_1,
      Topic  = <<"brod-test-topic-1">>,
      %% commit offsets to kafka every 5 seconds
      GroupConfig = [{offset_commit_policy, commit_to_kafka_v2}
                    ,{offset_commit_interval_seconds, 5}
                    ],
      GroupId = iolist_to_binary([Topic, "-group-id"]),
      {ok, _Subscriber} =
        brod:start_link_group_subscriber(ClientId, GroupId, [Topic],
                                         GroupConfig,
                                         _ConsumerConfig  = [{begin_offset, -2}],
                                         _CallbackModule  = ?MODULE,
                                         _CallbackInitArg = []).

## Other API to play with/inspect kafka
These functions open a connetion to kafka cluster, send a request,
await response and then close the connection.

    Hosts = [{"localhost", 9092}].
    Topic = <<"topic">>.
    Partition = 0.
    brod:get_metadata(Hosts).
    brod:get_metadata(Hosts, [Topic]).
    brod:get_offsets(Hosts, Topic, Partition).
    brod:fetch(Hosts, Topic, Partition, 1).

## Self-contained binary (needs erlang runtime)
This will build a self-contained binary with brod application

    make escript
    ./brod help
