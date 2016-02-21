# Brod - Apache Kafka Erlang client library

Brod is an erlang implementation of the Apache Kafka protocol, providing support for both producers and consumers.

[![Build Status](https://travis-ci.org/klarna/brod.svg?branch=2.0-dev)](https://travis-ci.org/klarna/brod)

Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

# Features

* Supports Apache Kafka v0.8.\*, 0.9.\*
* Robust producer implementation supporing in-flight requests and asynchronous acknowledgements
* Both consumer and producer handle leader re-election and other cluster disturbances internally
* Opens max 1 tcp connection to a broker per "brod_client", one can create more cleints if needed
* Producer: will start to batch automatically when number of unacknowledged (in flight) requests exceeds configurable maximum
* Producer: will try to re-send buffered messages on common errors like "Not a leader for partition", errors are resolved automatically by refreshing metadata
* Consumer: has a configurable "prefetch count" - it will send this many "fetch" requests to kafka while waiting for acknowledgement from a "subscriber" process on previously delivered messages

# Missing features

* New (0.9) API for consumer groups (work in progress)
* Compression
* Auto-partitioning

# Building and testing

    make
    make t

# Quick start

"client" in brod is a process responsible for establishing and maintaining
connections to kafka cluster. It also manages producer and consumer processes.

You can use brod:start_link_client/3 or brod:start_link_client/5 to start
a client on demand, or include its configuration in sys.config.

A required parameter for client is kafka endpoint(s). Optional parameters
are producers config and/or consumers config.

Example of configuration (for sys.config):

```
[{brod,
   [ { clients
     , [ { brod_client_1 %% registered name
         , [ { endpoints, [{"localhost", 9092}]}
           , { config
             , [ {restart_delay_seconds, 10}] %% connection error
             }
           , { producers
             , [ { <<"brod-test-topic-1">>
                   , [ {topic_restart_delay_seconds, 10} %% topic error
                     , {partition_restart_delay_seconds, 2} %% partition error
                     , {required_acks, -1}
                     ]
                 }
               ]
             }
           , { consumers
             , [ { <<"brod-test-topic-1">>
                   , [ {topic_restart_delay_seconds, 10} %% topic error
                     , {partition_restart_delay_seconds, 2} %% partition error
                     , {begin_offset, -1}
                     ]
                 }
               ]
             }
           %% other producers and consumers in client_1 will share the same
           %% set of connections to the the kafka cluster at endpoints
           %%  specified above for client_1
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

    {ok, ClientPid} =
      brod:start_link_client(_ClientId  = brod_client_1
                             _Endpoints = [{"localhost", 9092}],
                             _Config    = [] %% use default client configs
                             _Producers = [{<<"brod-test-topic-1">>,
                                             []} %% use default producer configs
                             _Consumers = [{<<"brod-test-topic-1">>,
                                             []} %% use default consumer configs
                             ]),

## Producer

### Produce to a known topic-partition:

    {ok, CallRef} =
      brod:produce(_Client    = brod_client_1, %% may also be the pid
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
      brod:produce(_Client    = brod_client_1, %% may also be the pid
                   _Topic     = <<"brod-test-topic-1">>,
                   _Partition = 0
                   _Key       = <<"some-key">>
                   _Value     = <<"some-value">>),
    brod:sync_produce_request(CallRef).

or the same in one call:

    brod:produce_sync(_Client    = brod_client_1, %% may also be the pid
                      _Topic     = <<"brod-test-topic-1">>,
                      _Partition = 0
                      _Key       = <<"some-key">>
                      _Value     = <<"some-value">>).

### Using your own partionner (e.g. random):

    Client = brod_client_1, %% may also be the pid
    Topic  = <<"brod-test-topic-1">>,
    {ok, Partitions} = brod:get_partitions(Client, Topic),
    ProduceFun = fun(Key, Value) ->
                    Partition = random:uniform(1, length(Partitions)),
                    brod:produce(Client, Topic, Partition, Key, Value)
                  end,
    case ProduceFun(<<"some-key">>, <<"some-value">>) of
      {ok, CallRef} ->
        %% keep the reference to match on acknowledgment
      {error, {producer_down, _} ->
        %% maybe retry?
    end,

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

TODO

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
