# Kafka client library in Erlang
Supports v0.8+.  
Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

# Building and testing
    make
    make tests

# Quick start

## Start producers

### Permanent producers

In sys.config

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
           %% other producers in client_1 will share the same set of connections
           %% to the the kafka cluster at endpoints specified above for client_1
           ]
         }
       ]
     }
     %% start another client if producing to another kafka cluster
     %% or if you think it's necessary to start another set of tcp connections
   ]
}]
```

### Start producers linked to caller process

Start a client

    % Producer will be linked to the calling process
    {ok, ClientPid} =
      brod:start_link_client(_ClientId  = brod_client_1
                             _Endpoints = [{"localhost", 9092}],
                             _Config    = [] %% use default client configs
                             _Producers = [{<<"brod-test-topic-1">>,
                                             []} %% use default producer configs
                             ]),

### Produce messages

#### Assertively produce to a known topic-partition:

    {ok, CallRef} =
      brod:produce(_Client    = brod_client_1, %% may also be the pid
                   _Topic     = <<"brod-test-topic-1">>,
                   _Partition = 0
                   _Key       = <<"some-key">>
                   _Value     = <<"some-value">>),

#### Use your own partionner (e.g. random):

    Client = brod_client_1, %% may also be the pid
    Topic  = <<"brod-test-topic-1">>,
    {ok, Partitions} = brod:get_partitions(Client, Topic),
    ProduceFun = fun(Key, Value) ->
                    Partition = random:uniform(1, length(Partitions)),
                    brod:produce(Client, Topic, Partition, Key, Value)
                  end,
    case ProduceFun(<<"some-key">>, <<"some-value">>) of
      {ok, CallRef} ->
        %% maybe keep the reference ?
      {error, {producer_down, _} ->
        %% maybe retry ?
    end,

### Handle acks from kafka

Unless brod:produce_sync was called, callers of brod:produce should 
expect a message of below pattern for each produce call. 
Add `-include_libi("brod/include/brod.hrl").` to use the record.

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
i.e. If the caller is producing to two or more partitions, 
it may receive replies ordered differently than in which order 
bord:produce API was called.

## Consumer as a part of an application

Include brod.hrl:

    -include_lib("brod/include/brod.hrl").

In gen_server's init/1:

    % Consumer will be linked to the calling process
    {ok, Consumer} = brod:start_link_consumer(Hosts, Topic, Partition),
    Self = self(),
    Callback = fun(#message_set{} = MsgSet) -> gen_server:call(Self, MsgSet) end,
    ok = brod:consume(Consumer, Callback, Offset),
    {ok, #state{ consumer = Consumer }}.

Handler:

    handle_call(#message_set{messages = Msgs}, _From, State) ->
      lists:foreach(
        fun(#message{offset = Offset, key = K, value = V}) ->
            io:format(user, "[~p] ~s:~s\n", [Offset, K, V]),
        end, Msgs),
      {reply, ok, State};

## More simple consumer callback (no need to include brod.hrl)

In gen_server's init/1:

    {ok, Consumer} = brod:start_link_consumer(Hosts, Topic, Partition),
    Self = self(),
    Callback = fun(Offset, Key, Value) -> gen_server:call(Self, {msg, Offset, Key, Value}) end,
    ok = brod:consume(Consumer, Callback, Offset),
    {ok, #state{ consumer = Consumer }}.

Handler:

    handle_call({msg, Offset, Key, Value}, _From, State) ->
      io:format(user, "[~p] ~s:~s\n", [Offset, Key, Value]),
      {reply, ok, State};

## In erlang shell
    rr(brod).
    Hosts = [{"localhost", 9092}].
    Topic = <<"t">>.
    Partition = 0.
    {ok, Producer} = brod:start_link_producer(Hosts).
    {ok, Consumer} = brod:start_link_consumer(Hosts, Topic, Partition).
    Callback = fun(Offset, Key, Value) -> io:format(user, "[~p] ~s:~s\n", [Offset, Key, Value]) end.
    ok = brod:consume(Consumer, Callback, -1).
    brod:produce_sync(Producer, Topic, Partition, <<"k">>, <<"v">>).

## Simple consumers
    f(C), C = brod:console_consumer(Hosts, Topic, Partition, -1).
    % this will print messages from the Topic on your stdout
    C ! stop.
    f(C), C = brod:file_consumer(Hosts, Topic, Partition, -1, "/tmp/kafka.log").
    % this will write messages to /tmp/kafka.log
    C ! stop.

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
