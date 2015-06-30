# Kafka client library in Erlang
Supports v0.8+.  
Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

# Building and testing
    make
    make clean test

# Usage
## Producer as a part of an application

In gen_server's init/1:

    % Producer will be linked to the calling process
    {ok, Producer} = brod:start_link_producer(Hosts, RequiredAcks, AckTimeout),
    {ok, #state{ producer = Producer }}.

Sending a message:

    {ok, Ref} = brod:produce( State#state.producer
                            , Topic
                            , Partition
                            , [{Key, Value}]),
    % remember the Ref somewhere, e.g. in #state{}

Handling acks from kafka broker:

    handle_info({{Ref, P}, ack}, #state{producer = P} = State) ->
      % do something with Ref, e.g. notify a client and remove from #state{}

Sending a message with a blocking call:

    ok = brod:produce_sync( State#state.producer
                          , Topic
                          , Partition
                          , [{Key, Value}]).

## Consumer as a part of an application

Include brod.hrl:

    -include_lib("brod/include/brod.hrl").

In gen_server's init/1:

    % Consumer will be linked to the calling process
    {ok, Consumer} = brod:start_link_consumer(Hosts, Topic, Partition),
    ok = brod:consume(Consumer, self(), Offset, 1000, 0, 100000),
    {ok, #state{ consumer = Consumer }}.

Handling payloads from kafka broker:

    handle_info(#message_set{messages = Msgs}, State) ->
      lists:foreach(
        fun(#message{key = K, value = V}) ->
            io:format(Io, "\n~s\n~s\n", [K, V])
        end, Msgs).

## In erlang shell
    rr(brod).
    Hosts = [{"localhost", 9092}].
    Topic = <<"t">>.
    Key = <<"key">>.
    Value = <<"value">>.
    Partition = 0.
    {ok, Producer} = brod:start_link_producer(Hosts).
    {ok, Consumer} = brod:start_link_consumer(Hosts, Topic, Partition).
    ok = brod:consume(Consumer, -1).
    {ok, Ref} = brod:produce(Producer, Topic, Partition, Key, Value).
    receive {{Ref, Producer}, ack} -> ok end.
    receive #message_set{messages = [#message{key = Key, value = Value}]} -> ok end.

More advanced versions of the functions above are also available, see brod.erl.

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
