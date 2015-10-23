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
    {ok, Producer} = brod:start_link_producer(Hosts, RequiredAcks, AckTimeout, ClientId),
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

    make escriptize
    ./brod help
