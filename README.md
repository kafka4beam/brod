Kafka client library in Erlang
------------------------------
Supports v0.8+.  
Why "brod"? [http://en.wikipedia.org/wiki/Max_Brod](http://en.wikipedia.org/wiki/Max_Brod)

Usage
-----
    Hosts = [{"kafka-host1", 9092}, {"kafka-host2", 9092}].
    Topic = <<"topic">>.
    Msg = <<"test">>.
    Partition = 0.
    {ok, Producer} = brod:start_producer(Hosts).
    {ok, Consumer} = brod:start_consumer(Hosts, Topic, Partition).
    ok = brod:consume(Consumer, -1).
    {ok, Ref} = brod:produce(Producer, Topic, Partition, Msg).
    receive {{Ref, Producer}, ack} -> ok end.
    receive #message_set{messages = [#message{value = Msg}]} -> ok end.

More advanced versions of the functions above are also available, see brod.erl.

Other API to play with/inspect kafka
-------------------------------
These functions open a connetion to kafka cluster, send a request,
await response and then close the connection.

    Hosts = [{"kafka-host1", 9092}, {"kafka-host2", 9092}].
    Topic = <<"topic">>.
    Partition = 0.
    brod:get_metadata(Hosts).
    brod:get_metadata(Hosts, [Topic]).
    brod:get_offsets(Hosts, Topic, Partition).
    brod:fetch(Hosts, Topic, Partition, 1).
