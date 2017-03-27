-module(test_kafka).

-export([init/0, run/4]).

-define(TOPIC, <<"test-topic">>).
-define(ENDPOINTS_DEV, [{"localhost", 9092}]).
-define(ENDPOINTS, ?ENDPOINTS_DEV).
-define(PINGPONG_SERVER, pingpong_server).

-include("brod.hrl").

-define(CLIENT1, client1).

flush_reply(0) -> 0;
flush_reply(N) ->
  receive
    #brod_produce_reply{result = brod_produce_req_acked} ->
      flush_reply(N-1)
  after
    0 ->
      N
  end.

loop(0, _, _, 0) ->
  ok;
loop(0, P, W, N) ->
  loop(0, P, W, flush_reply(N));
loop(ReqNr, Producer, pingpong, 0) ->
  ok = ping(),
  loop(ReqNr - 1, Producer, pingpong, 0);
loop(ReqNr, Producer, produce, ExpectedReplies) ->
  case brod:produce(Producer, <<"k1">>, <<"hello world!">>) of
    {ok, _} -> ok;
    Error   -> io:format("unexpected response error: ~p ~n", [Error])
  end,
  loop(ReqNr -1, Producer, produce, flush_reply(ExpectedReplies));
loop(ReqNr, Producer, produce_cast, ExpectedReplies) ->
  case brod:produce_cast(Producer, [{<<"k1">>, <<"hello world!">>}]) of
    {ok, _} -> ok;
    Error   -> io:format("unexpected response error: ~p ~n", [Error])
  end,
  _ = process_info(Producer, message_queue_len),
  loop(ReqNr -1, Producer, produce_cast, flush_reply(ExpectedReplies)).

init() ->
  case application:ensure_all_started(brod) of
    {ok, [_H|_T] } ->
      ok = brod:start_client(?ENDPOINTS, ?CLIENT1),
      ProducerConfig =
        [ {partition_buffer_limit, 1000}
        , {partition_onwire_limit, 1}
        , {max_batch_size, 1000000}
        , {required_acks, 1}
        , {linger_age_ms, 10}
        ],
      ok = brod:start_producer(?CLIENT1, ?TOPIC, ProducerConfig),
      spawn_link(fun() -> register(?PINGPONG_SERVER, self()), roundtrip_loop() end),
      ok;
    _ ->
      {ok, []}
  end.

-spec run(integer(), integer(), integer(), pinpong | produce | produce_cast) -> ok.
run(ClientsNr, ReqNr, Partition, What) ->
  init(),
  ReqPerProcess = round(ReqNr/ClientsNr),
  ExpectedReplies = case What of
                      pingpong -> 0;
                      _        -> ReqPerProcess
                    end,
  FunTest = fun()->
                {ok, Pid} = brod:get_producer(?CLIENT1, ?TOPIC, Partition),
                ok = loop(ReqPerProcess, Pid, What, ExpectedReplies)
            end,
  {Time, _} = timer:tc(fun()-> do_work(FunTest, ClientsNr) end),
  TimeMs = Time/1000,
  TimeS = TimeMs/1000,
  io:format("## Test complete time: ~p ms => throughput ~p/s ~n", [TimeMs, ReqNr/TimeS]),
  ok.

do_work(Fun, Count) ->
    process_flag(trap_exit, true),
    spawn_childrens(Fun, Count),
    wait_responses(Count).

spawn_childrens(_Fun, 0) ->
    ok;
spawn_childrens(Fun, Count) ->
    spawn_link(Fun),
    spawn_childrens(Fun, Count -1).

wait_responses(0) ->
    ok;
wait_responses(Count) ->
    receive
        {'EXIT',_FromPid, _Reason} ->
            wait_responses(Count -1)
    end.

ping() ->
  Ref = erlang:monitor(process, ?PINGPONG_SERVER),
  ?PINGPONG_SERVER ! {ping, self()},
  receive
    pong ->
      erlang:demonitor(Ref, [flush]),
      ok
  end.

roundtrip_loop() ->
  receive
    {ping, Pid} ->
      Pid ! pong,
      roundtrip_loop()
  end.

