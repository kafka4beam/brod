%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%=============================================================================
%%% @doc
%%% @copyright 2015 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_producer_buffer_tests).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("src/brod_int.hrl").

%%%_* Test functions ===========================================================

no_ack_test_() ->
  {timeout, 10,
   fun() -> ?assert(proper:quickcheck(prop_no_ack_run(), 1000)) end
  }.

random_latency_ack_test_() ->
  {timeout, 60,
   fun() -> ?assert(proper:quickcheck(prop_random_latency_ack_run(), 1000)) end
  }.

%%%_* Help functions ===========================================================

-define(MAX_DELAY, 5).

prop_buffer_limit() -> proper_types:pos_integer().
prop_onwire_limit() -> proper_types:pos_integer().
prop_msgset_bytes() -> proper_types:pos_integer().
prop_value_list() -> proper_types:list(proper_types:binary()).

%% latency in milliseconds for fake kafka to process a key-value pair
prop_latency_ms() -> proper_types:range(0, ?MAX_DELAY).

%% pre-generate the latency together with the binary value.
prop_value_with_processing_latency_list() ->
  proper_types:list({prop_latency_ms(), proper_types:binary()}).

prop_no_ack_run() ->
  SendFun = fun(_KvList) -> ok end,
  ?FORALL(
    {BufferLimit, OnWireLimit, MsgSetBytes, ValueList},
    {prop_buffer_limit(), prop_onwire_limit(),
     prop_msgset_bytes(), prop_value_list()},
    begin
      KeyList = lists:seq(1, length(ValueList)),
      KvList = lists:zip(KeyList, ValueList),
      Buf = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                     MsgSetBytes, SendFun),
      no_ack_produce(Buf, KvList)
    end).

prop_random_latency_ack_run() ->
  SendFun0 =
    fun(FakeKafka, KvList) ->
      %% use reference as correlation to simplify test
      CorrId = make_ref(),
      %% send the message to fake kafka
      %% the pre-generated latency values are in KvList
      %% fake kafka should receive the KvList, sleep a while
      %% and reply ack
      FakeKafka ! {produce, self(), CorrId, KvList},
      {ok, CorrId}
    end,
  ?FORALL(
    {BufferLimit, OnWireLimit, MsgSetBytes, ValueList},
    {prop_buffer_limit(), prop_onwire_limit(),
     prop_msgset_bytes(), prop_value_with_processing_latency_list()},
    begin
      KeyList = lists:seq(1, length(ValueList)),
      KvList = lists:zip(KeyList, ValueList),
      FakeKafka = spawn_fake_kafka(),
      SendFun = fun(KvList_) -> SendFun0(FakeKafka, KvList_) end,
      Buf = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                     MsgSetBytes, SendFun),
      random_latency_ack_produce(FakeKafka, Buf, KvList)
    end).

no_ack_produce(Buf, []) ->
  brod_producer_buffer:is_empty(Buf) orelse
    erlang:error({buffer_not_empty, Buf});
no_ack_produce(Buf, [{Key, Value} | Rest]) ->
  CallRef = #brod_call_ref{ caller = self()
                          , callee = ignore
                          , ref    = Key
                          },
  BinKey = list_to_binary(integer_to_list(Key)),
  NewBuf = brod_producer_buffer:maybe_send(Buf, CallRef, BinKey, Value),
  %% in case of no ack required, expect 'buffered' immediately
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_buffered
                       } ->
      ok
    after 100 ->
      erlang:error({timeout, brod_produce_req_buffered, Key})
  end,
  %% in case of no ack required, expect 'acked' immediately
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_acked
                       } ->
      ok
    after 100 ->
      erlang:error({timeout, brod_produce_req_acked, Key})
  end,
  no_ack_produce(NewBuf, Rest).

random_latency_ack_produce(FakeKafka, Buf, KvList) ->
  {Buffered, Acked} =
    produce_loop(FakeKafka, Buf, KvList, _Buffered = [], _Acked = []),
  N = length(KvList),
  ok = assert_reply_sequence(Buffered, N),
  ok = assert_reply_sequence(Acked, N),
  ok = stop_fake_kafka(FakeKafka),
  true.

produce_loop(FakeKafka, Buf, [], Buffered, Acked) ->
  %% no more to send, block wait for all acks if still pending
  try
    FakeKafka ! flush
  catch _ : _ ->
    ok
  end,
  {NewBuffered, NewAcked, NewBuf} =
    collect_replies(Buffered, Acked, Buf, ?MAX_DELAY),
  brod_producer_buffer:is_empty(NewBuf) orelse
    erlang:error({buffer_not_empty, NewBuf}),
  {NewBuffered, NewAcked};
produce_loop(FakeKafka, Buf0, [{Key, Value} | Rest], Buffered, Acked) ->
  CallRef = #brod_call_ref{ caller = self()
                          , callee = ignore
                          , ref    = Key
                          },
  BinKey = list_to_binary(integer_to_list(Key)),
  Buf1 = brod_producer_buffer:maybe_send(Buf0, CallRef, BinKey, Value),
  {NewBuffered, NewAcked, Buf} = collect_replies(Buffered, Acked, Buf1, 0),
  produce_loop(FakeKafka, Buf, Rest, NewBuffered, NewAcked).

collect_replies(Buffered, Acked, Buf0, Timeout) ->
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_buffered
                       } ->
      collect_replies([Key | Buffered], Acked, Buf0, Timeout);
    {ack_from_kafka, CorrId} ->
      Buf = brod_producer_buffer:ack(Buf0, CorrId),
      collect_replies(Buffered, Acked, Buf, Timeout);
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_acked
                       } ->
      collect_replies(Buffered, [Key | Acked], Buf0, Timeout);
    Msg ->
      erlang:error({unexpected, Msg})
  after Timeout ->
    {Buffered, Acked, Buf0}
  end.

%% reply collection was accumulated in reversed order.
assert_reply_sequence([], 0) -> ok;
assert_reply_sequence([N | Rest], N) ->
  assert_reply_sequence(Rest, N-1).

spawn_fake_kafka() ->
  erlang:spawn_link(fun() -> fake_kafka_loop(_IsFlushing = false) end).

stop_fake_kafka(FakeKafka) when is_pid(FakeKafka) ->
  MRef = monitor(process, FakeKafka),
  FakeKafka ! stop,
  receive
    {'DOWN', MRef, process, FakeKafka, _} ->
     ok
  after 1000 ->
    exit(FakeKafka, kill),
    erlang:error(timeout)
  end.

fake_kafka_loop(IsFlushing) ->
  receive
    {produce, FromPid, CorrId, KvList} ->
      NewIsFlushing = fake_kafka_process_msgs(IsFlushing, KvList),
      FromPid ! {ack_from_kafka, CorrId},
      fake_kafka_loop(NewIsFlushing);
    stop ->
      exit(normal);
    flush ->
      fake_kafka_loop(true);
    Msg ->
      exit({fake_kafka, unexpected, Msg})
  end.

fake_kafka_process_msgs(IsFlushing, []) -> IsFlushing;
fake_kafka_process_msgs(IsFlushing, [{_Key, {DelayMs0, _Value}} | Rest]) ->
  DelayMs = case IsFlushing of
              true  -> 0;
              false -> DelayMs0
            end,
  receive
    flush ->
      fake_kafka_process_msgs(true, Rest)
  after DelayMs ->
    fake_kafka_process_msgs(IsFlushing, Rest)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
