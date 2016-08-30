%%%
%%%   Copyright (c) 2015, Klarna AB
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
-module(brod_producer_buffer_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_no_ack/1
        , t_random_latency_ack/1
        , t_nack/1
        , t_send_fun_error/1
        ]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) -> Config.

end_per_testcase(_Case, Config) -> Config.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

t_no_ack(Config) when is_list(Config) ->
  Opts = [{numtests, 1000}, {to_file, user}],
  ?assert(proper:quickcheck(prop_no_ack_run(), Opts)).

t_random_latency_ack(Config) when is_list(Config) ->
  Opts = [{numtests, 500}, {to_file, user}],
  ?assert(proper:quickcheck(prop_random_latency_ack_run(), Opts)).

t_nack(Config) when is_list(Config) ->
  SendFun =
    fun(SockPid, KvList) ->
      CorrId = make_ref(),
      NumList = lists:map(fun({Bin, Bin}) ->
                            list_to_integer(binary_to_list(Bin))
                          end, KvList),
      SockPid ! {produce, CorrId, NumList},
      {ok, CorrId}
    end,
  Buf0 = brod_producer_buffer:new(_BufferLimit = 2,
                                  _OnWireLimit = 2,
                                  _MaxBatchSize = 4, %% 4 bytes, 2 messages
                                  _MaxRetry = 1,
                                  SendFun),
  AddFun =
    fun(BufIn, Num) ->
      CallRef = #brod_call_ref{ caller = self()
                              , callee = ignore
                              , ref    = Num
                              },
      Bin = list_to_binary(integer_to_list(Num)),
      {ok, BufOut} = brod_producer_buffer:add(BufIn, CallRef, Bin, Bin),
      BufOut
    end,
  MaybeSend =
    fun(BufIn) ->
      {ok, BufOut} = brod_producer_buffer:maybe_send(BufIn, self()),
      BufOut
    end,
  AckFun =
    fun(BufIn, CorrId) ->
      {ok, BufOut} = brod_producer_buffer:ack(BufIn, CorrId),
      BufOut
    end,
  NackFun =
    fun(BufIn, CorrId) ->
      {ok, BufOut} = brod_producer_buffer:nack(BufIn, CorrId, test),
      BufOut
    end,
  ReceiveFun =
    fun(ExpectedNums) ->
      receive
        {produce, CorrId_, NumList} ->
          ExpectedNums =:= NumList orelse
            erlang:error({ExpectedNums, NumList}),
          CorrId_
      after 1000 ->
        erlang:error("timed out receiving produce message")
      end
    end,
  Buf1 = AddFun(Buf0, 0),
  Buf2 = AddFun(Buf1, 1),
  Buf3 = AddFun(AddFun(Buf2, 2), 3),
  Buf4 = MaybeSend(Buf3),
  CorrId1 = ReceiveFun([0, 1]),
  Buf5 = NackFun(Buf4, CorrId1),
  Buf6 = MaybeSend(Buf5),
  CoorId2 = ReceiveFun([0]),
  ?assertEqual(Buf6, MaybeSend(Buf6)), %% no new request before last is acked
  Buf7 = AckFun(Buf6, CoorId2),
  Buf8 = MaybeSend(Buf7),
  Buf9 = MaybeSend(Buf8), %% allow sending another one now
  CorrId3 = ReceiveFun([1]),
  CorrId4 = ReceiveFun([2, 3]),
  BufA = AckFun(Buf9, CorrId3),
  BufB = AckFun(BufA, CorrId4),
  ?assert(brod_producer_buffer:is_empty(BufB)).

t_send_fun_error(Config) when is_list(Config) ->
  SendFun =
    fun(_SockPid, _KvList) ->
      {error, "the reason"}
    end,
  Buf0 = brod_producer_buffer:new(_BufferLimit = 1,
                                  _OnWireLimit = 1,
                                  _MaxBatchSize = 10000,
                                  _MaxRetry = 1,
                                  SendFun),
  AddFun =
    fun(BufIn, Num) ->
      CallRef = #brod_call_ref{ caller = self()
                              , callee = ignore
                              , ref    = Num
                              },
      Bin = list_to_binary(integer_to_list(Num)),
      {ok, BufOut} = brod_producer_buffer:add(BufIn, CallRef, Bin, Bin),
      BufOut
    end,
  MaybeSend =
    fun(BufIn) ->
      {retry, BufOut} = brod_producer_buffer:maybe_send(BufIn, self()),
      BufOut
    end,
  Buf1 = AddFun(AddFun(Buf0, 0), 1),
  Buf2 = MaybeSend(Buf1),
  ?assertException(exit, {reached_max_retries, "the reason"},
                   MaybeSend(Buf2)).

%%%_* Help functions ===========================================================

-define(MAX_DELAY, 3).

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
  SendFun = fun(_SockPid, _KvList) -> ok end,
  ?FORALL(
    {BufferLimit, OnWireLimit, MsgSetBytes, ValueList},
    {prop_buffer_limit(), prop_onwire_limit(),
     prop_msgset_bytes(), prop_value_list()},
    begin
      KeyList = lists:seq(1, length(ValueList)),
      KvList = lists:zip(KeyList, ValueList),
      Buf = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                     MsgSetBytes, _MaxRetries = 0,
                                     SendFun),
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
      SendFun = fun(_SockPid, KvList_) -> SendFun0(FakeKafka, KvList_) end,
      Buf = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                     MsgSetBytes, _MaxRetries = 0,
                                     SendFun),
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
  {ok, Buf1} = brod_producer_buffer:add(Buf, CallRef, BinKey, Value),
  FakeSockPid = self(),
  {ok, NewBuf} = brod_producer_buffer:maybe_send(Buf1, FakeSockPid),
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
  case brod_producer_buffer:is_empty(Buf) of
    true ->
      {Buffered, Acked};
    false ->
      {NewBuffered, NewAcked, NewBuf} =
        collect_replies(Buffered, Acked, Buf, ?MAX_DELAY),
      produce_loop(FakeKafka, NewBuf, [], NewBuffered, NewAcked)
  end;
produce_loop(FakeKafka, Buf0, [{Key, Value} | Rest], Buffered, Acked) ->
  CallRef = #brod_call_ref{ caller = self()
                          , callee = ignore
                          , ref    = Key
                          },
  BinKey = list_to_binary(integer_to_list(Key)),
  FakeSockPid = self(),
  {ok, Buf1} = brod_producer_buffer:add(Buf0, CallRef, BinKey, Value),
  {ok, Buf2} = brod_producer_buffer:maybe_send(Buf1, FakeSockPid),
  {NewBuffered, NewAcked, Buf} = collect_replies(Buffered, Acked, Buf2, 0),
  produce_loop(FakeKafka, Buf, Rest, NewBuffered, NewAcked).

collect_replies(Buffered, Acked, Buf0, Timeout) ->
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_buffered
                       } ->
      collect_replies([Key | Buffered], Acked, Buf0, Timeout);
    {ack_from_kafka, CorrId} ->
      FakeSockPid = self(), %% any pid should work
      {ok, Buf1} = brod_producer_buffer:ack(Buf0, CorrId),
      {ok, Buf} = brod_producer_buffer:maybe_send(Buf1, FakeSockPid),
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
  erlang:spawn_link(fun() -> fake_kafka_loop() end).

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

fake_kafka_loop() ->
  receive
    {produce, FromPid, CorrId, KvList} ->
      ok = fake_kafka_process_msgs(KvList),
      FromPid ! {ack_from_kafka, CorrId},
      fake_kafka_loop();
    stop ->
      exit(normal);
    Msg ->
      exit({fake_kafka, unexpected, Msg})
  end.

fake_kafka_process_msgs([]) -> ok;
fake_kafka_process_msgs([{_Key, {DelayMs, _Value}} | Rest]) ->
  timer:sleep(DelayMs),
  fake_kafka_process_msgs(Rest).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
