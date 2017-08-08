%%%
%%%   Copyright (c) 2015-2017, Klarna AB
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
-include("brod_int.hrl").

%% producer state
-record(state, { buffered  = []
               , acked     = []
               , delay_ref = ?undef :: ?undef | {timer:tref() | reference()}
               , buf
               }).

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
  UnknownCorrId = 0,
  SendFun =
    fun(SockPid, KvList) ->
      CorrId = case get(<<"t_nack_corr_id">>) of
                 undefined -> UnknownCorrId + 1;
                 N         -> N + 1
               end,
      put(<<"t_nack_corr_id">>, CorrId),
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
                                  _MaxLingerTime = 0,
                                  _MaxLingerCount = 0,
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
      brod_producer_buffer:nack(BufIn, CorrId, test)
    end,
  ReceiveFun =
    fun(Line, ExpectedNums) ->
      receive
        {produce, CorrId_, NumList} ->
          case ExpectedNums =:= NumList of
            true ->
              ok;
            false ->
              ct:fail("~p\nexp=~p\ngot=~p\n", [Line, ExpectedNums, NumList])
          end,
          CorrId_
      after 1000 ->
        erlang:error({Line, "timed out receiving produce message"})
      end
    end,
  Buf1 = AddFun(Buf0, 0),
  Buf2 = AddFun(Buf1, 1),
  Buf3 = AddFun(AddFun(Buf2, 2), 3),
  Buf4 = MaybeSend(Buf3),
  CorrId1 = ReceiveFun(?LINE, [0, 1]), %% max batch size
  CorrId2 = ReceiveFun(?LINE, [2, 3]), %% max onwire is 2
  ?assertEqual({error, CorrId1}, NackFun(Buf4, UnknownCorrId)),
  ?assertException(exit, {bad_order, _, _}, NackFun(Buf4, CorrId2)),
  {ok, Buf5} = NackFun(Buf4, CorrId1), %% re-queue all
  Buf6 = MaybeSend(Buf5),              %% as if a sheduled retry
  CorrId3 = ReceiveFun(?LINE, [0, 1]), %% receive a max batch
  CorrId4 = ReceiveFun(?LINE, [2, 3]), %% another max batch (max onwire is 2)
  Buf7 = AckFun(Buf6, CorrId3),
  Buf8 = AckFun(Buf7, CorrId4),
  ?assert(brod_producer_buffer:is_empty(Buf8)).

t_send_fun_error(Config) when is_list(Config) ->
  SendFun =
    fun(_SockPid, _KvList) ->
      {error, "the reason"}
    end,
  Buf0 = brod_producer_buffer:new(_BufferLimit = 1,
                                  _OnWireLimit = 1,
                                  _MaxBatchSize = 10000,
                                  _MaxRetry = 1,
                                  _MaxLingerTime = 0,
                                  _MaxLingerCount = 0,
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
prop_linger_time() -> proper_types:integer(0, 10).
prop_linger_count() -> proper_types:integer(0, 100).
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
                                     _MaxLingerTime = 0, _MaxLingerCount = 0,
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
    {BufferLimit, OnWireLimit, MsgSetBytes,
     MaxLingerTime, MaxLingerCount, ValueList},
    {prop_buffer_limit(), prop_onwire_limit(),
     prop_msgset_bytes(), prop_linger_time(), prop_linger_count(),
     prop_value_with_processing_latency_list()},
    begin
      KeyList = lists:seq(1, length(ValueList)),
      KvList = lists:zip(KeyList, ValueList),
      FakeKafka = spawn_fake_kafka(),
      SendFun = fun(_SockPid, KvList_) -> SendFun0(FakeKafka, KvList_) end,
      Buf = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                     MsgSetBytes, _MaxRetries = 0,
                                     MaxLingerTime, MaxLingerCount, SendFun),
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
  State0 = #state{buf = Buf, buffered = [], acked = []},
  #state{buffered = Buffered, acked = Acked} =
    produce_loop(FakeKafka, KvList, State0),
  N = length(KvList),
  ok = assert_reply_sequence(Buffered, N),
  ok = assert_reply_sequence(Acked, N),
  ok = stop_fake_kafka(FakeKafka),
  true.

produce_loop(FakeKafka, [], #state{buf = Buf} = State) ->
  case brod_producer_buffer:is_empty(Buf) of
    true ->
      State;
    false ->
      NewState = collect_replies(State, ?MAX_DELAY),
      produce_loop(FakeKafka, [], NewState)
  end;
produce_loop(FakeKafka, [{Key, Value} | Rest], State0) ->
  #state{buf = Buf0} = State0,
  CallRef = #brod_call_ref{ caller = self()
                          , callee = ignore
                          , ref    = Key
                          },
  BinKey = list_to_binary(integer_to_list(Key)),
  {ok, Buf1} = brod_producer_buffer:add(Buf0, CallRef, BinKey, Value),
  State1 = State0#state{buf = Buf1},
  State2 = maybe_send(State1),
  State = collect_replies(State2, _Delay = 0),
  produce_loop(FakeKafka, Rest, State).

collect_replies(#state{ buffered  = Buffered
                      , acked     = Acked
                      , buf       = Buf0
                      , delay_ref = DelayRef
                      } = State0, Timeout) ->
  receive
    {delayed_send, Ref} when is_tuple(DelayRef) andalso
                             Ref =:= element(2, DelayRef) ->
      State = maybe_send(State0#state{delay_ref = ?undef}),
      collect_replies(State, Timeout);
    {delayed_send, _} ->
      %% stale message
      collect_replies(State0, Timeout);
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_buffered
                       } ->
      State = State0#state{buffered = [Key | Buffered]},
      collect_replies(State, Timeout);
    {ack_from_kafka, CorrId} ->
      {ok, Buf1} = brod_producer_buffer:ack(Buf0, CorrId),
      State1 = State0#state{buf = Buf1},
      State = maybe_send(State1),
      collect_replies(State, Timeout);
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_acked
                       } ->
      State = State0#state{acked = [Key | Acked]},
      collect_replies(State, Timeout);
    Msg ->
      erlang:error({unexpected, Msg})
  after Timeout ->
    State0
  end.

maybe_send(#state{buf = Buf0, delay_ref = DelayRef} = State) ->
  SendTo = self(),
  _ = cancel_delay_send_timer(DelayRef),
  case brod_producer_buffer:maybe_send(Buf0, SendTo) of
    {ok, Buf} ->
      State#state{buf = Buf};
    {{delay, Timeout}, Buf} ->
      NewDelayRef = start_delay_send_timer(Timeout),
      State#state{buf = Buf, delay_ref = NewDelayRef}
  end.

%% @private Start delay send timer.
start_delay_send_timer(Timeout) ->
  MsgRef = make_ref(),
  TRef = erlang:send_after(Timeout, self(), {delayed_send, MsgRef}),
  {TRef, MsgRef}.

%% @private Ensure delay send timer is canceled.
%% But not flushing the possibly already sent (stale) message
%% Stale message should be discarded in handle_info
%% @end
cancel_delay_send_timer(?undef) -> ok;
cancel_delay_send_timer({Tref, _Msg}) -> _ = erlang:cancel_timer(Tref).

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
