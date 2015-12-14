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

no_ack_test() ->
  ?assert(proper:quickcheck(prop_no_ack_run(), 1000)).

%%%_* Help functions ===========================================================

prop_buffer_limit() -> proper_types:pos_integer().
prop_onwier_limit() -> proper_types:pos_integer().
prop_msgset_bytes() -> proper_types:pos_integer().
prop_value_list() -> proper_types:list(proper_types:binary()).

prop_no_ack_run() ->
  SendFun = fun(_KvList) -> ok end,
  ?FORALL(
    {BufferLimit, OnWireLimit, MsgSetBytes, ValueList},
    {prop_buffer_limit(), prop_onwier_limit(),
     prop_msgset_bytes(), prop_value_list()},
    begin
      KeyList = lists:seq(1, length(ValueList)),
      KvList = lists:zip(KeyList, ValueList),
      Buf = brod_producer_buffer:new(BufferLimit, OnWireLimit,
                                     MsgSetBytes, SendFun),
      no_ack_run(Buf, KvList)
    end).

no_ack_run(Buf, KvList) ->
  Pid = spawn_fake_caller(),
  true = no_ack_run(Pid, Buf, KvList),
  true = assert_ack_sequence(Pid, length(KvList)),
  Pid ! stop,
  true.

no_ack_run(_FakeCaller, Buf, []) ->
  brod_producer_buffer:is_empty(Buf) orelse
    erlang:error({buffer_not_empty, Buf});
no_ack_run(FakeCaller, Buf0, [{Key, Value} | Rest]) ->
  CallRef = #brod_call_ref{ caller = FakeCaller
                          , callee = ignore
                          , ref    = Key
                          },
  BinKey = list_to_binary(integer_to_list(Key)),
  Buf = brod_producer_buffer:maybe_send(Buf0, CallRef, BinKey, Value),
  no_ack_run(FakeCaller, Buf, Rest).

spawn_fake_caller() ->
  spawn_link(fun() -> fake_caller_loop([], []) end).

fake_caller_loop(Buffered, Acked) ->
  receive
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_buffered
                       } ->
      fake_caller_loop([Key | Buffered], Acked);
    #brod_produce_reply{ call_ref = #brod_call_ref{ref = Key}
                       , result   = brod_produce_req_acked
                       } ->
      lists:member(Key, Buffered) orelse
        exit("'acked' reply came before 'buffered'"),
      fake_caller_loop(Buffered, [Key | Acked]);
    {take_result, Pid, Ref} ->
      Pid ! {Ref, lists:reverse(Buffered), lists:reverse(Acked)},
      fake_caller_loop([], []);
    stop ->
      exit(normal);
    Msg ->
      exit({unexpected, Msg})
  end.

assert_ack_sequence(FakeCallerPid, ExpectedReplyCount) ->
  Ref = make_ref(),
  FakeCallerPid ! {take_result, self(), Ref},
  receive
    {Ref, Buffered, Acked} ->
      is_sequential(Buffered, ExpectedReplyCount) andalso
        is_sequential(Acked, ExpectedReplyCount)
  after 1000 ->
    erlang:error(timeout)
  end.

is_sequential(Keys, ExpectedReplyCount) ->
  Keys =:= lists:seq(1, ExpectedReplyCount).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
