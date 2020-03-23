%%%
%%%   Copyright (c) 2015-2018, Klarna Bank AB (publ)
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

%% @private
-module(brod_topic_subscriber_SUITE).


%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , common_init_per_testcase/2
        , common_end_per_testcase/2
        , suite/0
        ]).

%% brod subscriber callbacks
-export([ init/2
        , handle_message/3
        ]).

%% Test cases
-export([ t_async_acks/1
        , t_demo/1
        , t_demo_message_set/1
        , t_consumer_crash/1
        , t_begin_offset/1
        ]).

-include("brod_test_setup.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include("brod.hrl").

-define(CLIENT_ID, ?MODULE).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
  kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) -> ok.

common_init_per_testcase(Case, Config0) ->
  ok = brod_demo_topic_subscriber:delete_commit_history(?topic(Case, 1)),
  Config = kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config0),
  ok = brod:start_client(bootstrap_hosts(), ?CLIENT_ID, client_config()),
  Config.

common_end_per_testcase(Case, Config) ->
  brod:stop_client(?CLIENT_ID),
  kafka_test_helper:common_end_per_testcase(Case, Config).

%%%_* Topic subscriber callbacks ===============================================

-record(state, { ct_case_ref
               , ct_case_pid
               , is_async_ack
               }).

init(Topic, {CaseRef, CasePid, IsAsyncAck}) ->
  init(Topic, {CaseRef, CasePid, IsAsyncAck, _CommittedOffsets = []});
init(_Topic, {CaseRef, CasePid, IsAsyncAck, CommittedOffsets}) ->
  State = #state{ ct_case_ref  = CaseRef
                , ct_case_pid  = CasePid
                , is_async_ack = IsAsyncAck
                },
  {ok, CommittedOffsets, State}.

handle_message(Partition, Message, #state{ ct_case_ref  = Ref
                                         , ct_case_pid  = Pid
                                         , is_async_ack = IsAsyncAck
                                         } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  %% forward the message to ct case for verification.
  Pid ! {Ref, Partition, Offset, Value},
  case IsAsyncAck of
    true  -> {ok, State};
    false -> {ok, ack, State}
  end.

%%%_* Test functions ===========================================================

t_demo(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_topic_subscriber:bootstrap(1, message),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_demo_message_set(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_topic_subscriber:bootstrap(1, message_set),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_async_acks(Config) when is_list(Config) ->
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  MaxSeqNo       = 100,
  ConsumerConfig = [ {prefetch_count, MaxSeqNo}
                   , {prefetch_bytes, 0} %% as discard
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   ],
  CaseRef = t_async_acks,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true},
  Partition = 0,
  {ok, SubscriberPid} =
    brod:start_link_topic_subscriber(?CLIENT_ID, ?topic, ConsumerConfig,
                                     ?MODULE, InitArgs),
  SendFun =
    fun(I) ->
      Value = integer_to_binary(I),
      produce({?topic, Partition}, Value)
    end,
  RecvFun =
    fun F(Timeout, Acc) ->
      receive
        {CaseRef, Partition, Offset, Value} ->
          ok = brod_topic_subscriber:ack(SubscriberPid, Partition, Offset),
          I = binary_to_integer(Value),
          F(0, [I | Acc]);
        Msg ->
          erlang:error({unexpected_msg, Msg})
      after Timeout ->
        Acc
      end
    end,
  SendFun(0),
  %% wait at most 2 seconds to receive the first message
  %% it may or may not receive the first message (0) depending on when
  %% the consumers starts polling --- before or after the first message
  %% is produced.
  _ = RecvFun(2000, []),
  L = lists:seq(1, MaxSeqNo),
  ok = lists:foreach(SendFun, L),
  %% worst case scenario, the receive loop will cost (100 * 5 + 5 * 1000) ms
  Timeouts = lists:duplicate(MaxSeqNo, 5) ++ lists:duplicate(5, 1000),
  ReceivedL = lists:foldl(RecvFun, [], Timeouts ++ [1,2,3,4,5]),
  ?assertEqual(L, lists:reverse(ReceivedL)),
  ok = brod_topic_subscriber:stop(SubscriberPid),
  ok.

t_begin_offset(Config) when is_list(Config) ->
  ConsumerConfig = [ {prefetch_count, 100}
                   , {prefetch_bytes, 0} %% as discard
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   ],
  CaseRef = t_begin_offset,
  CasePid = self(),
  Partition = 0,
  SendFun =
    fun(I) ->
      Value = integer_to_binary(I),
      produce({?topic, Partition}, Value)
    end,
  RecvFun =
    fun F(Pid, Timeout, Acc) ->
      receive
        {CaseRef, Partition, Offset, Value} ->
          ok = brod_topic_subscriber:ack(Pid, Partition, Offset),
          I = binary_to_integer(Value),
          F(Pid, 0, [{Offset, I} | Acc]);
        Msg ->
          erlang:error({unexpected_msg, Msg})
      after Timeout ->
        Acc
      end
    end,
  _Offset0 = SendFun(111),
  Offset1 = SendFun(222),
  Offset2 = SendFun(333),
  %% Start as if committed Offset1, expect it to start fetching from Offset2
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true,
              _ConsumerOffsets = [{0, Offset1}]},
  {ok, SubscriberPid} =
    brod:start_link_topic_subscriber(?CLIENT_ID, ?topic, ConsumerConfig,
                                     ?MODULE, InitArgs),
  ?assertEqual([{Offset2, 333}], RecvFun(SubscriberPid, 5000, [])),
  ok = brod_topic_subscriber:stop(SubscriberPid),
  ok.

t_consumer_crash(Config) when is_list(Config) ->
  ConsumerConfig = [ {prefetch_count, 10}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   , {partition_restart_delay_seconds, 1}
                   ],
  CaseRef = t_consumer_crash,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true},
  Partition = 0,
  {ok, SubscriberPid} =
    brod:start_link_topic_subscriber(?CLIENT_ID, ?topic, ConsumerConfig,
                                     ?MODULE, InitArgs),
  SendFun =
    fun(I) ->
        produce({?topic, Partition}, <<I>>)
    end,
  ReceiveFun =
    fun F(MaxI, Acc) ->
        receive
          {CaseRef, Partition, Offset, <<MaxI>>} ->
            lists:unzip(lists:reverse([{Offset, MaxI} | Acc]));
          {CaseRef, Partition, Offset, <<I>>} when I < MaxI ->
            F(MaxI, [{Offset, I} | Acc]);
          Msg ->
            ct:fail("Unexpected msg: ~p", [Msg])
        after 5000 ->
            lists:unzip(lists:reverse(Acc))
        end
    end,
  SendFun(0),
  %% the first message may or may not be received depending on when
  %% the consumer starts polling
  ReceiveFun(0, []),
  %% send and receive some messages, ack some of them
  [SendFun(I) || I <- lists:seq(1, 5)],
  {[_, _, O3, _, O5], [1, 2, 3, 4, 5]} = ReceiveFun(5, []),
  ok = brod_topic_subscriber:ack(SubscriberPid, Partition, O3),
  %% do a sync request to the subscriber, so that we know it has
  %% processed the ack, then kill the brod_consumer process
  sys:get_state(SubscriberPid),
  {ok, ConsumerPid} = brod:get_consumer(?CLIENT_ID, ?topic, Partition),
  Mon = monitor(process, ConsumerPid),
  exit(ConsumerPid, kill),
  receive {'DOWN', Mon, process, ConsumerPid, killed} -> ok
  after 1000 -> ct:fail("timed out waiting for the consumer process to die")
  end,
  %% ack all previously received messages
  %% so topic subscriber can re-subscribe to the restarted consumer
  ok = brod_topic_subscriber:ack(SubscriberPid, Partition, O5),
  %% send and receive some more messages, check each message arrives only once
  [SendFun(I) || I <- lists:seq(6, 8)],
  {_, [6, 7, 8]} = ReceiveFun(8, []),
  %% stop the subscriber and check there are no more late messages delivered
  ok = brod_topic_subscriber:stop(SubscriberPid),
  receive
    {CaseRef, Partition, Offset, Value} ->
      ct:fail("Unexpected msg: offset ~p, value ~p", [Offset, Value])
  after 0 -> ok
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
