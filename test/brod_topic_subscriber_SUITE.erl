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
%%% @copyright 20150-2016 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_topic_subscriber_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
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
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod.hrl").

-define(CLIENT_ID, ?MODULE).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).
-define(config(Name), proplists:get_value(Name, Config)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  ct:pal("=== ~p begin ===", [Case]),
  ClientId       = ?CLIENT_ID,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig   = [],
  Topic          = ?TOPIC,
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, Topic, _ProducerConfig = []),
  Config.

end_per_testcase(Case, Config) when is_list(Config) ->
  ok = brod:stop_client(?CLIENT_ID),
  ct:pal("=== ~p end ===", [Case]),
  ok.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Topic subscriber callbacks ===============================================

-record(state, { ct_case_ref
               , ct_case_pid
               , is_async_ack
               , my_id
               }).

init(_Topic, {CaseRef, SubscriberId, CasePid, IsAsyncAck}) ->
  State = #state{ ct_case_ref  = CaseRef
                , ct_case_pid  = CasePid
                , is_async_ack = IsAsyncAck
                , my_id        = SubscriberId
                },
  {ok, [], State}.

handle_message(Partition, Message, #state{ ct_case_ref  = Ref
                                         , ct_case_pid  = Pid
                                         , is_async_ack = IsAsyncAck
                                         , my_id        = MyId
                                         } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  %% forward the message to ct case for verification.
  Pid ! {Ref, MyId, Partition, Offset, Value},
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
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   ],
  CaseRef        = t_async_acks,
  CasePid        = self(),
  InitArgs       = {CaseRef, _SubscriberId = 0, CasePid, _IsAsyncAck = true},
  Partition      = 0,
  {ok, SubscriberPid} =
    brod:start_link_topic_subscriber(?CLIENT_ID, ?TOPIC, ConsumerConfig,
                                     ?MODULE, InitArgs),
  SendFun =
    fun(I) ->
      Value = list_to_binary(integer_to_list(I)),
      ok = brod:produce_sync(?CLIENT_ID, ?TOPIC, Partition, <<>>, Value)
    end,
  RecvFun =
    fun(Timeout, {ContinueFun, Acc}) ->
      receive
        {CaseRef, 0, 0, Offset, Value} ->
          ok = brod_topic_subscriber:ack(SubscriberPid, Partition, Offset),
          I = binary_to_list(Value),
          NewAcc = [list_to_integer(I) | Acc],
          ContinueFun(0, {ContinueFun, NewAcc});
        Msg ->
          erlang:error({unexpected_msg, Msg})
      after Timeout ->
        {ContinueFun, Acc}
      end
    end,
  ok = SendFun(0),
  %% wait at most 2 seconds to receive the first message
  %% it may or may not receive the first message (0) depending on when
  %% the consumers starts polling --- before or after the first message
  %% is produced.
  _ = RecvFun(2000, {RecvFun, []}),
  L = lists:seq(1, MaxSeqNo),
  ok = lists:foreach(SendFun, L),
  %% worst case scenario, the receive loop will cost (1000 * 5 + 5 * 1000) ms
  Timeouts = lists:duplicate(MaxSeqNo, 5) ++ lists:duplicate(5, 1000),
  {_, ReceivedL} = lists:foldl(RecvFun, {RecvFun, []}, Timeouts ++ [1,2,3,4,5]),
  ?assertEqual(L, lists:reverse(ReceivedL)),
  ok = brod_topic_subscriber:stop(SubscriberPid),
  ok.

%%%_* Help funtions ============================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
