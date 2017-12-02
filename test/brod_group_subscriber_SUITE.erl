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

%% @private
-module(brod_group_subscriber_SUITE).

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
        , get_committed_offsets/3
        , handle_message/4
        , rand_uniform/1
        ]).

%% Test cases
-export([ t_async_acks/1
        , t_koc_demo/1
        , t_koc_demo_message_set/1
        , t_loc_demo/1
        , t_loc_demo_message_set/1
        , t_2_members_subscribe_to_different_topics/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod.hrl").

-define(CLIENT_ID, ?MODULE).
-define(TOPIC1, <<"brod-group-subscriber-1">>).
-define(TOPIC2, <<"brod-group-subscriber-2">>).
-define(TOPIC3, <<"brod-group-subscriber-3">>).
-define(GROUP_ID, list_to_binary(atom_to_list(?MODULE))).
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
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, ?TOPIC1, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC2, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC3, _ProducerConfig = []),
  try
    ?MODULE:Case({init, Config})
  catch
    error : function_clause ->
      Config
  end.

end_per_testcase(Case, Config) when is_list(Config) ->
  ok = brod:stop_client(?CLIENT_ID),
  try
    ?MODULE:Case({'end', Config})
  catch
    error : function_clause ->
      ok
  end,
  ct:pal("=== ~p end ===", [Case]),
  ok.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Group subscriber callbacks ===============================================

-record(state, { ct_case_ref
               , ct_case_pid
               , is_async_ack
               }).

-define(MSG(Ref, Pid, Topic, Partition, Offset, Value),
        {Ref, Pid, Topic, Partition, Offset, Value}).

init(_GroupId, {CaseRef, CasePid, IsAsyncAck}) ->
  {ok, #state{ ct_case_ref  = CaseRef
             , ct_case_pid  = CasePid
             , is_async_ack = IsAsyncAck
             }}.

handle_message(Topic, Partition, Message, #state{ ct_case_ref  = Ref
                                                , ct_case_pid  = Pid
                                                , is_async_ack = IsAsyncAck
                                                } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  %% forward the message to ct case for verification.
  Pid ! ?MSG(Ref, self(), Topic, Partition, Offset, Value),
  case IsAsyncAck of
    true  -> {ok, State};
    false -> {ok, ack, State}
  end.

get_committed_offsets(_GroupId, _TopicPartitions, State) ->
  %% always return []: always fetch from latest available offset
  {ok, [], State}.

%%%_* Test functions ===========================================================

t_loc_demo(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_loc:bootstrap(1),
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

t_loc_demo_message_set(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_loc:bootstrap(1, message_set),
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

t_koc_demo(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_koc:bootstrap(1),
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

t_koc_demo_message_set(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_koc:bootstrap(1, message_set),
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

t_async_acks({init, Config}) when is_list(Config) ->
  PARTITION = 0,
  CasePid = self(),
  meck:new(brod, [passthrough, no_passthrough_cover, no_history]),
  meck:expect(brod, subscribe,
              fun(Client, Pid, Topic, Partition, Opts) ->
                  Result = meck:passthrough([Client, Pid, Topic,
                                             Partition, Opts]),
                  case Partition =:= PARTITION of
                    true -> CasePid ! subscribed;
                    false -> ok
                  end,
                  Result
              end),
  [{partition, PARTITION} | Config];
t_async_acks({'end', Config}) when is_list(Config) ->
  meck:unload(brod);
t_async_acks(Config) when is_list(Config) ->
  MaxSeqNo = 100,
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  GroupConfig = [{offset_commit_policy, consumer_managed}],
  ConsumerConfig = [ {prefetch_count, MaxSeqNo}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 100}
                   ],
  CaseRef = t_async_acks,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true},
  Partition = ?config(partition),
  {ok, SubscriberPid} =
    brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, [?TOPIC1],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE, InitArgs),
  SendFun =
    fun(I) ->
      Value = list_to_binary(integer_to_list(I)),
      ok = brod:produce_sync(?CLIENT_ID, ?TOPIC1, Partition, <<>>, Value)
    end,
  Timeout = 4000,
  RecvFun =
    fun Continue(Acc) ->
      receive
        ?MSG(CaseRef, SubscriberPid, ?TOPIC1, Partition, Offset, Value) ->
          ok = brod_group_subscriber:ack(SubscriberPid, ?TOPIC1,
                                         Partition, Offset),
          ok = brod_group_subscriber:commit(SubscriberPid),
          I = binary_to_integer(Value),
          case I =:= MaxSeqNo of
            true -> lists:reverse([I | Acc]);
            false -> Continue([I | Acc])
          end;
        Msg ->
          erlang:error({unexpected_msg, Msg})
      after
        Timeout ->
          erlang:error({timeout, Acc})
      end
    end,
  %% Make sure subscriber is ready before sending messages
  receive subscribed -> ok
  after 4000 -> erlang:error(<<"timeout waiting for subscriber">>) end,
  L = lists:seq(1, MaxSeqNo),
  ok = lists:foreach(SendFun, L),
  ?assertEqual(L, RecvFun([])),
  ok = brod_group_subscriber:stop(SubscriberPid),
  ok.

t_2_members_subscribe_to_different_topics({init, Config}) ->
  CasePid = self(),
  Ref = make_ref(),
  meck:new(brod, [passthrough, no_passthrough_cover, no_history]),
  meck:expect(brod, subscribe,
              fun(Client, Pid, Topic, Partition, Opts) ->
                  {ok, ConsumerPid} = meck:passthrough([Client, Pid, Topic,
                                                       Partition, Opts]),
                  CasePid ! {subscribed, Ref, {Topic, Partition}, Pid},
                  {ok, ConsumerPid}
              end),
  meck:expect(brod, unsubscribe,
              fun(ConsumerPid, SubscriberPid) ->
                  meck:passthrough([ConsumerPid, SubscriberPid]),
                  CasePid ! {unsubscribed, Ref, ConsumerPid},
                  ok
              end),
  [{ref, Ref} | Config];
t_2_members_subscribe_to_different_topics({'end', Config}) when is_list(Config) ->
  meck:unload(brod);
t_2_members_subscribe_to_different_topics(Config) when is_list(Config) ->
  MaxSeqNo = 100,
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  GroupConfig = [{offset_commit_policy, consumer_managed}],
  ConsumerConfig = [ {prefetch_count, MaxSeqNo}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 100}
                   ],
  CaseRef = t_2_members_subscribe_to_different_topics,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = false},
  {ok, SubscriberPid1} =
    brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, [?TOPIC2],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE, InitArgs),
  {ok, SubscriberPid2} =
    brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, [?TOPIC3],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE, InitArgs),
  Ref = ?config(ref),
  ok = wait_for_subscribers(Ref, [?TOPIC2, ?TOPIC3]),
  Partitioner = fun(_Topic, PartitionCnt, _Key, _Value) ->
                    {ok, rand_uniform(PartitionCnt)}
                end,
  SendFun =
    fun(I) ->
      Value = list_to_binary(integer_to_list(I)),
      Topic =
        case rand_uniform(2) of
          0 -> ?TOPIC2;
          1 -> ?TOPIC3
        end,
      ok = brod:produce_sync(?CLIENT_ID, Topic, Partitioner, <<>>, Value)
    end,
  RecvFun =
    fun Continue(Acc) ->
      receive
        ?MSG(CaseRef, SubscriberPid, Topic, _Partition, _Offset, Value) ->
          %% assert subscribers assigned with only topics in subscription list
          ?assert((SubscriberPid =:= SubscriberPid1 andalso Topic =:= ?TOPIC2)
                  orelse
                  (SubscriberPid =:= SubscriberPid2 andalso Topic =:= ?TOPIC3)),
          I = binary_to_integer(Value),
          case I =:= MaxSeqNo of
            true -> [I | Acc];
            false -> Continue([I | Acc])
          end;
        Msg ->
          erlang:error({unexpected_msg, Msg})
      after
        4000 ->
          erlang:error({timeout, Acc})
      end
    end,
  L = lists:seq(1, MaxSeqNo),
  ok = lists:foreach(SendFun, L),
  %% since the nubmers are produced to different partitions and collected
  %% by different consumers, they have a very good chance to go out of the
  %% original order, hence we do not verify the order here
  ?assertEqual(L, lists:sort(RecvFun([]))),
  ok = brod_group_subscriber:stop(SubscriberPid1),
  ok = brod_group_subscriber:stop(SubscriberPid2),
  ok.

%%%_* Help funtions ============================================================

%% For test deterministc, we wait until all consumer group members are
%% ready to receive messages before start sending messages for tests.
%%
%% brod:subscribe and brod:unsubscribe calls are mocked (with passthrough).
%% the mocked functions send a 'subscribed' (when partitions are assigned)
%% or 'unsubscribed' (when assignments revoked) message to test case runner.
%%
%% This function maintains a list of subscription states,
%% and returns once all topic-partitions reach 'subscribed' state.
wait_for_subscribers(Ref, Topics) ->
  PerTopic =
    fun(Topic) ->
        {ok, Count} = brod_client:get_partitions_count(?CLIENT_ID, Topic),
        lists:map(fun(Partition) ->
                      {{Topic, Partition}, _ConsumerPid = undefined}
                  end, lists:seq(0, Count - 1))
    end,
  States = lists:flatten(lists:map(PerTopic, Topics)),
  do_wait_for_subscribers(Ref, States).

do_wait_for_subscribers(Ref, States) ->
  case lists:all(fun({_, Pid}) -> is_pid(Pid) end, States) of
    true -> ok;
    false ->
      receive
        {subscribed, Ref, TP, ConsumerPid} ->
          NewStates = lists:keystore(TP, 1, States, {TP, ConsumerPid}),
          do_wait_for_subscribers(Ref, NewStates);
        {unsubscribed, Ref, ConsumerPid} ->
          {TP, ConsumerPid} = lists:keyfind(ConsumerPid, 2, States),
          NewStates = lists:keystore(TP, 1, States, {TP, undefined}),
          do_wait_for_subscribers(Ref, NewStates)
        after
          4000 ->
            erlang:error({timeout, States})
      end
  end.

rand_uniform(Max) ->
  {_, _, Micro} = os:timestamp(),
  Micro rem Max.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
