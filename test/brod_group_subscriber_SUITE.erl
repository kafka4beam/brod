%%%
%%%   Copyright (c) 2015-2018 Klarna Bank AB (publ)
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
        , t_async_commit/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod.hrl").

-define(CLIENT_ID, ?MODULE).
-define(TOPIC1, <<"brod-group-subscriber-1">>).
-define(TOPIC2, <<"brod-group-subscriber-2">>).
-define(TOPIC3, <<"brod-group-subscriber-3">>).
-define(TOPIC4, <<"brod-group-subscriber-4">>).
-define(GROUP_ID, list_to_binary(atom_to_list(?MODULE))).
-define(config(Name), proplists:get_value(Name, Config)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  ClientId       = ?CLIENT_ID,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig   = client_config(),
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, ?TOPIC1, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC2, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC3, _ProducerConfig = []),
  ok = brod:start_producer(ClientId, ?TOPIC4, _ProducerConfig = []),
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
               , is_async_commit
               }).

-define(MSG(Ref, Pid, Topic, Partition, Offset, Value),
        {Ref, Pid, Topic, Partition, Offset, Value}).

init(_GroupId, {CaseRef, CasePid, IsAsyncAck, IsAsyncCommit}) ->
  {ok, #state{ ct_case_ref     = CaseRef
             , ct_case_pid     = CasePid
             , is_async_ack    = IsAsyncAck
             , is_async_commit = IsAsyncCommit
             }}.

handle_message(Topic, Partition, Message, #state{ ct_case_ref     = Ref
                                                , ct_case_pid     = Pid
                                                , is_async_ack    = IsAsyncAck
                                                , is_async_commit = IsAsyncCommit
                                                } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  %% forward the message to ct case for verification.
  Pid ! ?MSG(Ref, self(), Topic, Partition, Offset, Value),
  case {IsAsyncAck, IsAsyncCommit} of
    {true, _}      -> {ok, State};
    {false, false} -> {ok, ack, State};
    {false, true}  -> {ok, ack_no_commit, State}
  end.

get_committed_offsets(_GroupId, _TopicPartitions, State) ->
  %% always return []: always fetch from latest available offset
  {ok, [], State}.

%%%_* Test functions ===========================================================

t_loc_demo(Config) when is_list(Config) ->
  CgId = iolist_to_binary("t_loc_demo-" ++
                          integer_to_list(erlang:system_time())),
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_loc:bootstrap(1, message, CgId),
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
  CgId = iolist_to_binary("t_loc_demo_message_set-" ++
                          integer_to_list(erlang:system_time())),
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_group_subscriber_loc:bootstrap(1, message_set, CgId),
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
                   , {prefetch_bytes, 0}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 100}
                   ],
  CaseRef = t_async_acks,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = true, _IsAsyncCommit = false},
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
  meck:new(brod, [passthrough, no_passthrough_cover, no_history]),
  meck:expect(brod, subscribe,
              fun(Client, Pid, Topic, Partition, Opts) ->
                  {ok, ConsumerPid} = meck:passthrough([Client, Pid, Topic,
                                                       Partition, Opts]),
                  CasePid ! {subscribed, {Topic, Partition}, ConsumerPid},
                  {ok, ConsumerPid}
              end),
  meck:expect(brod, unsubscribe,
              fun(ConsumerPid, SubscriberPid) ->
                  meck:passthrough([ConsumerPid, SubscriberPid]),
                  CasePid ! {unsubscribed, ConsumerPid},
                  ok
              end),
  Config;
t_2_members_subscribe_to_different_topics({'end', Config}) when is_list(Config) ->
  meck:unload(brod);
t_2_members_subscribe_to_different_topics(Config) when is_list(Config) ->
  MaxSeqNo = 100,
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  GroupConfig = [{offset_commit_policy, consumer_managed}],
  ConsumerConfig = [ {prefetch_count, MaxSeqNo}
                   , {prefetch_bytes, 0}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 100}
                   ],
  CaseRef = t_2_members_subscribe_to_different_topics,
  CasePid = self(),
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = false, _IsAsyncCommit = false},
  {ok, SubscriberPid1} =
    brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, [?TOPIC2],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE, InitArgs),
  {ok, SubscriberPid2} =
    brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, [?TOPIC3],
                                     GroupConfig, ConsumerConfig,
                                     ?MODULE, InitArgs),
  ok = wait_for_subscribers([?TOPIC2, ?TOPIC3]),
  Partitioner = fun(_Topic, PartitionCnt, _Key, _Value) ->
                    {ok, rand_uniform(PartitionCnt)}
                end,
  SendFun =
    fun(I) ->
      Value = integer_to_binary(I),
      Topic =
        case rand_uniform(2) of
          0 -> ?TOPIC2;
          1 -> ?TOPIC3
        end,
      ok = brod:produce_sync(?CLIENT_ID, Topic, Partitioner, <<>>, Value)
    end,
  RecvFun =
    fun Continue(Acc) when length(Acc) =:= MaxSeqNo -> lists:sort(Acc);
        Continue(Acc) ->
          receive
            ?MSG(CaseRef, SubscriberPid, Topic, _Partition, _Offset, Value) ->
            %% assert subscribers assigned with only topics in subscription list
            ?assert((SubscriberPid =:= SubscriberPid1 andalso Topic =:= ?TOPIC2)
                    orelse
                    (SubscriberPid =:= SubscriberPid2 andalso Topic =:= ?TOPIC3)
                   ),
            Continue([binary_to_integer(Value) | Acc]);
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
  ?assertEqual(L, RecvFun([])),
  ok = brod_group_subscriber:stop(SubscriberPid1),
  ok = brod_group_subscriber:stop(SubscriberPid2),
  ok.

t_async_commit({init, Config}) ->
  meck:new(brod, [passthrough, no_passthrough_cover, no_history]),
  CasePid = self(),
  meck:expect(brod, subscribe,
              fun(Client, Pid, Topic, Partition, Opts) ->
                  {ok, ConsumerPid} = meck:passthrough([Client, Pid, Topic,
                                                       Partition, Opts]),
                  CasePid ! {subscribed, {Topic, Partition}, ConsumerPid},
                  {ok, ConsumerPid}
              end),
  meck:new(brod_group_coordinator, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_async_commit({'end', _Config}) ->
  meck:unload(brod);
t_async_commit(Config) when is_list(Config) ->
  CaseRef = t_async_commit,
  CasePid = self(),
  Partition = 0,
  InitArgs = {CaseRef, CasePid, _IsAsyncAck = false, _IsAsyncCommit = true},
  StartSubscriber =
    fun() ->
        GroupConfig = [],
        ConsumerConfig = [ {sleep_timeout, 0}
                         , {begin_offset, latest}
                         , {prefetch_bytes, 0}
                         , {sleep_timeout, 0}
                         , {max_wait_time, 100}
                         ],
        {ok, SubscriberPid} =
          brod:start_link_group_subscriber(?CLIENT_ID, ?GROUP_ID, [?TOPIC4],
                                           GroupConfig, ConsumerConfig,
                                           ?MODULE, InitArgs),
        wait_for_subscribers([?TOPIC4]),
        SubscriberPid
    end,
  EmulateRestart =
    fun(Pid) ->
        ct:pal("Stopping consumer for ~p", [?TOPIC4]),
        brod_group_subscriber:stop(Pid),
        StartSubscriber()
    end,
  CommitOffset =
    fun(Pid, Offset) ->
        ct:pal("Acking offset = ~p", [Offset]),
        ok = brod_group_subscriber:commit(Pid, ?TOPIC4, 0, Offset),
        timer:sleep(5500)
    end,
  Pid1 = StartSubscriber(),
  {ok, Offset} = brod:produce_sync_offset(?CLIENT_ID, ?TOPIC4, Partition, <<>>, <<"test">>),
  ct:pal("Produced at offset = ~p", [Offset]),
  ?assertEqual([[Offset]],
               receive_match(4000, ?MSG(CaseRef, '_', ?TOPIC4, Partition, '$1', '_'))
              ),
  %% Slightly unsound: commit _previous_ offset to avoid starting
  %% brod_consumer with `latest' offset and thus losing all data
  %% during restart:
  CommitOffset(Pid1, Offset - 1),
  %% Emulate subscriber restart:
  Pid2 = EmulateRestart(Pid1),
  %% Since we haven't commited offset, our message should be replayed:
  ?assertEqual([[Offset]],
               receive_match(4000, ?MSG(CaseRef, '_', ?TOPIC4, Partition, '$1', '_'))
              ),
  %% Commit offset and restart subscriber again:
  CommitOffset(Pid2, Offset),
  Pid3 = EmulateRestart(Pid2),
  %% This time we shouldn't receive anything:
  ?assertEqual([],
               receive_match(4000, ?MSG(CaseRef, '_', ?TOPIC4, 0, '$1', '_'))
              ),
  brod_group_subscriber:stop(Pid3),
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
wait_for_subscribers(Topics) ->
  PerTopic =
    fun(Topic) ->
        {ok, Count} = brod_client:get_partitions_count(?CLIENT_ID, Topic),
        lists:map(fun(Partition) ->
                      {{Topic, Partition}, _ConsumerPid = undefined}
                  end, lists:seq(0, Count - 1))
    end,
  States = lists:flatten(lists:map(PerTopic, Topics)),
  do_wait_for_subscribers(States).

do_wait_for_subscribers(States) ->
  case lists:all(fun({_, Pid}) -> is_pid(Pid) end, States) of
    true -> ok;
    false ->
      receive
        {subscribed, TP, ConsumerPid} ->
          NewStates = lists:keystore(TP, 1, States, {TP, ConsumerPid}),
          do_wait_for_subscribers(NewStates);
        {unsubscribed, ConsumerPid} ->
          {TP, ConsumerPid} = lists:keyfind(ConsumerPid, 2, States),
          NewStates = lists:keystore(TP, 1, States, {TP, undefined}),
          do_wait_for_subscribers(NewStates)
        after
          10000 ->
            erlang:error({timeout, States})
      end
  end.

rand_uniform(Max) ->
  {_, _, Micro} = os:timestamp(),
  Micro rem Max.

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

receive_match(Timeout, MatchSpec) ->
  MS = ets:match_spec_compile([{MatchSpec, [], ['$$']}]),
  Messages = receive_all(Timeout),
  ets:match_spec_run(Messages, MS).

receive_all(Timeout) ->
  lists:reverse(receive_all([], Timeout)).
receive_all(Msgs, Timeout) ->
  receive
    A ->
      ct:pal("Received ~p", [A]),
      receive_all([A|Msgs], Timeout)
  after Timeout ->
      Msgs
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
