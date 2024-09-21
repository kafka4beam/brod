%%%
%%%   Copyright (c) 2015-2021, Klarna Bank AB (publ)
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
-module(brod_consumer_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_drop_aborted/1
        , t_wait_for_unstable_offsets/1
        , t_fetch_aborted_from_the_middle/1
        , t_direct_fetch/1
        , t_fold/1
        , t_fold_transactions/1
        , t_direct_fetch_with_small_max_bytes/1
        , t_direct_fetch_expand_max_bytes/1
        , t_resolve_offset/1
        , t_consumer_max_bytes_too_small/1
        , t_consumer_connection_restart_0/1
        , t_consumer_connection_restart_1/1
        , t_consumer_connection_restart_2/1
        , t_consumer_resubscribe/1
        , t_consumer_resubscribe_earliest/1
        , t_subscriber_restart/1
        , t_subscribe_with_unknown_offset/1
        , t_offset_reset_policy/1
        , t_stop_kill/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

-define(WAIT(Pattern, Expr),
        fun() ->
          receive Pattern -> Expr
          after 5000 -> erlang:error({timeout, ??Pattern})
          end
        end()).
-define(WAIT_ONLY(Pattern, Expr),
        fun() ->
            Msg = receive X -> X
                  after 5000 -> erlang:error({timeout, ??Pattern})
                  end,
            case Msg of
              Pattern ->
                Expr;
              _ ->
                ct:pal("exp: ~s\ngot: ~p\n", [??Pattern, Msg]),
                erlang:error({unexpected_msg, Msg})
            end
        end()).

-define(RETRY(EXPR, Timeout),
        retry(
          fun() ->
              case EXPR of
                {ok, R} ->
                  {ok, R};
                {error, _} ->
                  false
              end
          end, Timeout)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config0) ->
  ct:pal("=== ~p begin ===", [Case]),
  Client = Case,
  Topic = ?TOPIC,
  CooldownSecs = 1,
  ProducerRestartDelay = 1,
  ClientConfig0 = [{reconnect_cool_down_seconds, CooldownSecs}],
  ProducerConfig = [ {partition_restart_delay_seconds, ProducerRestartDelay}
                   , {max_retries, 0}],
  Config =
    try
      ?MODULE:Case({init, Config0})
    catch
      error : function_clause ->
        Config0
    end,
  ClientConfig1 = proplists:get_value(client_config, Config, []),
  brod:stop_client(Client),
  ClientConfig = kafka_test_helper:client_config() ++ ClientConfig0 ++ ClientConfig1,
  ok = brod:start_client(?HOSTS, Client, ClientConfig),
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  try ?MODULE:Case(standalone_consumer) of
    true ->
      ok
  catch
    error : function_clause ->
      ConsumerCfg = consumer_config(),
      ok = brod:start_consumer(Client, Topic, ConsumerCfg)
  end,
  [{client, Client}, {client_config, ClientConfig} | Config].

end_per_testcase(Case, Config) ->
  try
    ?MODULE:Case({'end', Config})
  catch
    error : function_clause ->
      ok
  end,
  ct:pal("=== ~p end ===", [Case]),
  Client = ?config(client),
  try
    Ref = erlang:monitor(process, whereis(Client)),
    brod:stop_client(Client),
    receive
      {'DOWN', Ref, process, _Pid, _} -> ok
    end
  catch _ : _ ->
    ok
  end.

all() ->
  Cases = [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end],
  Filter = fun(Case) ->
               try
                 ?MODULE:Case(kafka_version_match)
               catch
                 _:_ ->
                   true
               end
           end,
  lists:filter(Filter, Cases).

%%%_* Test functions ===========================================================

%% produce two transactions, abort the first and commit the second
%% messages fetched back should only contain the committed message
%% i.e. aborted messages (testing with isolation_level=read_committed)
%% should be dropped, control messages (transaction abort) should be dropped
t_drop_aborted(kafka_version_match) ->
  has_txn();
t_drop_aborted(Config) when is_list(Config) ->
  test_drop_aborted(Config, true),
  test_drop_aborted(Config, false).

%% When QueryApiVsn is set to false,
%% brod will use lowest supported API version.
%% This is to test fethcing transactional messages using old version API
test_drop_aborted(Config, QueryApiVsn) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  TxnProduceFun =
    fun(CommitOrAbort) ->
        TxnId = make_transactional_id(),
        {ok, Conn} = connect_txn_coordinator(TxnId, ?config(client_config)),
        {ok, TxnCtx} = ?RETRY(kpro:txn_init_ctx(Conn, TxnId), 10),
        ok = kpro:txn_send_partitions(TxnCtx, [{Topic, Partition}]),
        Key = bin([atom_to_list(CommitOrAbort), "-", make_unique_key()]),
        Vsn = 3, %% lowest API version which supports transactional produce
        Opts = #{txn_ctx => TxnCtx, first_sequence => 0},
        Batch = [#{key => Key, value => <<>>}],
        ProduceReq = kpro_req_lib:produce(Vsn, Topic, Partition, Batch, Opts),
        {ok, LeaderConn} =
          brod_client:get_leader_connection(Client, Topic, Partition),
        {ok, ProduceRsp} = kpro:request_sync(LeaderConn, ProduceReq, 5000),
        {ok, Offset} = brod_utils:parse_rsp(ProduceRsp),
        case CommitOrAbort of
          commit -> ok = kpro:txn_commit(TxnCtx);
          abort -> ok = kpro:txn_abort(TxnCtx)
        end,
        ok = kpro:close_connection(Conn),
        {Offset, Key}
    end,
  {Offset1, Key1} = TxnProduceFun(abort),
  {Offset2, Key2} = TxnProduceFun(commit),
  Cfg = [{query_api_versions, QueryApiVsn} | ?config(client_config)],
  Fetch =
    fun(Offset, Opts) ->
        {ok, {_HighWmOffset, Msgs}} =
          brod:fetch({?HOSTS, Cfg}, Topic, Partition, Offset, Opts),
          Msgs
    end,
  case QueryApiVsn of
    true ->
      %% Always expect only one message in fetch result: the committed
      [Msg] = Fetch(Offset1, #{}),
      [Msg] = Fetch(Offset1, #{min_bytes => 0, max_bytes => 1}),
      [Msg] = Fetch(Offset1, #{min_bytes => 0, max_bytes => 12}),
      [Msg] = Fetch(Offset1 + 1, #{}),
      [Msg] = Fetch(Offset2, #{}),
      ?assertMatch(#kafka_message{offset = Offset2, key = Key2}, Msg);
    false ->
      %% Kafka sends aborted batches to old version clients!
      Msgs = Fetch(Offset1, #{max_bytes => 1000}),
      ?assertMatch([#kafka_message{offset = Offset1, key = Key1},
                    #kafka_message{offset = Offset2, key = Key2}
                   ], Msgs)
  end.

t_wait_for_unstable_offsets(kafka_version_match) ->
  has_txn();
t_wait_for_unstable_offsets(Config) when is_list(Config) ->
  t_wait_for_unstable_offsets({run, Config});
t_wait_for_unstable_offsets({run, Config}) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  TxnId = make_transactional_id(),
  {ok, Conn} = connect_txn_coordinator(TxnId, ?config(client_config)),
  %% ensure we have enough time to test before expire
  TxnOpts = #{txn_timeout => timer:seconds(30)},
  {ok, TxnCtx} = ?RETRY(kpro:txn_init_ctx(Conn, TxnId, TxnOpts), 10),
  ok = kpro:txn_send_partitions(TxnCtx, [{Topic, Partition}]),
  %% Send one message in this transaction, return the offset in kafka
  ProduceFun =
    fun(Seqno, Msg) ->
      Vsn = 3, %% lowest API version which supports transactional produce
      Opts = #{txn_ctx => TxnCtx, first_sequence => Seqno},
      Batch = [#{value => Msg}],
      ProduceReq = kpro_req_lib:produce(Vsn, Topic, Partition, Batch, Opts),
      {ok, LeaderConn} =
          brod_client:get_leader_connection(Client, Topic, Partition),
      {ok, ProduceRsp} = kpro:request_sync(LeaderConn, ProduceReq, 5000),
      {ok, Offset} = brod_utils:parse_rsp(ProduceRsp),
      Offset
    end,
  Seqnos = lists:seq(0, 100),
  Msgs = [{Seqno, iolist_to_binary(make_ts_str())} || Seqno <- Seqnos],
  Offsets = [{Seqno, ProduceFun(Seqno, Msg)} || {Seqno, Msg} <- Msgs],
  {_, BaseOffset} = hd(Offsets),
  {_, LastOffset} = lists:last(Offsets),
  Cfg = ?config(client_config),
  Fetch = fun(O) ->
              {ok, {StableOffset, MsgL}} =
                brod:fetch({?HOSTS, Cfg}, Topic, Partition, O,
                           #{max_wait_time => 100}),
              {StableOffset, MsgL}
          end,
  %% Transaction is not committed yet, fetch should not see anything
  ?assertMatch({BaseOffset, []}, Fetch(BaseOffset)),
  ?assertMatch({BaseOffset, []}, Fetch(BaseOffset + 1)),
  ?assertMatch({BaseOffset, []}, Fetch(LastOffset)),
  ?assertMatch({BaseOffset, []}, Fetch(LastOffset + 1)),
  ok = kpro:txn_commit(TxnCtx),
  ok = kpro:close_connection(Conn),
  %% A commit batch is appended behind last message
  %% commit batch is empty but takes one offset, hence + 2
  StableOffset = LastOffset + 2,
  {FetchedStableOffset, [FetchedFirstMsg | _]} = Fetch(BaseOffset),
  {_, ExpectedMsg} = hd(Msgs),
  ?assertMatch(#kafka_message{value = ExpectedMsg}, FetchedFirstMsg),
  ?assertEqual(StableOffset, FetchedStableOffset),
  ?assertMatch({StableOffset, [_ | _]}, Fetch(BaseOffset + 1)),
  ?assertMatch({StableOffset, []}, Fetch(StableOffset)),
  ok.

%% Produce large(-ish) transactional batches, then abort them all
%% try fetch from offsets in the middle of large batches,
%% expect no delivery of any aborted batches.
t_fetch_aborted_from_the_middle(kafka_version_match) ->
  has_txn();
t_fetch_aborted_from_the_middle(Config) when is_list(Config) ->
  test_fetch_aborted_from_the_middle(Config).

test_fetch_aborted_from_the_middle(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  TxnId = make_transactional_id(),
  {ok, Conn} = connect_txn_coordinator(TxnId, ?config(client_config)),
  {ok, TxnCtx} = ?RETRY(kpro:txn_init_ctx(Conn, TxnId), 10),
  ok = kpro:txn_send_partitions(TxnCtx, [{Topic, Partition}]),
  %% make a large-ish message
  MkMsg = fun(Key) ->
              #{ key => Key
               , value => bin(lists:duplicate(10000, 0))
               }
          end,
  %% produce a non-transactional message
  Send =
    fun() ->
        Key = make_unique_key(),
        {ok, Offset} =
          brod:produce_sync_offset(Client, Topic, Partition, Key, <<>>),
        {Offset, Key}
    end,
  %% Send a transactional batch and return base offset
  SendTxn =
    fun(N) ->
        Keys = [make_unique_key() || _ <- lists:duplicate(N, 0)],
        Batch = [MkMsg(Key) || Key <- Keys],
        Seqno = case get({seqno, TxnId}) of
                  undefined -> 0;
                  S -> S
                end,
        put({seqno, TxnId}, Seqno + length(Batch)),
        Opts = #{txn_ctx => TxnCtx, first_sequence => Seqno},
        Vsn = 3, %% lowest API version which supports transactional produce
        Req = kpro_req_lib:produce(Vsn, Topic, Partition, Batch, Opts),
        {ok, Leader} =
          brod_client:get_leader_connection(Client, Topic, Partition),
        {ok, Rsp} = kpro:request_sync(Leader, Req, 5000),
        {ok, Offset} = brod_utils:parse_rsp(Rsp),
        {Offset, Keys}
    end,
  Cfg = ?config(client_config),
  Fetch =
    fun(Offset, Opts) ->
        {ok, {_HighWmOffset, Msgs}} =
          brod:fetch({?HOSTS, Cfg}, Topic, Partition, Offset, Opts),
        lists:map(fun(#kafka_message{key = Key, offset = OffsetX}) ->
                      {OffsetX, Key}
                  end, Msgs)
    end,
  {OffsetBgn, KeyBgn} = Send(), %% the begin mark
  {Offset1, _Keys1} = SendTxn(10),
  {Offset2, _Keys2} = SendTxn(20),
  ok = kpro:txn_abort(TxnCtx),
  ok = kpro:close_connection(Conn),
  {OffsetEnd, KeyEnd} = Send(), %% the end mark
  ?assertEqual([{OffsetBgn, KeyBgn}], Fetch(OffsetBgn, #{max_bytes => 12})),
  ?assertEqual([{OffsetEnd, KeyEnd}], Fetch(OffsetEnd, #{max_bytes => 100000})),
  ?assertEqual([{OffsetEnd, KeyEnd}], Fetch(Offset1, #{max_bytes => 12})),
  ?assertEqual([{OffsetEnd, KeyEnd}], Fetch(Offset1 + 1, #{max_bytes => 12})),
  ?assertEqual([{OffsetEnd, KeyEnd}], Fetch(Offset2, #{max_bytes => 12})),
  ok.

t_direct_fetch(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key = make_unique_key(),
  Value = <<>>,
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  {ok, Offset} = brod:resolve_offset(?HOSTS, Topic, Partition,
                                     ?OFFSET_LATEST, ?config(client_config)),
  {ok, {_, [Msg]}} = brod:fetch({?HOSTS, ?config(client_config)},
                                 Topic, Partition, Offset - 1),
  {ok, {_, [Msg]}} = brod:fetch(Client, Topic, Partition, Offset - 1),
  ?assertEqual(Key, Msg#kafka_message.key),
  ok.

t_resolve_offset(Config) when is_list(Config) ->
  % Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  {ok, Offset} = brod:resolve_offset(?HOSTS, Topic, Partition,
                                     ?OFFSET_LATEST, ?config(client_config),
                                     #{timeout => timer:seconds(45)}),
  ?assert(is_integer(Offset)).

t_fold(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Nums = lists:seq(1, 100),
  Batch = [#{value => integer_to_binary(I)} || I <- Nums],
  {ok, Offset} =
    brod:produce_sync_offset(Client, ?TOPIC, Partition, <<>>, Batch),
  FoldF =
    fun F(#kafka_message{value = V}, Acc) -> {ok, F(binary_to_integer(V), Acc)};
        F(I, Acc) -> I + Acc
    end,
  Sum = lists:foldl(FoldF, 0, Nums),
  FetchOpts = #{max_bytes => 12},
  ?assertMatch({Sum, O, reached_end_of_partition}
                 when O =:= Offset + length(Nums),
    brod:fold(Client, Topic, Partition, Offset, FetchOpts, 0, FoldF, #{})),
  ?assertMatch({10, O, reached_message_count_limit}
                when O =:= Offset + 4,
    brod:fold(Client, Topic, Partition, Offset, FetchOpts,
              0, FoldF, #{message_count => 4})),
  ?assertMatch({SumX, O, reached_end_of_partition}
                when O =:= Offset + length(Nums) andalso SumX =:= Sum - 10,
    brod:fold(Client, Topic, Partition, Offset + 4, FetchOpts, 0, FoldF, #{})),
  ?assertMatch({10, _, reached_target_offset},
    brod:fold({?HOSTS, ?config(client_config)}, Topic, Partition, Offset,
              FetchOpts, 0, FoldF, #{reach_offset => Offset + 3})),
  ErrorFoldF =
    fun(#kafka_message{value = <<"5">>}, _) -> {error, <<"stop now">>};
       (#kafka_message{value = V}, Acc) -> {ok, binary_to_integer(V) + Acc}
    end,
  ?assertMatch({10, _, <<"stop now">>},
    brod:fold(Client, Topic, Partition, Offset, #{},
              0, ErrorFoldF, #{})),
  BadOffset = 1 bsl 63 - 1,
  ?assertMatch({0, BadOffset, {fetch_failure, offset_out_of_range}},
    brod:fold(Client, Topic, Partition, BadOffset, #{},
              0, ErrorFoldF, #{})),
  ok.

t_fold_transactions(kafka_version_match) ->
  has_txn();
t_fold_transactions(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Batch = [#{value => <<"one">>}, #{value => <<"two">>}],
  {ok, Tx} = brod:transaction(Client, <<"some_transaction">>, []),
  {ok, Offset} = brod:txn_produce(Tx, ?TOPIC, Partition, Batch),
  ok = brod:commit(Tx),
  FoldF =
    fun F(#kafka_message{value = V}, Acc) -> {ok, F(V, Acc)};
        F(V, Acc) -> [V | Acc]
    end,
  FetchOpts = #{max_bytes => 1},
  ?assertMatch({Result, O, reached_end_of_partition}
                when O =:= Offset + length(Batch) + 1 andalso length(Result) =:= 2,
    brod:fold(Client, Topic, Partition, Offset, FetchOpts, [], FoldF, #{})),
  ok.

%% This test case does not work with Kafka 0.9, not sure aobut 0.10 and 0.11
%% since all 0.x versions are old enough, we only try to verify this against
%% 1.x or newer
t_direct_fetch_with_small_max_bytes(kafka_version_match) ->
  {Major, _Minor} = kafka_test_helper:kafka_version(),
  Major > 1;
t_direct_fetch_with_small_max_bytes(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key = make_unique_key(),
  Value = crypto:strong_rand_bytes(100),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  {ok, Offset} = brod:resolve_offset(?HOSTS, Topic, Partition,
                                     ?OFFSET_LATEST, ?config(client_config)),
  {ok, {_, [Msg]}} =
    brod:fetch({?HOSTS, ?config(client_config)},
               Topic, Partition, Offset - 1, #{max_bytes => 1}),
  ?assertEqual(Key, Msg#kafka_message.key),
  ok.

%% Starting from version 3, Kafka no longer returns incomplete batch
%% for Fetch request v0, cannot test max_bytes expansion anymore.
t_direct_fetch_expand_max_bytes(kafka_version_match) ->
  {Major, _Minor} = kafka_test_helper:kafka_version(),
  Major < 3;
t_direct_fetch_expand_max_bytes({init, Config}) when is_list(Config) ->
  %% kafka returns empty message set when it's 0.9
  %% or when fetch request sent was version 0
  %% Avoid querying api version will make brod send v0 requests
  [{client_config, [{query_api_versions, false}]} | Config];
t_direct_fetch_expand_max_bytes(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key = make_unique_key(),
  Value = crypto:strong_rand_bytes(100),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  {ok, Offset} = brod:resolve_offset(?HOSTS, Topic, Partition,
                                    ?OFFSET_LATEST, ?config(client_config)),
  {ok, {_, [Msg]}} = brod:fetch({?HOSTS, ?config(client_config)},
                                Topic, Partition, Offset - 1,
                                #{max_bytes => 13}),
  ?assertEqual(Key, Msg#kafka_message.key),
  ok.

%% @doc Consumer should be smart enough to try greater max_bytes
%% when it's not great enough to fetch one single message
t_consumer_max_bytes_too_small(kafka_version_match) ->
  {Major, _Minor} = kafka_test_helper:kafka_version(),
  Major < 3;
t_consumer_max_bytes_too_small({init, Config}) ->
  meck:new(brod_kafka_request, [passthrough, no_passthrough_cover, no_history]),
  %% kafka returns empty message set when it's 0.9
  %% or when fetch request sent was version 0
  %% Avoid querying api version will make brod send v0 requests
  [{client_config, [{query_api_versions, false}]} | Config];
t_consumer_max_bytes_too_small({'end', _Config}) ->
  meck:unload(brod_kafka_request);
t_consumer_max_bytes_too_small(Config) ->
  Client = ?config(client),
  Partition = 0,
  brod:unsubscribe(Client, ?TOPIC, Partition),
  Key = make_unique_key(),
  ValueBytes = 2000,
  MaxBytes1 = 12, %% too small for even the header
  MaxBytes2 = size(Key) + ValueBytes,
  Tester = self(),
  F = fun(Conn, Topic, Partition1, BeginOffset, MaxWait,
          MinBytes, MaxBytes, IsolationLevel) ->
        Tester ! {max_bytes, MaxBytes},
        meck:passthrough([Conn, Topic, Partition1, BeginOffset,
                          MaxWait, MinBytes, MaxBytes, IsolationLevel])
      end,
  %% Expect the fetch_request construction function called twice
  meck:expect(brod_kafka_request, fetch, F),
  Value = make_bytes(ValueBytes),
  Options = [{max_bytes, MaxBytes1}],
  {ok, ConsumerPid} =
    brod:subscribe(Client, self(), ?TOPIC, Partition, Options),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  ok = wait_for_max_bytes_sequence([{'=', MaxBytes1},
                                    {'>', MaxBytes2}],
                                   _TriedCount = 0),
  ?WAIT_ONLY(
     {ConsumerPid, #kafka_message_set{messages = Messages}},
     begin
       [#kafka_message{key = KeyReceived, value = ValueReceived}] = Messages,
       ?assertEqual(Key, KeyReceived),
       ?assertEqual(Value, ValueReceived)
     end).

%% @doc Consumer should auto recover from connection down, subscriber should not
%% notice a thing except for a few seconds of break in data streaming.
%% Covers the case when connection is shared with other partition leaders
t_consumer_connection_restart_0(Config) ->
  ConsumerConfig = [{share_leader_conn, true} | consumer_config()],
  consumer_connection_restart(Config, ConsumerConfig).

%% @doc Consumer should auto recover from connection down, subscriber should not
%% notice a thing except for a few seconds of break in data streaming.
%% Covers the case when connection is NOT shared with other partition leaders
t_consumer_connection_restart_1(Config) ->
  ConsumerConfig = [{share_leader_conn, false} | consumer_config()],
  consumer_connection_restart(Config, ConsumerConfig).

consumer_connection_restart(Config, ConsumerConfig) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  %% ensure slow fetch
  ConsumerCfg = [ {prefetch_count, 0}
                , {prefetch_bytes, 0}
                , {min_bytes, 1}
                , {max_bytes, 12} %% ensure fetch exactly one message at a time
                | ConsumerConfig
                ],
  {ok, ConsumerPid} =
    brod_consumer:start_link(whereis(Client), Topic, Partition, ConsumerCfg),
  ProduceFun =
    fun(I) ->
      Value = Key= integer_to_binary(I),
      {ok, Offset} =
        brod:produce_sync_offset(Client, Topic, Partition, Key, Value),
      Offset
    end,
  NumsCnt = 100,
  Nums = lists:seq(1, NumsCnt),
  Offsets = lists:map(ProduceFun, Nums),
  ok = brod_consumer:subscribe(ConsumerPid, self(),
                               [{begin_offset, hd(Offsets)}]),
  ConnPid = brod_consumer:get_connection(ConsumerPid),
  MatchNum = fun(#kafka_message{key = N, value = N}, [H | T]) ->
                 ?assertEqual(binary_to_integer(N), H),
                 T
             end,
  Receive =
    fun(NumsIn, Timeout) ->
        receive
          {ConsumerPid, #kafka_message_set{messages = Msgs}} ->
            lists:foldl(MatchNum, NumsIn, Msgs)
        after Timeout -> error(timeout)
        end
    end,
  %% Match exactly one message
  Nums1 = Receive(Nums, 5000),
  ?assertError(timeout, Receive(Nums1, 100)),
  ?assertEqual(NumsCnt - 1, length(Nums1)),
  %% kill connection, expect a restart later
  exit(ConnPid, kill),
  ok = brod:consume_ack(ConsumerPid, hd(Offsets)),
  NewConnPid = wait_for_consumer_connection(ConsumerPid, ConnPid),
  Nums2 = Receive(Nums1, 5000),
  ?assertError(timeout, Receive(Nums2, 100)),
  ?assertEqual(NumsCnt - 2, length(Nums2)),
  case proplists:get_bool(share_leader_conn, ConsumerConfig) of
    true ->
      ?assertEqual({ok, NewConnPid},
                   brod_client:get_leader_connection(Client, Topic, Partition)),
      ok = brod_consumer:stop(ConsumerPid),
      ?assertNot(is_process_alive(ConsumerPid)),
      ?assert(is_process_alive(NewConnPid));
    false ->
      %% assert normal shutdown
      Ref1 = erlang:monitor(process, NewConnPid),
      Ref2 = erlang:monitor(process, ConsumerPid),
      %% assert connection linked to consumer
      {links, Links} = process_info(ConsumerPid, links),
      ?assert(lists:member(NewConnPid, Links)),
      ok = brod_consumer:stop(ConsumerPid),
      Wait = fun() ->
        ?WAIT_ONLY({'DOWN', Ref, process, _, Reason},
        begin
          ?assertEqual(normal, Reason),
          ?assert(Ref =:= Ref1 orelse Ref =:= Ref2)
        end)
      end,
    Wait(),
    Wait()
  end,
  ok.

%% @doc same as t_consumer_connection_restart,
%% but test with brod_consumer started standalone.
%% i.e. not under brod_client's management
t_consumer_connection_restart_2(standalone_consumer) ->
  true;
t_consumer_connection_restart_2(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  %% ensure slow fetch
  ConsumerCfg = [ {prefetch_count, 0}
                , {prefetch_bytes, 0}
                , {min_bytes, 1}
                , {max_bytes, 12} %% ensure fetch exactly one message at a time
                | consumer_config()
                ],
  Bootstrap = {?HOSTS, ?config(client_config)},
  {ok, ConsumerPid} =
    brod_consumer:start_link(Bootstrap, Topic, Partition, ConsumerCfg),
  ProduceFun =
    fun(I) ->
      Value = Key= integer_to_binary(I),
      {ok, Offset} =
        brod:produce_sync_offset(Client, Topic, Partition, Key, Value),
      Offset
    end,
  NumsCnt = 100,
  Nums = lists:seq(1, NumsCnt),
  Offsets = lists:map(ProduceFun, Nums),
  ok = brod_consumer:subscribe(ConsumerPid, self(),
                               [{begin_offset, hd(Offsets)}]),
  ConnPid = brod_consumer:get_connection(ConsumerPid),
  MatchNum = fun(#kafka_message{key = N, value = N}, [H | T]) ->
                 ?assertEqual(binary_to_integer(N), H),
                 T
             end,
  Receive =
    fun(NumsIn, Timeout) ->
        receive
          {ConsumerPid, #kafka_message_set{messages = Msgs}} ->
            lists:foldl(MatchNum, NumsIn, Msgs)
        after Timeout -> error(timeout)
        end
    end,
  %% Match exactly one message
  Nums1 = Receive(Nums, 5000),
  ?assertError(timeout, Receive(Nums1, 100)),
  ?assertEqual(NumsCnt - 1, length(Nums1)),
  %% kill connection, expect a restart later
  exit(ConnPid, kill),
  ok = brod:consume_ack(ConsumerPid, hd(Offsets)),
  NewConnPid = wait_for_consumer_connection(ConsumerPid, ConnPid),
  Nums2 = Receive(Nums1, 5000),
  ?assertError(timeout, Receive(Nums2, 100)),
  ?assertEqual(NumsCnt - 2, length(Nums2)),
  %% assert normal shutdown
  Ref1 = erlang:monitor(process, NewConnPid),
  Ref2 = erlang:monitor(process, ConsumerPid),
  %% assert connection linked to consumer
  {links, Links} = process_info(ConsumerPid, links),
  ?assert(lists:member(NewConnPid, Links)),
  ok = brod_consumer:stop(ConsumerPid),
  Wait = fun() ->
             ?WAIT_ONLY({'DOWN', Ref, process, _, Reason},
                        begin
                          ?assertEqual(normal, Reason),
                          ?assert(Ref =:= Ref1 orelse Ref =:= Ref2)
                        end)
         end,
  Wait(),
  Wait(),
  ok.

%% @doc Data stream should resume after re-subscribe starting from the
%% the last acked offset
t_consumer_resubscribe(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
      Value = Key = integer_to_binary(I),
      brod:produce_sync(Client, Topic, Partition, Key, Value)
    end,
  ReceiveFun =
    fun(ExpectedSeqNo, Line) ->
      receive
        {_ConsumerPid, #kafka_message_set{messages = Messages}} ->
          #kafka_message{ offset = Offset
                        , key    = SeqNoBin
                        , value  = SeqNoBin
                        } = hd(Messages),
          SeqNo = list_to_integer(binary_to_list(SeqNoBin)),
          case SeqNo =:= ExpectedSeqNo of
            true  -> Offset;
            false -> ct:fail("unexpected message at line ~p, "
                             "key=value=~p, offset=~p",
                             [Line, SeqNo, Offset])
          end
      after 2000 ->
        ct:fail("timed out receiving kafka message set at line ~p", [Line])
      end
    end,
  {ok, ConsumerPid} = brod:subscribe(Client, self(), Topic, Partition, []),
  ok = ProduceFun(0),
  ok = ProduceFun(1),
  Offset0 = ReceiveFun(0, ?LINE),
  ok = brod:consume_ack(ConsumerPid, Offset0),
  %% unsubscribe after ack 0
  ok = brod:unsubscribe(Client, Topic, Partition),
  {ok, ConsumerPid} = brod:subscribe(Client, self(), Topic, Partition, []),
  Offset1 = ReceiveFun(1, ?LINE),
  %% unsubscribe before ack 1
  ok = brod:unsubscribe(Client, Topic, Partition),
  {ok, ConsumerPid} = brod:subscribe(Client, self(), Topic, Partition, []),
  %% message 1 should be redelivered
  Offset1 = ReceiveFun(1, ?LINE),
  ok.

%% @doc brod_consumer should allow re-subscribing with a 'earliest'
%% offset even though the default begin_offset 'lastest'.
t_consumer_resubscribe_earliest(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
      Value = Key = integer_to_binary(I),
      brod:produce_sync(Client, Topic, Partition, Key, Value)
    end,
  ReceiveFun =
    fun() ->
      receive
        {_ConsumerPid, #kafka_message_set{messages = Messages}} ->
          case hd(Messages) of
            #kafka_message{ offset = Offset
                          , key    = SeqNoBin
                          , value  = SeqNoBin
                          } ->
              {Offset, list_to_integer(binary_to_list(SeqNoBin))};
            #kafka_message{offset = Offset} ->
              {Offset, false}
          end
      after 2000 ->
        ct:fail("timed out receiving kafka message set", [])
      end
    end,
  {ok, ConsumerPid} = brod:subscribe(Client, self(), Topic, Partition,
                                     [{begin_offset, ?OFFSET_LATEST}]),
  SeqBase = erlang:monotonic_time(),
  ok = ProduceFun(SeqBase),
  ok = ProduceFun(SeqBase + 1),
  {Offset0, SeqNoRec0} = ReceiveFun(),
  ?assertEqual(SeqNoRec0, SeqBase),
  ok = brod:consume_ack(ConsumerPid, Offset0),
  {ok, ConsumerPid} = brod:subscribe(Client, self(), Topic, Partition,
                                     [{begin_offset, ?OFFSET_EARLIEST}]),
  {Offset1, SeqNoRec1} = ReceiveFun(),
  ?assert(Offset1 =< Offset0),
  ?assert(SeqNoRec1 =:= false orelse SeqNoRec1 =< SeqNoRec0),
  ok.

t_subscriber_restart(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
      Value = Key = integer_to_binary(I),
      brod:produce_sync(Client, Topic, Partition, Key, Value)
    end,
  Parent = self(),
  %% function to consume one message then exit(normal)
  SubscriberFun =
    fun() ->
      {ok, ConsumerPid} = brod:subscribe(Client, self(), Topic, Partition, []),
      Parent ! {subscribed, self()},
      receive
        {ConsumerPid, #kafka_message_set{messages = Messages}} ->
          #kafka_message{ offset = Offset
                        , key    = SeqNoBin
                        , value  = SeqNoBin
                        } = hd(Messages),
          Parent ! {self(), binary_to_integer(SeqNoBin)},
          ok = brod:consume_ack(ConsumerPid, Offset),
          exit(normal)
      after 2000 ->
        ct:pal("timed out receiving kafka message set", []),
        exit(timeout)
      end
    end,
  {Subscriber0, _} = erlang:spawn_monitor(SubscriberFun),
  receive {subscribed, Subscriber0} -> ok end,
  ReceiveFun =
    fun(Subscriber, ExpectedSeqNo) ->
      receive {Subscriber, ExpectedSeqNo} -> ok
      after 3000 -> ct:fail("timed out receiving seqno ~p", [ExpectedSeqNo])
      end
    end,
  ok = ProduceFun(0),
  ok = ProduceFun(1),
  ok = ReceiveFun(Subscriber0, 0),
  receive {'DOWN', _, process, Subscriber0, normal} -> ok
  after 2000 -> ct:fail("timed out waiting for Subscriber0 to exit")
  end,
  {Subscriber1, _} = erlang:spawn_monitor(SubscriberFun),
  receive {subscribed, Subscriber1} -> ok end,
  ok = ReceiveFun(Subscriber1, 1),
  ok.

t_subscribe_with_unknown_offset(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key = make_unique_key(),
  %% produce some random data just to make sure the partition is not empty
  ok = brod:produce_sync(Client, Topic, Partition, Key, Key),
  Options1 = [{begin_offset, 10000000000}],
  Options2 = [{sleep_ms, 0}, {begin_offset, -2}],
  {ok, Consumer} = brod:subscribe(Client, self(), Topic, Partition, Options1),
  {ok, Consumer} = brod:subscribe(Client, self(), Topic, Partition, Options2),
  receive
    {Consumer, #kafka_message_set{}} ->
      ok
  after 1000 ->
    ct:fail("timed out receiving kafka message set", [])
  end.

t_offset_reset_policy(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key0 = make_unique_key(),
  %% produce some random data just to make sure the partition is not empty
  ok = brod:produce_sync(Client, Topic, Partition, Key0, Key0),
  Options = [{sleep_ms, 0},
             {begin_offset, 10000000000}, %% make sure it's an unknown offset
             {offset_reset_policy, reset_to_earliest}],
  {ok, Consumer} = brod:subscribe(Client, self(), Topic, Partition, Options),
  receive
    {Consumer, #kafka_message_set{}} ->
      ok
  after 2000 ->
    ct:fail("timed out receiving kafka message set", [])
  end.

t_stop_kill(Config) when is_list(Config) ->
  {Pid, Mref} = spawn_monitor(fun() -> timer:sleep(100000) end),
  ?assert(is_process_alive(Pid)),
  ok = brod_consumer:stop_maybe_kill(Pid, 100),
  ?WAIT_ONLY({'DOWN', Mref, process, Pid, killed}, ok),
  ok = brod_consumer:stop_maybe_kill(Pid, 100).

%%%_* Help functions ===========================================================

%% os:timestamp should be unique enough for testing
make_unique_key() ->
  iolist_to_binary(["key-", make_ts_str()]).

make_ts_str() ->
  Ts = os:timestamp(),
  {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

make_bytes(Bytes) ->
  iolist_to_binary(lists:duplicate(Bytes, 0)).

%% Wait in a loop for the max_bytes to be tried in brod_consumer
%% The trigger of sending fetch request is non-deterministic
%% but the retry sequence should be at most 3 elements and monotonic
wait_for_max_bytes_sequence([], _Cnt) ->
  %% all expected max_bytes have been tried
  ok;
wait_for_max_bytes_sequence(_, 10) ->
  %% default sleep is 1 second, makes no sese to wait longer
  erlang:error(timeout);
wait_for_max_bytes_sequence([{Compare, MaxBytes} | Rest] = Waiting, Cnt) ->
  receive
    {max_bytes, Bytes} ->
      case Compare of
        '=' when Bytes =:= MaxBytes ->
          wait_for_max_bytes_sequence(Rest, 0);
        '>' when Bytes > MaxBytes ->
          wait_for_max_bytes_sequence(Rest, 0);
        _ when Bytes < MaxBytes ->
          %% still trying the old amx_bytes
          wait_for_max_bytes_sequence(Waiting, Cnt + 1);
        _ ->
          ct:fail("unexpected ~p, expecting ~p", [Bytes, {Compare, MaxBytes}])
      end;
    Other ->
      error(Other)
  after
    3000 ->
      ct:fail("timeout", [])
  end.

%% Make a random transactional id, so test cases would not interfere each other.
make_transactional_id() ->
  bin([atom_to_list(?MODULE), "-txn-", bin(rand())]).

bin(I) when is_integer(I) -> integer_to_binary(I);
bin(X) -> iolist_to_binary(X).

rand() -> rand:uniform(1000000).

connect_txn_coordinator(TxnId, Config) ->
  connect_txn_coordinator(TxnId, Config, 5, false).

%% Freshly setup kafka often fails on first few attempts for transaction
%% coordinator discovery, use a fail-retry loop to workaround
connect_txn_coordinator(_TxnId, _Config, 0, LastError) ->
  error({failed_to_connect_transaction_coordinator, LastError});
connect_txn_coordinator(TxnId, Config, RetriesLeft, _LastError) ->
  Args = #{type => txn, id => TxnId},
  case kpro:connect_coordinator(?HOSTS, Config, Args) of
    {ok, Conn} ->
      {ok, Conn};
    {error, Reason} ->
      timer:sleep(1000),
      connect_txn_coordinator(TxnId, Config, RetriesLeft - 1, Reason)
  end.

consumer_config() -> [{max_wait_time, 1000}, {sleep_timeout, 10}].

retry(_F, 0) -> error(timeout);
retry(F, Seconds) ->
  case F() of
    false ->
      timer:sleep(1000),
      retry(F, Seconds - 1);
    Value ->
      Value
  end.

wait_for_consumer_connection(Consumer, OldConn) ->
  F = fun() ->
          Pid = brod_consumer:get_connection(Consumer),
          Pid =/= undefined andalso Pid =/= OldConn andalso Pid
      end,
  retry(F, 5).

has_txn() ->
  case kafka_test_helper:kafka_version() of
    {0, Minor} when Minor < 11 ->
      false;
    _ ->
      true
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
