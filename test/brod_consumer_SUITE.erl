%%%
%%%   Copyright (c) 2015 - 2017, Klarna AB
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
-export([ t_direct_fetch/1
        , t_direct_fetch_with_small_max_bytes/1
        , t_direct_fetch_expand_max_bytes/1
        , t_consumer_max_bytes_too_small/1
        , t_consumer_socket_restart/1
        , t_consumer_resubscribe/1
        , t_consumer_restart_with_prefetch_window/1
        , t_subscriber_restart/1
        , t_subscribe_with_unknown_offset/1
        , t_offset_reset_policy/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

-define(WAIT(Pattern, Expr),
        fun() ->
          receive
            Pattern ->
              Expr;
            _Msg ->
              ct:pal("exp: ~s\ngot: ~p\n", [??Pattern, _Msg]),
              erlang:error(unexpected_msg)
          after
            3000 ->
              erlang:error({timeout, ??Pattern})
          end
        end()).

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
  CooldownSecs = 2,
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
  ok = brod:start_client(?HOSTS, Client, ClientConfig0 ++ ClientConfig1),
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  ok = brod:start_consumer(Client, Topic, []),
  [{client, Client} | Config].

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

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

t_direct_fetch(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key = make_unique_key(),
  Value = <<>>,
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  {ok, Offset} = brod:resolve_offset(?HOSTS, Topic, Partition,
                                     ?OFFSET_LATEST),
  {ok, [Msg]} = brod:fetch(?HOSTS, Topic, Partition, Offset - 1),
  ?assertEqual(Key, Msg#kafka_message.key),
  ok.

t_direct_fetch_with_small_max_bytes(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  Key = make_unique_key(),
  Value = crypto:strong_rand_bytes(100),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  {ok, Offset} = brod:resolve_offset(?HOSTS, Topic, Partition,
                                     ?OFFSET_LATEST),
  {ok, [Msg]} = brod:fetch(?HOSTS, Topic, Partition, Offset - 1,
                           _Timeout = 1000, _MinBytes = 0, _MaxBytes = 1),
  ?assertEqual(Key, Msg#kafka_message.key),
  ok.

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
                                     ?OFFSET_LATEST),
  {ok, [Msg]} = brod:fetch(?HOSTS, Topic, Partition, Offset - 1,
                           _Timeout = 1000, _MinBytes = 0, _MaxBytes = 13),
  ?assertEqual(Key, Msg#kafka_message.key),
  ok.

%% @doc Consumer should be smart enough to try greater max_bytes
%% when it's not great enough to fetch one single message
%% @end
t_consumer_max_bytes_too_small({init, Config}) ->
  meck:new(kpro, [passthrough, no_passthrough_cover, no_history]),
  %% kafka returns empty message set when it's 0.9
  %% or when fetch request sent was version 0
  %% Avoid querying api version will make brod send v0 requests
  [{client_config, [{query_api_versions, false}]} | Config];
t_consumer_max_bytes_too_small({'end', _Config}) ->
  meck:unload(kpro);
t_consumer_max_bytes_too_small(Config) ->
  Client = ?config(client),
  Partition = 0,
  brod:unsubscribe(Client, ?TOPIC, Partition),
  Key = make_unique_key(),
  ValueBytes = 2000,
  MaxBytes1 = 8, %% too small for even the header
  MaxBytes2 = 12, %% too small but message size is fetched
  MaxBytes3 = size(Key) + ValueBytes,
  Tester = self(),
  F = fun(Vsn, Topic, Partition1, BeginOffset, MaxWait, MinBytes, MaxBytes) ->
        Tester ! {max_bytes, MaxBytes},
        meck:passthrough([Vsn, Topic, Partition1, BeginOffset,
                          MaxWait, MinBytes, MaxBytes])
      end,
  %% Expect the fetch_request construction function called twice
  meck:expect(kpro, fetch_request, F),
  Value = make_bytes(ValueBytes),
  Options = [{max_bytes, MaxBytes1}],
  {ok, ConsumerPid} =
    brod:subscribe(Client, self(), ?TOPIC, Partition, Options),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  ok = wait_for_max_bytes_sequence([{'=', MaxBytes1},
                                    {'=', MaxBytes2},
                                    {'>', MaxBytes3}],
                                   _TriedCount = 0),
  ?WAIT({ConsumerPid, #kafka_message_set{messages = Messages}},
        begin
          [#kafka_message{key = KeyReceived, value = ValueReceived}] = Messages,
          ?assertEqual(Key, KeyReceived),
          ?assertEqual(Value, ValueReceived)
        end).

%% @doc Consumer shoud auto recover from socket down, subscriber should not
%% notice a thing except for a few seconds of break in data streaming
%% @end
t_consumer_socket_restart(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
      Key = list_to_binary(integer_to_list(I)),
      Value = Key,
      brod:produce_sync(Client, Topic, Partition, Key, Value)
    end,
  Parent = self(),
  SubscriberPid =
    erlang:spawn_link(
      fun() ->
        {ok, _ConsumerPid} =
          brod:subscribe(Client, self(), Topic, Partition, []),
        Parent ! {subscriber_ready, self()},
        seqno_consumer_loop(0, _ExitSeqNo = undefined)
      end),
  receive
    {subscriber_ready, SubscriberPid} ->
      ok
  end,
  ProducerPid =
    erlang:spawn_link(
      fun() -> seqno_producer_loop(ProduceFun, 0, undefined, Parent) end),
  receive
    {produce_result_change, ProducerPid, ok} ->
      ok
  after 1000 ->
    ct:fail("timed out waiting for seqno producer loop to start")
  end,
  {ok, SocketPid} =
    brod_client:get_leader_connection(Client, Topic, Partition),
  exit(SocketPid, kill),
  receive
    {produce_result_change, ProducerPid, error} ->
      ok
  after 1000 ->
    ct:fail("timed out receiving seqno producer loop to report error")
  end,
  receive
    {produce_result_change, ProducerPid, ok} ->
      ok
  after 5000 ->
    ct:fail("timed out waiting for seqno producer to recover")
  end,
  ProducerPid ! stop,
  ExitSeqNo =
    receive
      {producer_exit, ProducerPid, SeqNo} ->
        SeqNo
    after 2000 ->
      ct:fail("timed out waiting for seqno producer to report last seqno")
    end,
  Mref = erlang:monitor(process, SubscriberPid),
  %% tell subscriber to stop at ExitSeqNo because producer stopped before
  %% that number.
  SubscriberPid ! {exit_seqno, ExitSeqNo},
  receive
    {'DOWN', Mref, process, SubscriberPid, Reason} ->
      ?assertEqual(normal, Reason)
  after 6000 ->
    ct:fail("timed out waiting for seqno subscriber to exit")
  end.

%% @doc Data stream should resume after re-subscribe starting from the
%% the last acked offset
%% @end
t_consumer_resubscribe(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
      Key = list_to_binary(integer_to_list(I)),
      Value = Key,
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

t_subscriber_restart(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
      Key = list_to_binary(integer_to_list(I)),
      Value = Key,
      brod:produce_sync(Client, Topic, Partition, Key, Value)
    end,
  Parent = self(),
  %% fuction to consume one message then exit(normal)
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
          SeqNo = list_to_integer(binary_to_list(SeqNoBin)),
          Parent ! {self(), SeqNo},
          ok = brod:consume_ack(ConsumerPid, Offset),
          exit(normal)
      after 2000 ->
        ct:pal("timed out receiving kafka message set", []),
        exit(timeout)
      end
    end,
  Subscriber0 = erlang:spawn_link(SubscriberFun),
  receive {subscribed, Subscriber0} -> ok end,
  ReceiveFun =
    fun(Subscriber, ExpectedSeqNo) ->
      receive
        {Subscriber, ExpectedSeqNo} ->
          ok
      after 3000 ->
        ct:fail("timed out receiving seqno ~p", [ExpectedSeqNo])
      end
    end,
  ok = ProduceFun(0),
  ok = ProduceFun(1),
  ok = ReceiveFun(Subscriber0, 0),
  Mref = erlang:monitor(process, Subscriber0),
  receive
    {'DOWN', Mref, process, Subscriber0, normal} ->
      ok
    after 1000 ->
      ct:fail("timed out waiting for Subscriber0 to exit")
  end,
  Subscriber1 = erlang:spawn_link(SubscriberFun),
  receive {subscribed, Subscriber1} -> ok end,
  ok = ReceiveFun(Subscriber1, 1),
  ok.

%% @doc When a consumer restarts with unacked messages pending, the
%% prefetch window is shrunk with the number of pending messages upon
%% resubscribe.
t_consumer_restart_with_prefetch_window(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  Partition = 0,
  ProduceFun =
    fun(I) ->
        Key = list_to_binary(integer_to_list(I)),
        Value = Key,
        brod:produce_sync(Client, Topic, Partition, Key, Value)
    end,
  ParseFun =
    fun (#kafka_message{ offset = Offset
                       , key    = SeqNoBin
                       , value  = SeqNoBin
                       }) ->
        {Offset, list_to_integer(binary_to_list(SeqNoBin))}
    end,
  ReceiveOneFun =
    fun (Line) ->
        receive
          {_ConsumerPid, #kafka_message_set{messages = Messages}} ->
            {_, FirstSeqNo} = ParseFun(hd(Messages)),
            {ROffsets, NextSeqNo} =
              lists:foldl(
                fun (Msg, {Ack, SeqNo}) ->
                    case ParseFun(Msg) of
                      {Offset, SeqNo} ->
                        {[Offset | Ack], SeqNo + 1};
                      {Offset, BadSeqNo} ->
                        ct:fail("unexpected message at line ~p, "
                                "key=value=~p, offset=~p",
                                [Line, BadSeqNo, Offset])
                    end
                end,
                {[], FirstSeqNo},
                Messages),
            ct:log("ReceiveOneFun -> ~p", [{ok, FirstSeqNo, NextSeqNo, lists:reverse(ROffsets)}]),
            {ok, FirstSeqNo, NextSeqNo, lists:reverse(ROffsets)}
        after 2000 ->
            ct:log("ReceiveOneFun -> ~p", [false]),
            false
        end
    end,
  ReceiveAllFun =
    fun F(ExpectedSeqNo, Line) ->
        ct:log("ReceiveAllFun(~p)", [ExpectedSeqNo]),
        case ReceiveOneFun(Line) of
          {ok, ExpectedSeqNo, NextSeqNo, Offsets} ->
            Offsets ++ F(NextSeqNo, Line);
          {ok, FirstSeqNo, _NextSeqNo, [FirstOffset | _]} ->
            ct:fail("unexpected message at line ~p, "
                    "key=value=~p, offset=~p",
                    [Line, FirstSeqNo, FirstOffset]);
          false ->
            []
        end
    end,
  ReceiveFun =
    fun (ExpectedSeqNo, Line) ->
        case ReceiveAllFun(ExpectedSeqNo, Line) of
          [] ->
            ct:fail("timed out receiving kafka message set at line ~p", [Line]);
          Offsets ->
            Offsets
        end
    end,
  %% set `prefetch_count' to 1, which means at most 2 unacked messages
  %% being delivered to the subscriber), and use a very small
  %% `max_bytes' to guarantee messages are received one-by-one
  %% (otherwise the tests may accidentally exceed the `prefetch_count'
  %% when Kafka delivers multiple messages in a single response)
  Opts = [{prefetch_count, 1}, {max_bytes, 10}],
  {ok, ConsumerPid1} = brod:subscribe(Client, self(), Topic, Partition, Opts),
  ok = ProduceFun(0),
  ok = ProduceFun(1),
  ok = ProduceFun(2),
  ok = ProduceFun(3),
  %% due to the prefetch window size only two messages are delivered
  [Offset0, _Offset1] = ReceiveFun(0, ?LINE),
  %% ack 0 and receive one more message
  ok = brod:consume_ack(ConsumerPid1, Offset0),
  [Offset2] = ReceiveFun(2, ?LINE),
  %% ack 2 and receive one more message (because the prefetch window
  %% is 2, but there's only one more message left in the partition)
  ok = brod:consume_ack(ConsumerPid1, Offset2),
  [Offset3] = ReceiveFun(3, ?LINE),
  %% kill the consumer process, produce 2 more messages and resubscribe
  Mon = monitor(process, ConsumerPid1),
  exit(ConsumerPid1, test_consumer_restart),
  receive {'DOWN', Mon, process, ConsumerPid1, test_consumer_restart} -> ok
  after 1000 -> ct:fail("timed out waiting for the consumer process to die")
  end,
  ok = ProduceFun(4),
  ok = ProduceFun(5),
  TrySubscribeFun =
    fun T(0) ->
        ct:fail("timed out waiting for the consumer process to restart");
        T(N) when N > 0 ->
        timer:sleep(200),
        case brod:subscribe(Client, self(), Topic, Partition,
                            [ {begin_offset, Offset3 + 1}
                            , {acked_offset, Offset2}
                            | Opts
                            ]) of
          {error, Reason} ->
            ct:log("failed to resubscribe after consumer process' crash: ~p",
                   [Reason]),
            T(N - 1);
          {ok, Pid} ->
            {ok, Pid}
        end
    end,
  {ok, ConsumerPid2} = TrySubscribeFun(30), % 30 * 200ms = 6s timeout
  %% as 3 is not yet acked, the prefetch window size shall be only 1
  [_Offset4] = ReceiveFun(4, ?LINE),
  %% ack 3 and receive the last message
  ok = brod:consume_ack(ConsumerPid2, Offset3),
  [_Offset5] = ReceiveFun(5, ?LINE),
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

%%%_* Help functions ===========================================================

%% @private Expecting sequence numbers delivered from kafka
%% not expecting any error messages.
%% @end
seqno_consumer_loop(ExitSeqNo, ExitSeqNo) ->
  %% we have verified all sequence numbers, time to exit
  exit(normal);
seqno_consumer_loop(ExpectedSeqNo, ExitSeqNo) ->
  receive
    {ConsumerPid, #kafka_message_set{messages = Messages}} ->
      MapFun = fun(#kafka_message{ offset = Offset
                                 , key    = SeqNo
                                 , value  = SeqNo}) ->
                 ok = brod:consume_ack(ConsumerPid, Offset),
                 list_to_integer(binary_to_list(SeqNo))
               end,
      SeqNoList = lists:map(MapFun, Messages),
      NextSeqNoToExpect = verify_seqno(ExpectedSeqNo, SeqNoList),
      seqno_consumer_loop(NextSeqNoToExpect, ExitSeqNo);
    {exit_seqno, SeqNo} ->
      seqno_consumer_loop(ExpectedSeqNo, SeqNo);
    Msg ->
      exit({"unexpected message received", Msg})
  end.

%% @private Verify if a received sequence number list is as expected
%% sequence numbers are allowed to get redelivered,
%% but should not be re-ordered.
%% @end
verify_seqno(SeqNo, []) ->
  SeqNo + 1;
verify_seqno(SeqNo, [X | _] = SeqNoList) when X < SeqNo ->
  verify_seqno(X, SeqNoList);
verify_seqno(SeqNo, [SeqNo | Rest]) ->
  verify_seqno(SeqNo + 1, Rest);
verify_seqno(SeqNo, SeqNoList) ->
  exit({"sequence number received is not as expected", SeqNo, SeqNoList}).

%% @private Produce sequence numbers in a retry loop.
%% Report produce API return value pattern changes to parent pid
%% @end
seqno_producer_loop(ProduceFun, SeqNo, LastResult, Parent) ->
  {Result, NextSeqNo} =
    case ProduceFun(SeqNo) of
      ok ->
        {ok, SeqNo + 1};
      {error, _Reason} ->
        {error, SeqNo}
    end,
  case Result =/= LastResult of
    true  -> Parent ! {produce_result_change, self(), Result};
    false -> ok
  end,
  receive
    stop ->
      Parent ! {producer_exit, self(), NextSeqNo},
      exit(normal)
  after 100 ->
    ok
  end,
  seqno_producer_loop(ProduceFun, NextSeqNo, Result, Parent).

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
      end
  after
    3000 ->
      ct:fail("timeout", [])
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
