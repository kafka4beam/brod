%%%
%%%   Copyright (c) 2015, 2016, Klarna AB
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
-export([ t_consumer_max_bytes_too_small/1
        , t_consumer_socket_restart/1
        , t_consumer_resubscribe/1
        , t_subscriber_restart/1
        , t_subscribe_with_unknown_offset/1
        , t_offset_reset_policy/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  ct:pal("=== ~p begin ===", [Case]),
  Client = Case,
  Topic = ?TOPIC,
  CooldownSecs = 2,
  ProducerRestartDelay = 1,
  ClientConfig = [{reconnect_cool_down_seconds, CooldownSecs}],
  ProducerConfig = [ {partition_restart_delay_seconds, ProducerRestartDelay}
                   , {max_retries, 0}],
  case whereis(Client) of
    ?undef -> ok;
    Pid_   -> brod:stop_client(Pid_)
  end,
  {ok, ClientPid} = brod:start_link_client(?HOSTS, Client, ClientConfig),
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  ok = brod:start_consumer(Client, Topic, []),
  [{client, Client}, {client_pid, ClientPid} | Config].

end_per_testcase(Case, Config) ->
  ct:pal("=== ~p end ===", [Case]),
  Pid = ?config(client_pid),
  try
    Ref = erlang:monitor(process, Pid),
    brod:stop_client(Pid),
    receive
      {'DOWN', Ref, process, Pid, _} -> ok
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

%% @doc Consumer should be smart enough to try greater max_bytes
%% when it's not great enough to fetch one single message
%% @end
t_consumer_max_bytes_too_small(Config) ->
  Client = ?config(client_pid),
  Partition = 0,
  Key = make_unique_key(),
  Value = make_bytes(2000),
  Options = [{max_bytes, 1500}],
  {ok, ConsumerPid} =
    brod:subscribe(Client, self(), ?TOPIC, Partition, Options),
  ok = brod:produce_sync(Client, ?TOPIC, Partition, Key, Value),
  receive
    {ConsumerPid, #kafka_message_set{messages = Messages}} ->
      [#kafka_message{key = KeyReceived}] = Messages,
      ?assertEqual(Key, KeyReceived);
    Msg ->
      ct:fail("unexpected message received:\n~p", [Msg])
  end.

%% @doc Consumer shoud auto recover from socket down, subscriber should not
%% notice a thing except for a few seconds of break in data streaming
%% @end
t_consumer_socket_restart(Config) ->
  Client = ?config(client_pid),
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
  Client = ?config(client_pid),
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
  Client = ?config(client_pid),
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

t_subscribe_with_unknown_offset(Config) when is_list(Config) ->
  Client = ?config(client_pid),
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
  Client = ?config(client_pid),
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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
