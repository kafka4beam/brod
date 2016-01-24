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
-compile(export_all).

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
  Producer = {Topic, [{partition_restart_delay_seconds, ProducerRestartDelay}]},
  Consumer = {Topic, []},
  case whereis(Client) of
    ?undef -> ok;
    Pid_   -> brod:stop_client(Pid_)
  end,
  {ok, ClientPid} =
    brod:start_link_client(Client, ?HOSTS,
                           [Producer], [Consumer], ClientConfig),
  [{client, Client}, {client_pid, ClientPid} | Config].

end_per_testcase(_Case, Config) ->
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
  %% that bumber.
  SubscriberPid ! {exit_seqno, ExitSeqNo},
  receive
    {'DOWN', Mref, process, SubscriberPid, Reason} ->
      ?assertEqual(normal, Reason)
  after 5000 ->
    ct:fail("timed out waiting for seqno subscriber to exit")
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
%% sequence numbers are allowed to get redelivered, but should not be re-ordered.
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
