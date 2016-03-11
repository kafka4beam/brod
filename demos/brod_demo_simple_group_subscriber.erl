%%%
%%%   Copyright (c) 2016 Klarna AB
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
%%% This is a consumer group consumer example
%%% This is called _simple because it demos a all-configs-by-default minimal
%%% implenemtation of a consumer group subscriber.
%%% See bootstrap/0 for more details about all prerequisite.
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_demo_simple_group_subscriber).

-behaviour(brod_group_subscriber).

-export([ bootstrap/0
        ]).

%% behabviour callbacks
-export([ init/1
        , handle_message/4
        ]).


-include("brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

%% @doc This function bootstraps everything to demo of group consumer.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-demo-1">>
%%     having two or more partitions.
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - X group subscribers, X is the number of partitions
%%     offsets are commited to kafak (using v2 commit requests)
%%     consumed sequence numbers are printed to console
%% @end
-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(?PRODUCE_DELAY_SECONDS).

bootstrap(DelaySeconds) ->
  ClientID = ?MODULE,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig = [],
  Topic = <<"brod-demo-1">>,
  {ok, _ClientPid} =
    brod:start_link_client(BootstrapHosts, ClientID, ClientConfig),
  ok = brod:start_producer(ClientID, Topic, _ProducerConfig = []),
  {ok, PartitionCount} = brod:get_partitions_count(ClientID, Topic),
  Partitions = lists:seq(0, PartitionCount - 1),
  %% spawn N + 1 consumers, one of them will have no assignment
  %% it should work as a 'standing-by' consumer.
  ok = spawn_consumers(ClientID, Topic, PartitionCount + 1),
  ok = spawn_producers(ClientID, Topic, DelaySeconds, Partitions),
  ok.

%% @doc Initialize nothing in our case.
init(_Arg) -> {ok, []}.

%% @doc Handle one message (not message-set).
handle_message(_Topic, Partition, Message, State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  Seqno = list_to_integer(binary_to_list(Value)),
  Now = os_time_utc_str(),
  io:format("~p ~p ~s: offset:~8w seqno:~8w\n",
            [self(), Partition, Now, Offset, Seqno]),
  {ok, ack, State}.

%%%_* Internal Functions =======================================================

spawn_consumers(ClientID, Topic, ConsumerCount) ->
  %% commit offsets to kafka every 10 seconds
  GroupConfig = [{offset_commit_policy, {periodic_seconds, 10}}],
  GroupID = iolist_to_binary([Topic, "-groupd-id"]),
  lists:foreach(
    fun(_I) ->
      {ok, _Subscriber} =
        brod_group_subscriber:start_link(ClientID, GroupID, [Topic],
                                         GroupConfig,
                                         _ConsumerConfig  = [],
                                         _CallbackModule  = ?MODULE,
                                         _CallbackInitArg = [])
    end, lists:seq(1, ConsumerCount)).

spawn_producers(_ClientID, _Topic, _DelaySeconds, []) -> ok;
spawn_producers(ClientID, Topic, DelaySeconds, [Partition | Partitions]) ->
  erlang:spawn_link(
    fun() ->
      producer_loop(ClientID, Topic, Partition, DelaySeconds, 0)
    end),
  spawn_producers(ClientID, Topic, DelaySeconds, Partitions).

producer_loop(ClientID, Topic, Partition, DelaySeconds, Seqno) ->
  KafkaValue = iolist_to_binary(integer_to_list(Seqno)),
  ok = brod:produce_sync(ClientID, Topic, Partition, _Key = <<>>, KafkaValue),
  timer:sleep(timer:seconds(DelaySeconds)),
  producer_loop(ClientID, Topic, Partition, DelaySeconds, Seqno+1).

-spec os_time_utc_str() -> string().
os_time_utc_str() ->
  Ts = os:timestamp(),
  {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
