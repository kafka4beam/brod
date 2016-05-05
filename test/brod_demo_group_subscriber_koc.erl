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
%%% This is a consumer group subscriber example
%%% This is called 'koc' as in kafka-offset-commit,
%%% it demos a all-configs-by-default minimal implenemtation of a
%%% consumer group subscriber which commits offsets to kafka.
%%% See bootstrap/0 for more details about all prerequisite.
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_demo_group_subscriber_koc).

-behaviour(brod_group_subscriber).

-export([ bootstrap/0
        , bootstrap/1
        ]).

%% behabviour callbacks
-export([ init/2
        , handle_message/4
        ]).


-include_lib("brod/include/brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

%% @doc This function bootstraps everything to demo of group subscriber.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-group-subscriber-demo-koc">>
%%     having two or more partitions.
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - X group subscribers, X is the number of partitions
%%
%% * consumed sequence numbers are printed to console
%% * consumed offsets are commited to kafka (using v2 commit requests)
%% @end
-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(?PRODUCE_DELAY_SECONDS).

bootstrap(DelaySeconds) ->
  ClientId = ?MODULE,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig = [],
  Topic = <<"brod-demo-group-subscriber-koc">>,
  {ok, _} = application:ensure_all_started(brod),
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, Topic, _ProducerConfig = []),
  {ok, PartitionCount} = brod:get_partitions_count(ClientId, Topic),
  Partitions = lists:seq(0, PartitionCount - 1),
  %% spawn N + 1 consumers, one of them will have no assignment
  %% it should work as a 'standing-by' consumer.
  ok = spawn_consumers(ClientId, Topic, PartitionCount + 1),
  ok = spawn_producers(ClientId, Topic, DelaySeconds, Partitions),
  ok.

%% @doc Initialize nothing in our case.
init(_GroupId, _Arg) -> {ok, []}.

%% @doc Handle one message (not message-set).
handle_message(_Topic, Partition, Message, State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  Seqno = list_to_integer(binary_to_list(Value)),
  Now = os_time_utc_str(),
  error_logger:info_msg("~p ~p ~s: offset:~w seqno:~w\n",
                        [self(), Partition, Now, Offset, Seqno]),
  {ok, ack, State}.

%%%_* Internal Functions =======================================================

spawn_consumers(ClientId, Topic, ConsumerCount) ->
  %% commit offsets to kafka every 10 seconds
  GroupConfig = [{offset_commit_policy, commit_to_kafka_v2}
                ,{offset_commit_interval_seconds, 5}
                ],
  GroupId = iolist_to_binary([Topic, "-group-id"]),
  lists:foreach(
    fun(_I) ->
      {ok, _Subscriber} =
        brod:start_link_group_subscriber(ClientId, GroupId, [Topic],
                                         GroupConfig,
                                         _ConsumerConfig  = [{begin_offset, -2}],
                                         _CallbackModule  = ?MODULE,
                                         _CallbackInitArg = [])
    end, lists:seq(1, ConsumerCount)).

spawn_producers(_ClientId, _Topic, _DelaySeconds, []) -> ok;
spawn_producers(ClientId, Topic, DelaySeconds, [Partition | Partitions]) ->
  erlang:spawn_link(
    fun() ->
      producer_loop(ClientId, Topic, Partition, DelaySeconds, 0)
    end),
  spawn_producers(ClientId, Topic, DelaySeconds, Partitions).

producer_loop(ClientId, Topic, Partition, DelaySeconds, Seqno) ->
  KafkaValue = iolist_to_binary(integer_to_list(Seqno)),
  ok = brod:produce_sync(ClientId, Topic, Partition, _Key = <<>>, KafkaValue),
  timer:sleep(timer:seconds(DelaySeconds)),
  producer_loop(ClientId, Topic, Partition, DelaySeconds, Seqno+1).

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
