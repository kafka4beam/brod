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
%%% This is called 'loc' as in 'Local Offset Commit'. i.e. it demos an
%%% implementation of group subscriber that writes offsets locally (to file
%%% in this module), but does not commit offsets to Kafka.
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_demo_group_subscriber_loc).

-behaviour(brod_group_subscriber).

-export([ bootstrap/0
        , bootstrap/1
        ]).

%% behabviour callbacks
-export([ init/2
        , handle_message/4
        , get_committed_offsets/3
        ]).


-include_lib("brod/include/brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

-record(state, { group_id :: binary()
               , offset_dir :: file:fd()
               }).

%% @doc This function bootstraps everything to demo of group subscriber.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-demo-group-subscriber-loc">>
%%     having two or more partitions.
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - X group subscribers, X is the number of partitions
%%
%% * consumed sequence numbers are printed to console
%% * consumed offsets are written to file /tmp/T/P.offset
%%   where T is the topic name and X is the partition number
%% @end
-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(?PRODUCE_DELAY_SECONDS).

bootstrap(DelaySeconds) ->
  ClientId = ?MODULE,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig = [],
  Topic = <<"brod-demo-group-subscriber-loc">>,
  GroupId = iolist_to_binary([Topic, "-group-id"]),
  {ok, _} = application:ensure_all_started(brod),
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, Topic, _ProducerConfig = []),
  {ok, PartitionCount} = brod:get_partitions_count(ClientId, Topic),
  Partitions = lists:seq(0, PartitionCount - 1),
  %% spawn N + 1 consumers, one of them will have no assignment
  %% it should work as a 'standing-by' consumer.
  ok = spawn_consumers(GroupId, ClientId, Topic, PartitionCount + 1),
  ok = spawn_producers(ClientId, Topic, DelaySeconds, Partitions),
  ok.

%% @doc Initialize nothing in our case.
init(GroupId, []) ->
  OffsetDir = "/tmp",
  {ok, #state{ group_id   = GroupId
             , offset_dir = OffsetDir
             }}.

%% @doc Handle one message (not message-set).
handle_message(Topic, Partition, Message,
               #state{ offset_dir = Dir
                     , group_id   = GroupId
                     } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  Seqno = list_to_integer(binary_to_list(Value)),
  Now = os_time_utc_str(),
  error_logger:info_msg("~p ~p ~s: offset:~w seqno:~w\n",
                        [self(), Partition, Now, Offset, Seqno]),
  ok = commit_offset(Dir, GroupId, Topic, Partition, Offset),
  {ok, ack, State}.

%% @doc This callback is called whenever there is a new assignment received.
%% e.g. when joining the group after restart, or group assigment rebalance
%% was triggered if other memgers join or leave the group
%% NOTE: A subscriber may get assigned with a random set of topic-partitions
%%       (unless some 'sticky' protocol is introduced to group controller),
%%       meaning, if group members are running in different hosts they may
%%       have to perform 'Local Offset Commit' in a central database or
%%       whatsoever instead of local file system.
%% @end
get_committed_offsets(GroupId, TopicPartitions,
                      #state{offset_dir = Dir} = State) ->
  F = fun({Topic, Partition}, Acc) ->
        case file:read_file(filename(Dir, GroupId, Topic, Partition)) of
          {ok, OffsetBin} ->
            OffsetStr = string:strip(binary_to_list(OffsetBin), both, $\n),
            Offset = list_to_integer(OffsetStr),
            [{{Topic, Partition}, Offset} | Acc];
          {error, enoent} ->
            Acc
        end
      end,
  {ok, lists:foldl(F, [], TopicPartitions), State}.

%%%_* Internal Functions =======================================================

filename(Dir, GroupId, Topic, Partition) ->
  filename:join([Dir, GroupId, Topic, integer_to_list(Partition)]).

commit_offset(Dir, GroupId, Topic, Partition, Offset) ->
  Filename = filename(Dir, GroupId, Topic, Partition),
  ok = filelib:ensure_dir(Filename),
  ok = file:write_file(Filename, [integer_to_list(Offset), $\n]).

spawn_consumers(GroupId, ClientId, Topic, ConsumerCount) ->
  %% commit offsets to kafka every 10 seconds
  GroupConfig = [{offset_commit_policy, consumer_managed}],
  lists:foreach(
    fun(_I) ->
      {ok, _Subscriber} =
        brod_group_subscriber:start_link(ClientId, GroupId, [Topic],
                                         GroupConfig,
                                         _ConsumerConfig  = [],
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

