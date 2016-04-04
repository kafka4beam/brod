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

-module(brod_demo_topic_subscriber).
-behaviour(brod_topic_subscriber).

%% behabviour callbacks
-export([ init/2
        , handle_message/3
        ]).

-export([ bootstrap/0
        , bootstrap/1
        ]).

-include_lib("brod/include/brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

-record(state, { offset_dir :: file:fd()
               }).

%% @doc This function bootstraps everything to demo of topic subscriber.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-demo-topic-subscriber">>
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - A subscriber which subscribes to all partitions.
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
  Topic = <<"brod-demo-topic-subscriber">>,
  {ok, _ClientPid} =
    brod:start_link_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, Topic, _ProducerConfig = []),
  {ok, _Pid} = spawn_consumer(ClientId, Topic),
  {ok, PartitionCount} = brod:get_partitions_count(ClientId, Topic),
  Partitions = lists:seq(0, PartitionCount - 1),
  ok = spawn_producers(ClientId, Topic, DelaySeconds, Partitions),
  ok.

%% @doc Initialize nothing in our case.
init(Topic, []) ->
  OffsetDir = filename:join(["/tmp", Topic]),
  Offsets = read_offsets(OffsetDir),
  State = #state{ offset_dir = OffsetDir },
  {ok, Offsets, State}.

%% @doc Handle one message (not message-set).
handle_message(Partition, Message,
               #state{ offset_dir = Dir
                     } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  Seqno = list_to_integer(binary_to_list(Value)),
  Now = os_time_utc_str(),
  error_logger:info_msg("~p ~p ~s: offset:~w seqno:~w\n",
                        [self(), Partition, Now, Offset, Seqno]),
  ok = commit_offset(Dir, Partition, Offset),
  {ok, ack, State}.

%%%_* Internal Functions =======================================================

-spec read_offsets(string()) -> [{kafka_partition(), kafka_offset()}].
read_offsets(Dir) when is_binary(Dir) ->
  read_offsets(binary_to_list(Dir));
read_offsets(Dir) ->
  Files = filelib:wildcard("*.offset", Dir),
  lists:map(fun(Filename) -> read_offset(Dir, Filename) end, Files).

-spec read_offset(string(), string()) -> {kafka_partition(), kafka_offset()}.
read_offset(Dir, Filename) ->
  PartitionStr = filename:basename(Filename, ".offset"),
  Partition = list_to_integer(PartitionStr),
  {ok, OffsetBin} = file:read_file(filename:join(Dir, Filename)),
  OffsetStr = string:strip(binary_to_list(OffsetBin), both, $\n),
  Offset = list_to_integer(OffsetStr),
  {Partition, Offset}.

filename(Dir, Partition) ->
  filename:join([Dir, integer_to_list(Partition) ++ ".offset"]).

commit_offset(Dir, Partition, Offset) ->
  Filename = filename(Dir, Partition),
  ok = filelib:ensure_dir(Filename),
  ok = file:write_file(Filename, [integer_to_list(Offset), $\n]).

spawn_consumer(ClientId, Topic) ->
  brod_topic_subscriber:start_link(ClientId, Topic, all,
                                   _ConsumerConfig  = [],
                                   _CallbackModule  = ?MODULE,
                                   _CallbackInitArg = []).

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

