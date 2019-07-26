%%%
%%%   Copyright (c) 2016-2018 Klarna Bank AB (publ)
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
%%% This is a topic subscriber example
%%% @end
%%%=============================================================================

-module(brod_demo_topic_subscriber).
-behaviour(brod_topic_subscriber).

%% behabviour callbacks
-export([ init/2
        , handle_message/3
        ]).

-export([ bootstrap/0
        , bootstrap/2
        ]).

-export([ delete_commit_history/1
        ]).

-include("brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).
-define(TOPIC, <<"brod-demo-topic-subscriber">>).

-record(state, { offset_dir   :: string()
               , message_type :: message | message_type
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
-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(?PRODUCE_DELAY_SECONDS, message).

bootstrap(DelaySeconds, MessageType) ->
  ClientId = ?MODULE,
  BootstrapHosts = [{"localhost", 9092}],
  ClientConfig = client_config(),
  Topic = ?TOPIC,
  {ok, _} = application:ensure_all_started(brod),
  ok = brod:start_client(BootstrapHosts, ClientId, ClientConfig),
  ok = brod:start_producer(ClientId, Topic, _ProducerConfig = []),
  {ok, _Pid} = spawn_consumer(ClientId, Topic, MessageType),
  {ok, PartitionCount} = brod:get_partitions_count(ClientId, Topic),
  Partitions = lists:seq(0, PartitionCount - 1),
  ok = spawn_producers(ClientId, Topic, DelaySeconds, Partitions),
  ok.

%% @doc Get committed offsets from file `/tmp/<topic>'
init(Topic, MessageType) ->
  OffsetDir = commit_dir(Topic),
  Offsets = read_offsets(OffsetDir),
  State = #state{ offset_dir   = OffsetDir
                , message_type =  MessageType
                },
  {ok, Offsets, State}.

%% @doc Handle one message (not message-set).
handle_message(Partition, Message,
               #state{ offset_dir   = Dir
                     , message_type = message
                     } = State) ->
  process_message(Dir, Partition, Message),
  {ok, ack, State};
handle_message(Partition, MessageSet,
               #state{ offset_dir   = Dir
                     , message_type = message_set
                     } = State) ->
  #kafka_message_set{ partition = Partition
                    , messages  = Messages
                    } = MessageSet,
  [process_message(Dir, Partition, Message) || Message <- Messages],
  {ok, ack, State}.

delete_commit_history(Topic) ->
  Files = list_offset_files(commit_dir(Topic)),
  lists:foreach(fun(F) -> file:delete(F) end, Files).

%%%_* Internal Functions =======================================================

-spec process_message(file:fd(), brod:partition(), brod:message()) -> ok.
process_message(Dir, Partition, Message) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  Seqno = binary_to_integer(Value),
  Now = os_time_utc_str(),
  error_logger:info_msg("~p ~p ~s: offset:~w seqno:~w\n",
                       [ self(), Partition, Now, Offset, Seqno]),
  ok = commit_offset(Dir, Partition, Offset).

-spec read_offsets(string()) -> [{brod:partition(), brod:offset()}].
read_offsets(Dir) ->
  Files = list_offset_files(Dir),
  lists:map(fun(Filename) -> read_offset(Dir, Filename) end, Files).

list_offset_files(Dir) when is_binary(Dir) ->
  list_offset_files(binary_to_list(Dir));
list_offset_files(Dir) ->
  filelib:wildcard("*.offset", Dir).

-spec read_offset(string(), string()) -> {brod:partition(), brod:offset()}.
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

spawn_consumer(ClientId, Topic, MessageType) ->
  CallbackInitArg = MessageType,
  Config = [{offset_reset_policy, reset_to_earliest}],
  brod_topic_subscriber:start_link(ClientId, Topic, all,
                                   Config, MessageType,
                                   _CallbackModule = ?MODULE,
                                   CallbackInitArg).

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

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

commit_dir(Topic) ->
  filename:join(["/tmp", Topic]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
