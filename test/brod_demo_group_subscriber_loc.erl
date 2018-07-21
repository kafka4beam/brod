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
%%% This is a consumer group subscriber example
%%% This is called 'loc' as in 'Local Offset Commit'. i.e. it demos an
%%% implementation of group subscriber that writes offsets locally (to file
%%% in this module), but does not commit offsets to Kafka.
%%% @end
%%%=============================================================================

-module(brod_demo_group_subscriber_loc).

-behaviour(brod_group_subscriber).

-export([ bootstrap/0
        , bootstrap/1
        , bootstrap/2
        , bootstrap/3
        ]).

%% behabviour callbacks
-export([ init/2
        , handle_message/4
        , get_committed_offsets/3
        ]).

-include("brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

-record(state, { group_id :: binary()
               , offset_dir :: file:fd()
               , message_type  :: message | message_set
               , handlers = [] :: [{{brod:topic(), brod:partition()}, pid()}]
               }).

%% @doc This function bootstraps everything to demo group subscribers.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-demo-group-subscriber-loc">>
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - X group subscribers, X is the number of partitions
%%
%% * consumed sequence numbers are printed to console
%% * consumed offsets are written to file /tmp/T/P.offset
%%   where T is the topic name and X is the partition number
%%
%% NOTE: Here we spawn two clients in one Erlang node just to demo how
%% group subscribers work.
%% Please keep in mind that a group subscriber requires one dedicated
%% TCP connection for group leader election, heartbeats and state syncing etc.
%% It is a good practice to limit the number of group members per Erlang
%% node for each group. One group member per Erlang node should be enough
%% for most of the use cases.
%% A group subscriber may receive messages from all topic-partitions assigned,
%% in its handle_message callback function, it may process the message
%% synchronously, or dispatch it to any number of worker processes for
%% concurrent processing, acks can be sent from the worker processes
%% by calling brod_group_subscriber:ack/4
%% @end
-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(?PRODUCE_DELAY_SECONDS).

bootstrap(DelaySeconds) ->
  bootstrap(DelaySeconds, message).

bootstrap(DelaySeconds, MessageType) ->
  %% A group ID is to be shared between the members (which often run in
  %% different Erlang nodes or even hosts).
  GroupId = <<"brod-demo-group-subscriber-loc-consumer-group">>,
  bootstrap(DelaySeconds, MessageType, GroupId).

bootstrap(DelaySeconds, MessageType, GroupId) ->
  BootstrapHosts = [{"localhost", 9092}],
  Topic = <<"brod-demo-group-subscriber-loc">>,
  {ok, _} = application:ensure_all_started(brod),
  %% Different members may subscribe to identical or different set of topics.
  %% In the assignments, a member receives only the partitions from the
  %% subscribed topic set.
  TopicSet = [Topic],
  %% In this demo, we spawn two members in the same Erlang node.
  MemberClients = [ 'brod-demo-group-subscriber-loc-client-1'
                  , 'brod-demo-group-subscriber-loc-client-2'
                  ],
  ok = bootstrap_subscribers(MemberClients, BootstrapHosts, GroupId, TopicSet,
                             MessageType),

  %% start one producer process for each partition to feed sequence numbers
  %% to kafka, then consumed by the group subscribers.
  ProducerClientId = ?MODULE,
  ok = brod:start_client(BootstrapHosts, ProducerClientId, client_config()),
  ok = brod:start_producer(ProducerClientId, Topic, _ProducerConfig = []),
  {ok, PartitionCount} = brod:get_partitions_count(ProducerClientId, Topic),
  ok = spawn_producers(ProducerClientId, Topic, DelaySeconds, PartitionCount),
  ok.

%% @doc Initialize nothing in our case.
init(GroupId, MessageType) ->
  OffsetDir = "/tmp",
  {ok, #state{ group_id     = GroupId
             , offset_dir   = OffsetDir
             , message_type = MessageType
             }}.

%% @doc Handle one message (not message-set).
handle_message(Topic, Partition, #kafka_message{} = Message,
               #state{ offset_dir   = Dir
                     , group_id     = GroupId
                     , message_type = message
                     } = State) ->
  ok = process_message(Topic, Partition, Dir, GroupId, Message),
  {ok, ack, State};
handle_message(Topic, Partition, #kafka_message_set{} = MessageSet,
               #state{ offset_dir   = Dir
                     , group_id     = GroupId
                     , message_type = message_set
                     } = State) ->
  #kafka_message_set{messages = Messages} = MessageSet,
  [process_message(Topic, Partition, Dir, GroupId, Msg) || Msg <- Messages],
  {ok, ack, State}.

process_message(Topic, Partition, Dir, GroupId, Message) ->
  %% Process the message synchronously here.
  %% Depending on the use case:
  %% It might be a good idea to spawn worker processes for each partition
  %% for concurrent message processing e.g. see brod_demo_group_subscriber_koc
  %% Or there could be a pool of handlers if the messages can be processed
  %% in arbitrary order.
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  Seqno = list_to_integer(binary_to_list(Value)),
  Now = os_time_utc_str(),
  error_logger:info_msg("~p ~p ~s: offset:~w seqno:~w\n",
                        [self(), Partition, Now, Offset, Seqno]),
  ok = commit_offset(Dir, GroupId, Topic, Partition, Offset).

%% @doc This callback is called whenever there is a new assignment received.
%% e.g. when joining the group after restart, or group assigment rebalance
%% was triggered if other members join or leave the group
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

bootstrap_subscribers([], _BootstrapHosts, _GroupId, _Topics, _MsgType) -> ok;
bootstrap_subscribers([ClientId | Rest], BootstrapHosts, GroupId,
                      Topics, MessageType) ->
  ok = brod:start_client(BootstrapHosts, ClientId, client_config()),
  %% commit offsets to kafka every 5 seconds
  GroupConfig = [{offset_commit_policy, consumer_managed}
                ],
  {ok, _Subscriber} =
    brod:start_link_group_subscriber(
      ClientId, GroupId, Topics, GroupConfig,
      _ConsumerConfig  = [{begin_offset, earliest}],
      MessageType,
      _CallbackModule  = ?MODULE,
      _CallbackInitArg = MessageType),
  bootstrap_subscribers(Rest, BootstrapHosts, GroupId, Topics, MessageType).

filename(Dir, GroupId, Topic, Partition) ->
  filename:join([Dir, GroupId, Topic, integer_to_list(Partition)]).

%% Offsets are committed locally in files for demo.
%% Due to the fact that a partition can be assigned to any group member,
%% in a real use case, when group members are distributed among Erlang nodes
%% (or even hosts), the offsets should be committed to a place where all
%% members have access to. e.g. a database.
commit_offset(Dir, GroupId, Topic, Partition, Offset) ->
  Filename = filename(Dir, GroupId, Topic, Partition),
  ok = filelib:ensure_dir(Filename),
  ok = file:write_file(Filename, [integer_to_list(Offset), $\n]).

spawn_producers(ClientId, Topic, DelaySeconds, P) when is_integer(P) ->
  Partitions = lists:seq(0, P-1),
  spawn_producers(ClientId, Topic, DelaySeconds, Partitions);
spawn_producers(ClientId, Topic, DelaySeconds, [Partition | Partitions]) ->
  erlang:spawn_link(
    fun() ->
      producer_loop(ClientId, Topic, Partition, DelaySeconds, 0)
    end),
  spawn_producers(ClientId, Topic, DelaySeconds, Partitions);
spawn_producers(_ClientId, _Topic, _DelaySeconds, []) -> ok.

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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
