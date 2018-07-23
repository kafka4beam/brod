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
%%% This is called 'koc' as in kafka-offset-commit,
%%% it demos a all-configs-by-default minimal implenemtation of a
%%% consumer group subscriber which commits offsets to kafka.
%%% See bootstrap/0 for more details about all prerequisite.
%%% @end
%%%=============================================================================

-module(brod_demo_group_subscriber_koc).

-behaviour(brod_group_subscriber).

-export([ bootstrap/0
        , bootstrap/1
        , bootstrap/2
        ]).

%% behabviour callbacks
-export([ init/2
        , handle_message/4
        ]).

-export([ message_handler_loop/3 ]).

-include("brod.hrl").

-define(PRODUCE_DELAY_SECONDS, 5).

-record(callback_state,
        { handlers     = []      :: [{{brod:topic(), brod:partition()}, pid()}]
        , message_type = message :: message | message_set
        }).

%% @doc This function bootstraps everything to demo group subscribers.
%% Prerequisites:
%%   - bootstrap docker host at {"localhost", 9092}
%%   - kafka topic named <<"brod-demo-group-subscriber-koc">>
%% Processes to spawn:
%%   - A brod client
%%   - A producer which produces sequence numbers to each partition
%%   - X group subscribers, X is the number of partitions
%%
%% * consumed sequence numbers are printed to console
%% * consumed offsets are commited to kafka (using v2 commit requests)
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
-spec bootstrap() -> ok.
bootstrap() ->
  bootstrap(?PRODUCE_DELAY_SECONDS).

bootstrap(DelaySeconds) ->
  bootstrap(DelaySeconds, message).

bootstrap(DelaySeconds, MessageType) ->
  BootstrapHosts = [{"localhost", 9092}],
  Topic = <<"brod-demo-group-subscriber-koc">>,
  {ok, _} = application:ensure_all_started(brod),

  %% A group ID is to be shared between the members (which often run in
  %% different Erlang nodes or even hosts).
  GroupId = <<"brod-demo-group-subscriber-koc-consumer-group">>,
  %% Different members may subscribe to identical or different set of topics.
  %% In the assignments, a member receives only the partitions from the
  %% subscribed topic set.
  TopicSet = [Topic],
  %% In this demo, we spawn two members in the same Erlang node.
  MemberClients = [ 'brod-demo-group-subscriber-koc-client-1'
                  , 'brod-demo-group-subscriber-koc-client-2'
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
init(_GroupId, _CallbackInitArg = {ClientId, Topics, MessageType}) ->
  %% For demo, spawn one message handler per topic-partition.
  %% Depending on the use case:
  %% It might be enough to handle the message locally in the subscriber
  %% pid without dispatching to handlers. (e.g. brod_demo_group_subscriber_loc)
  %% Or there could be a pool of handlers if the messages can be processed
  %% in arbitrary order.
  Handlers = spawn_message_handlers(ClientId, Topics),
  {ok, #callback_state{handlers = Handlers, message_type = MessageType}}.

%% @doc Handle one message or a message-set.
handle_message(Topic, Partition,
               #kafka_message{} = Message,
               #callback_state{ handlers = Handlers
                              , message_type = message
                              } = State) ->
  process_message(Topic, Partition, Handlers, Message),
  %% or return {ok, ack, State} in case the message can be handled
  %% synchronously here without dispatching to a worker
  {ok, State};
handle_message(Topic, Partition,
               #kafka_message_set{messages = Messages} = _MessageSet,
               #callback_state{ handlers = Handlers
                              , message_type = message_set
                              } = State) ->
  [process_message(Topic, Partition, Handlers, Message) || Message <- Messages],
  {ok, State}.

%%%_* Internal Functions =======================================================

process_message(Topic, Partition, Handlers, Message) ->
  %% send to a worker process
  {_, Pid} = lists:keyfind({Topic, Partition}, 1, Handlers),
  Pid ! Message.

bootstrap_subscribers([], _BootstrapHosts, _GroupId, _Topics, _MsgType) -> ok;
bootstrap_subscribers([ClientId | Rest], BootstrapHosts, GroupId,
                      Topics, MessageType) ->
  ok = brod:start_client(BootstrapHosts, ClientId, client_config()),
  %% commit offsets to kafka every 5 seconds
  GroupConfig = [{offset_commit_policy, commit_to_kafka_v2}
                ,{offset_commit_interval_seconds, 1}
                ],
  {ok, _Subscriber} =
    brod:start_link_group_subscriber(
      ClientId, GroupId, Topics, GroupConfig,
      _ConsumerConfig  = [{begin_offset, earliest}], MessageType,
      _CallbackModule  = ?MODULE,
      _CallbackInitArg = {ClientId, Topics, MessageType}),
  bootstrap_subscribers(Rest, BootstrapHosts, GroupId, Topics, MessageType).

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

%% Spawn one message handler per partition. Some of them may sit
%% idle if the partition is assigned to another group member.
%% Perhaps hibernate if idle for certain minutes.
%% Or even spawn dynamically in `handle_message` callback and
%% `exit(normal)` when idle for long.
-spec spawn_message_handlers(brod:client_id(), [brod:topic()]) ->
        [{{brod:topic(), brod:partition()}, pid()}].
spawn_message_handlers(_ClientId, []) -> [];
spawn_message_handlers(ClientId, [Topic | Rest]) ->
  {ok, PartitionCount} = brod:get_partitions_count(ClientId, Topic),
  [{{Topic, Partition},
    spawn_link(?MODULE, message_handler_loop, [Topic, Partition, self()])}
   || Partition <- lists:seq(0, PartitionCount-1)] ++
  spawn_message_handlers(ClientId, Rest).

message_handler_loop(Topic, Partition, SubscriberPid) ->
  receive
    #kafka_message{ offset = Offset
                  , value  = Value
                  } ->
    Seqno = list_to_integer(binary_to_list(Value)),
    Now = os_time_utc_str(),
    error_logger:info_msg("~p ~s-~p ~s: offset:~w seqno:~w\n",
                          [self(), Topic, Partition, Now, Offset, Seqno]),
    brod_group_subscriber:ack(SubscriberPid, Topic, Partition, Offset),
    ?MODULE:message_handler_loop(Topic, Partition, SubscriberPid)
  after 1000 ->
    ?MODULE:message_handler_loop(Topic, Partition, SubscriberPid)
  end.

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
