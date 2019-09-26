%%%
%%%   Copyright (c) 2015-2018 Klarna Bank AB (publ)
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

%% @private
-module(brod_test_group_subscriber).

-behavior(brod_group_subscriber_v2).

-include("brod.hrl").
-include("brod_group_subscriber_test.hrl").

%% brod subscriber callbacks
-export([ init/2
        , get_committed_offset/3
        , handle_message/2
        , assign_partitions/3
        ]).

init(InitInfo, Config) ->
  #{topic := Topic, partition := Partition} = InitInfo,
  IsAsyncAck         = maps:get(async_ack, Config, false),
  IsAsyncCommit      = maps:get(async_commit, Config, false),
  IsAssignPartitions = maps:get(assign_partitions, Config, false),
  brod_utils:log(info, "Started a test group subscriber.~n"
                       "Config: ~p~nInitInfo: ~p~n"
                     , [Config, InitInfo]),
  {ok, #state{ is_async_ack         = IsAsyncAck
             , is_async_commit      = IsAsyncCommit
             , is_assign_partitions = IsAssignPartitions
             , topic                = Topic
             , partition            = Partition
             }}.

handle_message(Message,
               #state{ is_async_ack         = IsAsyncAck
                     , is_async_commit      = IsAsyncCommit
                     , topic                = Topic
                     , partition            = Partition
                     } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  ?tp(group_subscriber_handle_message,
      #{ topic     => Topic
       , partition => Partition
       , offset    => Offset
       , value     => Value
       , worker    => self()
       }),
  case {IsAsyncAck, IsAsyncCommit} of
    {true,  _}     -> {ok, State};
    {false, false} -> {ok, commit, State};
    {false, true}  -> {ok, ack, State}
  end.

get_committed_offset(_CbConfig, _Topic, _Partition) ->
  %% always return undefined: always fetch from latest available offset
  undefined.

assign_partitions(_CbConfig, Members, TopicPartitions) ->
  PartitionsAssignments = [{Topic, [PartitionsN]}
                           || {Topic, PartitionsN} <- TopicPartitions],
  [{element(1, hd(Members)), PartitionsAssignments}].
