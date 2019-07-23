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
        , get_committed_offset/1
        , handle_message/2
        ]).

init(InitInfo, Config) ->
  #{topic := Topic, partition := Partition} = InitInfo,
  {CaseRef, CasePid, IsAsyncAck, IsAsyncCommit, IsAssignPartitions} = Config,
  brod_utils:log(info, "Started a test group subscriber.~n"
                       "Config: ~p~nInitInfo: ~p~n"
                     , [Config, InitInfo]),
  {ok, #state{ ct_case_ref          = CaseRef
             , ct_case_pid          = CasePid
             , is_async_ack         = IsAsyncAck
             , is_async_commit      = IsAsyncCommit
             , is_assign_partitions = IsAssignPartitions
             , topic                = Topic
             , partition            = Partition
             }}.

handle_message(Message,
               #state{ ct_case_ref          = Ref
                     , ct_case_pid          = Pid
                     , is_async_ack         = IsAsyncAck
                     , is_async_commit      = IsAsyncCommit
                     , is_assign_partitions = IsAssignPartitions
                     , topic                = Topic
                     , partition            = Partition
                     } = State) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  %% forward the message to ct case for verification.
  Pid ! ?MSG(Ref, self(), Topic, Partition, Offset, Value),
  case {IsAsyncAck, IsAsyncCommit, IsAssignPartitions} of
    {true, _, _}      -> {ok, State};
    {false, false, _} -> {ok, commit, State};
    {false, true, _}  -> {ok, ack, State}
  end.

get_committed_offset(_State) ->
  %% always return undefined: always fetch from latest available offset
  undefined.
