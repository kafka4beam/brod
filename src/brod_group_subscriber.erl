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
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_group_subscriber).
-behaviour(gen_server).

-export([ start_link/4
        ]).

%% callbacks for brod_group_controller
-export([ get_offsets_to_commit/1
        , new_assignments/2
        , unsubscribe_all_partitions/1
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-record(cursor, { topic_partition :: {topic(), partition()}
                , consumer_pid    :: pid()
                , begin_offset    :: offset()
                , acked_offset    :: offset()
                }).

-type cursor() :: #cursor{}.

-record(state,
        { client          :: client()
        , controller      :: pid()
        , assignments     :: [topic_assignment()]
        , cursors = []    :: [cursor()]
        , consumer_config :: consumer_config()
        }).

%%%_* APIs =====================================================================

-spec start_link(client(), group_id(), group_config(), consumer_config()) ->
        {ok, pid()} | {error, any()}.
start_link(Client, GroupId, GroupConfig, ConsumerConfig) ->
  Args = {Client, GroupId, GroupConfig, ConsumerConfig},
  gen_server:start_link(?MODULE, Args, []).

-spec get_offsets_to_commit(pid()) -> ok.
get_offsets_to_commit(Pid) ->
  gen_server:call(Pid, get_offsets_to_commit, infinity).

-spec new_assignments(pid(), [topic_assignment()]) -> ok.
new_assignments(Pid, TopicAssignments) ->
  gen_server:cast(Pid, {new_assignments, TopicAssignments}).

-spec unsubscribe_all_partitions(pid()) -> ok.
unsubscribe_all_partitions(Pid) ->
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

%%%_* gen_server callbacks =====================================================

init({Client, GroupId, Topic, GroupConfig, ConsumerConfig}) ->
  {ok, Pid} =
    brod_group_controller:start_link(Client, GroupId, Topic, GroupConfig),
  State = #state{ client          = Client
                , controller      = Pid
                , consumer_config = ConsumerConfig
                },
  {ok, State}.

handle_info(_Info, State) ->
  {noreply, State}.

handle_call(get_offsets_to_commit, _From,
            #state{cursors = Cursors} = State) ->
  TopicOffsets0 =
    lists:foldl(
      fun(#cursor{ topic_partition = {Topic, Partition}
                 , acked_offset    = Offset
                 }, Acc) ->
        case is_integer(Offset) andalso Offset >= 0 of
          true ->
            PartitionOffset =
              #kpro_OCReqV2Partition{ partition = Partition
                                    , offset    = Offset
                                    , metadata  = make_metadata()
                                    },
            orddict:append_list(Topic, [PartitionOffset], Acc);
          false ->
            Acc
        end
      end, [], Cursors),
  TopicOffsets =
    lists:map(
      fun({Topic, PartitionOffsets}) ->
        #kpro_OCReqV2Topic{ topicName          = Topic
                          , oCReqV2Partition_L = PartitionOffsets
                          }
      end, TopicOffsets0),
  {reply, TopicOffsets, State};
handle_call(unsubscribe_all_partitions, _From,
            #state{ client      = Client
                  , assignments = Assignments} = State) ->
  lists:foreach(fun({Topic, _PartitionAssignments}) ->
                  brod_client:stop_consumer(Client, Topic)
                end, Assignments),
  {reply, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({new_assignments, Assignments},
            #state{ client = Client
                  , consumer_config = ConsumerConfig
                  } = State) ->
  lists:foreach(
    fun({Topic, _PartitionAssignments}) ->
      ok = brod_client:start_consumer(Client, Topic, ConsumerConfig)
    end, Assignments),
  PendingAssignments =
    [ {Topic, Partition, BeginOffset}
      || {Topic, PartitionAssignments} <- Assignments,
         #partition_assignment{ partition = Partition
                              , begin_offset = BeginOffset
                              } <- PartitionAssignments
    ],
  Cursors =
    lists:map(
      fun({Topic, Partition, BeginOffset}) ->
        Options = [{begin_offset, BeginOffset}],
        case brod:subscribe(Client, self(), Topic, Partition, Options) of
          {ok, Pid} ->
            #cursor{ topic_partition = {Topic, Partition}
                   , consumer_pid    = Pid
                   , begin_offset    = BeginOffset
                   , acked_offset    = ?undef
                   };
          {error, Reason} ->
            error_logger:error_msg(
              "error when subscribing to topic ~s pattition ~p\nreason:~p",
              [Topic, Partition, Reason]),
            #cursor{ topic_partition = {Topic, Partition}
                   , consumer_pid    = ?undef
                   , begin_offset    = BeginOffset
                   , acked_offset    = ?undef
                   }
        end
      end, PendingAssignments),
  NewState =
    State#state{ assignments = Assignments
               , cursors     = Cursors
               },
  {noreply, NewState};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, #state{}) ->
  ok.

%%%_* Internal Functions =======================================================

make_metadata() ->
  io_lib:format("~p ~p ~p", [node(), self(), os:timestamp()]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
