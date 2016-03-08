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

-export([ start_link/5
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
                , consumer_mref   :: reference()
                , begin_offset    :: offset()
                , acked_offset    :: offset()
                }).

-type cursor() :: #cursor{}.

-record(state,
        { client             :: client()
        , controller         :: pid()
        , cursors = []       :: [cursor()]
        , consumer_config    :: consumer_config()
        , is_blocked = false :: boolean()
        }).

%% delay 2 seconds retry the failed subscription to partiton consumer process
-define(RESUBSCRIBE_DELAY, 2000).

-define(LO_CMD_SUBSCRIBE_PARTITIONS, subscribe_partitions).

%%%_* APIs =====================================================================

-spec start_link(client(), group_id(), [topic()],
                 group_config(), consumer_config()) ->
                    {ok, pid()} | {error, any()}.
start_link(Client, GroupId, Topics, GroupConfig, ConsumerConfig) ->
  Args = {Client, GroupId, Topics, GroupConfig, ConsumerConfig},
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

init({Client, GroupId, Topics, GroupConfig, ConsumerConfig}) ->
  {ok, Pid} =
    brod_group_controller:start_link(Client, GroupId, Topics, GroupConfig),
  State = #state{ client          = Client
                , controller      = Pid
                , consumer_config = ConsumerConfig
                },
  {ok, State}.

handle_info(?LO_CMD_SUBSCRIBE_PARTITIONS, State) ->
  NewState =
    case State#state.is_blocked of
      true ->
        State;
      false ->
        {ok, NewState_} = subscribe_partitions(State),
        NewState_
    end,
  _ = send_lo_cmd(?LO_CMD_SUBSCRIBE_PARTITIONS, ?RESUBSCRIBE_DELAY),
  {noreply, NewState};
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
            #state{ cursors = Cursors0
                  } = State) ->
  Cursors =
    lists:map(
      fun(#cursor{ consumer_pid  = ConsumerPid
                 , consumer_mref = ConsumerMref
                 } = Cursor) ->
        _ = brod:unsubscribe(ConsumerPid),
        _ = erlang:demonitor(ConsumerMref, [flush]),
       Cursor#cursor{ consumer_pid  = ?undef
                    , consumer_mref = ?undef
                    }
      end, Cursors0),
  {reply, ok, State#state{ cursors    = Cursors
                         , is_blocked = true
                         }};
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
  Cursors =
    [ #cursor{ topic_partition = {Topic, Partition}
             , consumer_pid    = ?undef
             , begin_offset    = BeginOffset
             , acked_offset    = ?undef
             }
      || {Topic, PartitionAssignments} <- Assignments,
         #partition_assignment{ partition    = Partition
                              , begin_offset = BeginOffset
                              } <- PartitionAssignments
    ],
  _ = send_lo_cmd(?LO_CMD_SUBSCRIBE_PARTITIONS),
  NewState = State#state{ cursors    = Cursors
                        , is_blocked = false
                        },
  {noreply, NewState};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, #state{}) ->
  ok.

%%%_* Internal Functions =======================================================

send_lo_cmd(CMD) -> send_lo_cmd(CMD, 0).

send_lo_cmd(CMD, 0)       -> self() ! CMD;
send_lo_cmd(CMD, DelayMS) -> erlang:send_after(DelayMS, self(), CMD).

subscribe_partitions(#state{ client  = Client
                           , cursors = Cursors0
                           } = State) ->
  Cursors =
    lists:map(
      fun(#cursor{ topic_partition = {Topic, Partition}
                 , consumer_pid    = ConsumerPid0
                 , begin_offset    = BeginOffset0
                 , acked_offset    = AckedOffset
                 } = Cursor) ->
        case is_pid(ConsumerPid0) andalso is_process_alive(ConsumerPid0) of
          true ->
            %% already subscribed, do nothing
            Cursor;
          false ->
            %% fetch from the last commited offset + 1
            %% otherwise fetch from the begin offset
            BeginOffset =
              case AckedOffset of
                ?undef        -> BeginOffset0;
                N when N >= 0 -> N + 1
              end,
            Options = [{begin_offset, BeginOffset}],
            case brod:subscribe(Client, self(), Topic, Partition, Options) of
              {ok, ConsumerPid} ->
                Mref = erlang:monitor(process, ConsumerPid),
                Cursor#cursor{ consumer_pid  = ConsumerPid
                             , consumer_mref = Mref
                             };
              {error, Reason} ->
                Cursor#cursor{ consumer_pid  = {error, Reason}
                             , consumer_mref = ?undef
                             }
            end
        end
      end, Cursors0),
  {ok, State#state{cursors = Cursors}}.

make_metadata() ->
  io_lib:format("~p ~p ~p", [node(), self(), os:timestamp()]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
