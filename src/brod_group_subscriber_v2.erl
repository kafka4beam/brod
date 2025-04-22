%%%
%%%   Copyright (c) 2019-2021 Klarna Bank AB (publ)
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
%%% @doc This module implements an improved version of
%%% `brod_group_subscriber' behavior. Key difference is that each partition
%%% worker runs in a separate Erlang process, allowing parallel message
%%% processing.
%%%
%%% Callbacks are documented in the source code of this module.
%%% @end
%%%=============================================================================
-module(brod_group_subscriber_v2).

-behaviour(gen_server).

-behaviour(brod_group_member).

%% API:
-export([ ack/4
        , commit/4
        , start_link/1
        , stop/1
        , get_workers/1
        ]).

%% brod_group_coordinator callbacks
-export([ get_committed_offsets/2
        , assignments_received/4
        , assignments_revoked/1
        , assign_partitions/3
        ]).

%% gen_server callbacks
-export([ handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-export_type([ init_info/0
             , subscriber_config/0
             , commit_fun/0
             , ack_fun/0
             ]).

-include("brod_int.hrl").

-type subscriber_config() ::
        #{ client          := brod:client()
         , group_id        := brod:group_id()
         , topics          := [brod:topic()]
         , cb_module       := module()
         , init_data       => term()
         , message_type    => message | message_set
         , consumer_config => brod:consumer_config()
         , group_config    => brod:group_config()
         }.

-type commit_fun() :: fun((brod:offset()) -> ok).
-type ack_fun() :: fun((brod:offset()) -> ok).

-type init_info() ::
        #{ group_id     := brod:group_id()
         , topic        := brod:topic()
         , partition    := brod:partition()
         , commit_fun   := commit_fun()
         , ack_fun      := ack_fun()
         }.

-type member_id() :: brod:group_member_id().

-callback init(brod_group_subscriber_v2:init_info(), _CbConfig) ->
  {ok, _State}.

-callback handle_message(brod:message() | brod:message_set(), State) ->
      {ok, commit, State}
    | {ok, ack, State}
    | {ok, State}.

%% Get committed offset (in case it is managed by the subscriber):
-callback get_committed_offset(_CbConfig, brod:topic(), brod:partition()) ->
  {ok, brod:offset() | {begin_offset, brod:offset_time()}} | undefined.

%% Assign partitions (in case `partition_assignment_strategy' is set
%% for `callback_implemented' in group config).
-callback assign_partitions(_CbConfig, [brod:group_member()],
                            [brod:topic_partition()]) ->
  [{member_id(), [brod:partition_assignment()]}].

-callback terminate(_Reason, _State) -> _.

-optional_callbacks([assign_partitions/3, get_committed_offset/3, terminate/2]).

-type worker() :: pid().

-type workers() :: #{brod:topic_partition() => worker()}.

-type committed_offsets() :: #{brod:topic_partition() =>
                                 { brod:offset()
                                 , boolean()
                                 }}.

-record(state,
        { config                  :: subscriber_config()
        , message_type            :: message | message_set
        , group_id                :: brod:group_id()
        , coordinator             :: undefined | pid()
        , generation_id           :: integer() | ?undef
        , workers = #{}           :: workers()
        , committed_offsets = #{} :: committed_offsets()
        , cb_module               :: module()
        , cb_config               :: term()
        , client                  :: brod:client()
        }).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start (link) a group subscriber.
%%
%% Possible `Config' keys:
%%
%% <ul><li> `client': Client ID (or pid, but not recommended) of the
%% brod client. Mandatory</li>
%%
%% <li>`group_id': Consumer group ID which should be unique per kafka
%% cluster. Mandatory</li>
%%
%% <li>`topics': Predefined set of topic names to join the
%% group. Mandatory
%%
%%   NOTE: The group leader member will collect topics from all
%%   members and assign all collected topic-partitions to members in
%%   the group.  i.e. members can join with arbitrary set of
%%   topics.</li>
%%
%% <li>`cb_module': Callback module which should have the callback
%% functions implemented for message processing. Mandatory</li>
%%
%% <li>`group_config': For group coordinator, see {@link
%% brod_group_coordinator:start_link/6} Optional</li>
%%
%% <li>`consumer_config': For partition consumer,
%% {@link brod_consumer:start_link/5}. Optional
%% </li>
%%
%% <li>`message_type': The type of message that is going to be handled
%% by the callback module. Can be either message or message set.
%% Optional, defaults to `message_set'</li>
%%
%% <li>`init_data': The `term()' that is going to be passed to
%% `CbModule:init/2' when initializing the subscriber. Optional,
%% defaults to `undefined'</li>
%% </ul>
%% @end
-spec start_link(subscriber_config()) -> {ok, pid()} | {error, any()}.
start_link(Config) ->
  gen_server:start_link(?MODULE, Config, []).

%% @doc Stop group subscriber, wait for pid `DOWN' before return.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  unlink(Pid),
  exit(Pid, shutdown),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Commit offset for a topic-partition, but don't commit it to
%% Kafka. This is an asynchronous call
-spec ack(pid(), brod:topic(), brod:partition(), brod:offset()) -> ok.
ack(Pid, Topic, Partition, Offset) ->
  gen_server:cast(Pid, {ack_offset, Topic, Partition, Offset}).

%% @doc Ack offset for a topic-partition. This is an asynchronous call
-spec commit(pid(), brod:topic(), brod:partition(), brod:offset()) -> ok.
commit(Pid, Topic, Partition, Offset) ->
  gen_server:cast(Pid, {commit_offset, Topic, Partition, Offset}).

%% @doc Returns a map from Topic-Partitions to worker PIDs for the
%% given group.  Useful for health checking.  This is a synchronous
%% call.
-spec get_workers(pid()) -> workers().
get_workers(Pid) ->
  get_workers(Pid, infinity).

-spec get_workers(pid(), timeout()) -> workers().
get_workers(Pid, Timeout) ->
  gen_server:call(Pid, get_workers, Timeout).

%%%===================================================================
%%% group_coordinator callbacks
%%%===================================================================

%% @doc Called by group coordinator when there is new assignment received.
-spec assignments_received(pid(), member_id(), integer(),
                           brod:received_assignments()) -> ok.
assignments_received(Pid, MemberId, GenerationId, TopicAssignments) ->
  gen_server:cast(Pid, {new_assignments, MemberId,
                        GenerationId, TopicAssignments}).

%% @doc Called by group coordinator before re-joining the consumer group.
-spec assignments_revoked(pid()) -> ok.
assignments_revoked(Pid) ->
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

%% @doc Called by group coordinator when initializing the assignments
%% for subscriber.
%%
%% NOTE: This function is called only when `offset_commit_policy' is set to
%%       `consumer_managed' in group config.
%% @end
-spec get_committed_offsets(pid(), [brod:topic_partition()]) ->
        {ok, [{brod:topic_partition(), brod:offset()}]}.
get_committed_offsets(Pid, TopicPartitions) ->
  gen_server:call(Pid, {get_committed_offsets, TopicPartitions}, infinity).

%% @doc This function is called only when `partition_assignment_strategy'
%% is set for `callback_implemented' in group config.
%% @end
-spec assign_partitions(pid(), [brod:group_member()],
                        [brod:topic_partition()]) ->
        [{member_id(), [brod:partition_assignment()]}].
assign_partitions(Pid, Members, TopicPartitionList) ->
  Call = {assign_partitions, Members, TopicPartitionList},
  gen_server:call(Pid, Call, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initialize the server and start group coordinator
%% @end
%%--------------------------------------------------------------------
-spec init(subscriber_config()) -> {ok, state()}.
init(Config) ->
  #{ client    := Client
   , group_id  := GroupId
   , topics    := Topics
   , cb_module := CbModule
   } = Config,
  process_flag(trap_exit, true),
  MessageType = maps:get(message_type, Config, message_set),
  DefaultGroupConfig = [],
  GroupConfig = maps:get(group_config, Config, DefaultGroupConfig),
  CbConfig = maps:get(init_data, Config, undefined),
  ok = brod_utils:assert_client(Client),
  ok = brod_utils:assert_group_id(GroupId),
  ok = brod_utils:assert_topics(Topics),
  {ok, Pid} = brod_group_coordinator:start_link( Client
                                               , GroupId
                                               , Topics
                                               , GroupConfig
                                               , ?MODULE
                                               , self()
                                               ),
  State = #state{ config       = Config
                , message_type = MessageType
                , client       = Client
                , coordinator  = Pid
                , cb_module    = CbModule
                , cb_config    = CbConfig
                , group_id     = GroupId
                },
  {ok, State}.

handle_call({get_committed_offsets, TopicPartitions}, _From,
            #state{ cb_module = CbModule
                  , cb_config = CbConfig
                  } = State) ->
  Fun = fun(TP = {Topic, Partition}) ->
            case CbModule:get_committed_offset(CbConfig, Topic, Partition) of
              {ok, Offset} ->
                {true, {TP, Offset}};
              undefined ->
                false
            end
        end,
  Result = lists:filtermap(Fun, TopicPartitions),
  {reply, {ok, Result}, State};
handle_call(unsubscribe_all_partitions, _From,
            #state{ workers   = Workers
                  } = State) ->
  terminate_all_workers(Workers),
  {reply, ok, State#state{ workers = #{}
                         }};
handle_call({assign_partitions, Members, TopicPartitionList}, _From, State) ->
  #state{ cb_module = CbModule
        , cb_config = CbConfig
        } = State,
  Reply = CbModule:assign_partitions(CbConfig, Members, TopicPartitionList),
  {reply, Reply, State};
handle_call(get_workers, _From, State = #state{workers = Workers}) ->
  {reply, Workers, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

%% @private
handle_cast({commit_offset, Topic, Partition, Offset}, State) ->
  #state{ coordinator   = Coordinator
        , generation_id = GenerationId
        } = State,
  %% Ask brod_consumer for more data:
  do_ack(Topic, Partition, Offset, State),
  %% Send an async message to group coordinator for offset commit:
  ok = brod_group_coordinator:ack( Coordinator
                                 , GenerationId
                                 , Topic
                                 , Partition
                                 , Offset
                                 ),
  {noreply, State};
handle_cast({ack_offset, Topic, Partition, Offset}, State) ->
  do_ack(Topic, Partition, Offset, State),
  {noreply, State};
handle_cast({new_assignments, MemberId, GenerationId, Assignments},
            #state{ config = Config
                  } = State0) ->
  %% Start worker processes:
  DefaultConsumerConfig = [],
  ConsumerConfig = maps:get( consumer_config
                           , Config
                           , DefaultConsumerConfig
                           ),
  State1 = State0#state{generation_id = GenerationId},
  State = lists:foldl( fun(Assignment, State_) ->
                           #brod_received_assignment
                             { topic        = Topic
                             , partition    = Partition
                             , begin_offset = BeginOffset
                             } = Assignment,
                           maybe_start_worker( MemberId
                                             , ConsumerConfig
                                             , Topic
                                             , Partition
                                             , BeginOffset
                                             , State_
                                             )
                       end
                     , State1
                     , Assignments
                     ),
  {noreply, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, _Reason}, #state{coordinator = Pid} = State) ->
    {stop, {shutdown, coordinator_failure}, State#state{coordinator = undefined}};
handle_info({'EXIT', Pid, Reason}, State) ->
  case [TP || {TP, Pid1} <- maps:to_list(State#state.workers), Pid1 =:= Pid] of
    [TopicPartition | _] ->
      ok = handle_worker_failure(TopicPartition, Pid, Reason, State),
      {stop, shutdown, State};
    _ -> % Other process wants to kill us, supervisor?
      ?BROD_LOG_INFO("Received EXIT:~p from ~p, shutting down", [Reason, Pid]),
      {stop, shutdown, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Terminate all workers and brod consumers
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, #state{config = Config,
                          workers = Workers,
                          coordinator = Coordinator,
                          group_id = GroupId
                         }) ->
  ok = terminate_all_workers(Workers),
  ok = flush_offset_commits(GroupId, Coordinator),
  ok = stop_consumers(Config),
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% best-effort commits flush, this is a synced call,
%% worst case scenario, it may timeout after 5 seconds.
flush_offset_commits(GroupId, Coordinator) when is_pid(Coordinator) ->
    case brod_group_coordinator:commit_offsets(Coordinator) of
        ok -> ok;
        {error, Reason} ->
            ?BROD_LOG_ERROR("group_subscriber_v2 ~s failed to flush commits "
                            "before termination ~p", [GroupId, Reason])
    end;
flush_offset_commits(_, _) ->
    ok.

-spec handle_worker_failure(brod:topic_partition(), pid(), term(), state()) ->
        ok.
handle_worker_failure({Topic, Partition}, Pid, Reason, State) ->
  #state{group_id = GroupId} = State,
  ?BROD_LOG_ERROR("group_subscriber_v2 worker crashed.~n"
                  "  group_id = ~s~n  topic = ~s~n  partition = ~p~n"
                  "  pid = ~p~n  reason = ~p",
                  [GroupId, Topic, Partition, Pid, Reason]),
  ok.

-spec terminate_all_workers(workers()) -> ok.
terminate_all_workers(Workers) ->
  maps:map( fun(_, Worker) ->
                ?BROD_LOG_INFO("Terminating worker pid=~p", [Worker]),
                terminate_worker(Worker)
            end
          , Workers
          ),
  ok.

-spec terminate_worker(worker()) -> ok.
terminate_worker(WorkerPid) ->
  case is_process_alive(WorkerPid) of
    true ->
      unlink(WorkerPid),
      brod_topic_subscriber:stop(WorkerPid);
    false ->
      ok
  end.

-spec maybe_start_worker( member_id()
                        , brod:consumer_config()
                        , brod:topic()
                        , brod:partition()
                        , brod:offset() | undefined
                        , state()
                        ) -> state().
maybe_start_worker( _MemberId
                  , ConsumerConfig
                  , Topic
                  , Partition
                  , BeginOffset
                  , State
                  ) ->
  #state{ workers      = Workers
        , client       = Client
        , cb_module    = CbModule
        , cb_config    = CbConfig
        , group_id     = GroupId
        , message_type = MessageType
        } = State,
  TopicPartition = {Topic, Partition},
  case Workers of
    #{TopicPartition := _Worker} ->
      State;
    _ ->
      Self = self(),
      CommitFun = fun(Offset) -> commit(Self, Topic, Partition, Offset) end,
      AckFun = fun(Offset) -> ack(Self, Topic, Partition, Offset) end,
      StartOptions = #{ cb_module    => CbModule
                      , cb_config    => CbConfig
                      , partition    => Partition
                      , begin_offset => BeginOffset
                      , group_id     => GroupId
                      , commit_fun   => CommitFun
                      , ack_fun      => AckFun
                      , topic        => Topic
                      },
      {ok, Pid} = start_worker( Client
                              , Topic
                              , MessageType
                              , Partition
                              , ConsumerConfig
                              , StartOptions
                              ),
      NewWorkers = Workers #{TopicPartition => Pid},
      State#state{workers = NewWorkers}
  end.

-spec start_worker( brod:client()
                  , brod:topic()
                  , message | message_set
                  , brod:partition()
                  , brod:consumer_config()
                  , brod_group_subscriber_worker:start_options()
                  ) -> {ok, pid()}.
start_worker(Client, Topic, MessageType, Partition, ConsumerConfig,
             StartOptions) ->
  {ok, Pid} = brod_topic_subscriber:start_link( Client
                                              , Topic
                                              , [Partition]
                                              , ConsumerConfig
                                              , MessageType
                                              , brod_group_subscriber_worker
                                              , StartOptions
                                              ),
  {ok, Pid}.

-spec do_ack(brod:topic(), brod:partition(), brod:offset(), state()) ->
                ok | {error, term()}.
do_ack(Topic, Partition, Offset, #state{ workers = Workers
                                       }) ->
  TopicPartition = {Topic, Partition},
  case Workers of
    #{TopicPartition := Pid} ->
      brod_topic_subscriber:ack(Pid, Partition, Offset),
      ok;
    _ ->
      {error, unknown_topic_or_partition}
  end.

stop_consumers(Config) ->
  #{ client := Client
   , topics := Topics
   } = Config,
  lists:foreach(
    fun(Topic) ->
        _ = brod_client:stop_consumer(Client, Topic)
    end,
    Topics).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
