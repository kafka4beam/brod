%%%
%%%   Copyright (c) 2019 Klarna Bank AB (publ)
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

-module(brod_group_subscriber_v2).

-behaviour(gen_server).

-behaviour(brod_group_member).

%% API:
-export([ ack/4
        , ack/5
        , commit/1
        , commit/4
        , start_link/7
        , start_link/8
        , stop/1
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

-include("brod_int.hrl").

-type subscriber_config() ::
        #{ client          := brod:client()
         , group_id        := brod:group_id()
         , topics          := [bord:topic()]
         , callback_module := module()
         , callback_config => term()
         , message_type    => message | message_set
         , consumer_config => brod:consumer_config()
         , group_config    => brod:group_config()
         }.

-type member_id() :: brod:group_member_id().

%% Start a partition worker
-callback start_worker( brod:group_id()
                      , brod:topic()
                      , brod:partition()
                      , _Configuration
                      , _ConsumerPid :: pid()
                      ) -> {ok, pid()}.

%% Stop a partition worker when the partition assignment is revoked
-callback stop_worker(pid()) -> term().

%% Get committed offset (in case it is managed by the subscriber):
-callback get_committed_offset(pid()) -> brod:offset() | undefined.

-define(DOWN(Reason), {down, brod_utils:os_time_utc_str(), Reason}).

-record(worker,
        { worker_pid      :: pid()
        , worker_mref     :: reference()
        , consumer_pid    :: pid()
        , consumer_mref   :: ?reference()
        }).

-type worker() :: #worker{}.

-type ack_ref() :: {brod:topic(), brod:partition(), brod:offset()}.

-type topic_partition() :: {brod:topic(), brod:partition()}.

-type workers() :: #{topic_partition() => workers()}.

-type committed_offsets() :: #{topic_partition() => { brod:offset()
                                                    , boolean()
                                                    }}.

-record(state,
        { config                  :: consumer_config()
        , coordinator             :: pid()
        , generation_id           :: integer() | ?undef
        , workers = #{}           :: workers()
        , committed_offsets = #{} :: committed_offsets()
        , cb_module               :: module()
        , client                  :: brod:client()
        }).

-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start (link) a group subscriber.
%% Client:
%%   Client ID (or pid, but not recommended) of the brod client.
%% GroupId:
%%   Consumer group ID which should be unique per kafka cluster
%% Topics:
%%   Predefined set of topic names to join the group.
%%   NOTE: The group leader member will collect topics from all members and
%%         assign all collected topic-partitions to members in the group.
%%         i.e. members can join with arbitrary set of topics.
%% GroupConfig:
%%   For group coordinator, @see brod_group_coordinator:start_link/5
%% ConsumerConfig:
%%   For partition consumer, @see brod_consumer:start_link/4
%% MessageType:
%%   The type of message that is going to be handled by the callback
%%   module. Can be either message or message set.
%% CbModule:
%%   Callback module which should have the callback functions
%%   implemented for message processing.
%% CbInitArg:
%%   The term() that is going to be passed to CbModule:init/1 when
%%   initializing the subscriger.
%% @end
-spec start_link(subscriber_config()) -> {ok, pid()} | {error, any()}.
start_link(Config) ->
  gen_server:start_link(?MODULE, Config, []).

%% @doc Stop group subscriber, wait for pid DOWN before return.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

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
%%
%% NOTE: The committed offsets should be the offsets for successfully processed
%%       (acknowledged) messages, not the `begin_offset' to start fetching from.
%% @end
-spec get_committed_offsets(pid(), [topic_partition()]) ->
        {ok, [{topic_partition(), brod:offset()}]}.
get_committed_offsets(Pid, TopicPartitions) ->
  gen_server:call(Pid, {get_committed_offsets, TopicPartitions}, infinity).

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
  process_flag(trap_exit, true),
  #{ client    := Client
   , group_id  := GroupId
   , topics    := Topics
   , cb_module := CbModule
   } = Config,
  DefaultGroupConfig = [],
  GroupConfig = maps:get(group_config, Config, DefaultGroupConfig),
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
  State = #state{ config      = Config
                , client      = Client
                , coordinator = Pid
                , cb_module   = CbModule
                },
  {ok, State}.

handle_call({get_committed_offsets, TopicPartitions}, _From,
            #state{ cb_module = CbModule
                  , workers   = Workers
                  } = State) ->
  Fun = fun(TopicPartition) ->
            case Workers of
              #{TopicPartition := #worker{worker_pid = Pid}} ->
                case CbModule:get_committed_offset(Pid) of
                  Offset when is_integer(Offset) ->
                    {true, {TopicPartition, Offset}};
                  undefined ->
                    false
                end;
              _ ->
                false
            end
        end,
  Result = lists:filtermap(Fun, TopicPartitions),
  {reply, Result, State};
handle_call(unsubscribe_all_partitions, _From,
            #state{ workers   = Workers
                  , cb_module = CbModule
                  } = State) ->
  lists:foreach(
    fun(#worker{ worker_pid    = WorkerPid
               , worker_mref   = WorkerMref
               , consumer_pid  = ConsumerPid
               , consumer_mref = ConsumerMref
               }) ->
        _ = brod:unsubscribe(ConsumerPid, self()),
        _ = erlang:demonitor(ConsumerMref, [flush]),
        _ = CbModule:stop_worker(WorkerPid),
        _ = erlang:demonitor(WorkerMref, [flush])
    end, Consumers),
  {reply, ok, State#state{ workers = #{}
                         }};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

%% @private
handle_cast({new_assignments, MemberId, GenerationId, Assignments},
            #state{ client          = Client
                  , config          = Config
                  , subscribe_tref  = Tref
                  } = State) ->
  DefaultConsumerConfig = [],
  ConsumerConfig = maps:get( consumer_config
                           , Config
                           , DefaultConsumerConfig
                           ),
  NewWorkers = lists:filtermap( fun(I) ->
                                    maybe_start_worker( State
                                                      , MemberId
                                                      , GenerationId
                                                      , I
                                                      )
                                end
                              , Assignments
                              ),
  AllTopics =
    lists:map(fun(#brod_received_assignment{topic = Topic}) ->
                Topic
              end, Assignments),
  lists:foreach(
    fun(Topic) ->
      ok = brod:start_consumer(Client, Topic, ConsumerConfig)
    end, lists:usort(AllTopics)),
  Consumers =
    [ #consumer{ topic_partition = {Topic, Partition}
               , consumer_pid    = ?undef
               , begin_offset    = BeginOffset
               , acked_offset    = ?undef
               }
    || #brod_received_assignment{ topic        = Topic
                                , partition    = Partition
                                , begin_offset = BeginOffset
                                } <- Assignments
    ],
  NewState = State#state{ consumers      = Consumers
                        , is_blocked     = false
                        , memberId       = MemberId
                        , generationId   = GenerationId
                        , subscribe_tref = start_subscribe_timer(Tref, 0)
                        },
  {noreply, NewState};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
                     {noreply, NewState :: term()} |
                     {noreply, NewState :: term(), Timeout :: timeout()} |
                     {noreply, NewState :: term(), hibernate} |
                     {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: term(),
                  Extra :: term()) -> {ok, NewState :: term()} |
                                      {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
                    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
  Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
