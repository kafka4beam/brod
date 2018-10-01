%%%
%%%   Copyright (c) 2017-2018 Klarna Bank AB (publ)
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
%%% @doc This is a utility module to help force commit offsets to kafka.
%%%=============================================================================

-module(brod_cg_commits).

-behaviour(gen_server).
-behaviour(brod_group_member).

-export([ run/2
        ]).

-export([ start_link/2
        , stop/1
        , sync/1
        ]).

%% callbacks for brod_group_coordinator
-export([ get_committed_offsets/2
        , assignments_received/4
        , assignments_revoked/1
        , assign_partitions/3
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type group_id() :: brod:group_id().
-type member_id() :: brod:group_member_id().
-type retention() :: integer(). %% -1 to use whatever configured in kafka
-type offsets() :: latest | earliest | [{partition(), offset()}].
-type prop_key() :: id | topic | retention | protocol | offsets.
-type prop_val() :: group_id() | topic() | retention() | offsets()
                  | brod_group_coordinator:protocol_name().
-type group_input() :: [{prop_key(), prop_val()}].

-record(state,
        { client             :: brod:client()
        , groupId            :: brod:group_id()
        , memberId           :: ?undef | member_id()
        , generationId       :: ?undef | brod:group_generation_id()
        , coordinator        :: pid()
        , topic              :: ?undef | topic()
        , offsets            :: ?undef | offsets()
        , is_elected = false :: boolean()
        , pending_sync       :: ?undef | {pid(), reference()}
        , is_done = false    :: boolean()
        }).

%%%_* APIs =====================================================================

%% @doc Force commit offsets.
run(ClientId, GroupInput) ->
  {ok, Pid} = start_link(ClientId, GroupInput),
  ok = sync(Pid),
  ok = stop(Pid).

%% @doc Start (link) a group member.
%% The member will try to join the consumer group and
%% get assignments for the given topic-partitions,
%% then commit given offsets to kafka.
%% In case not all given partitions are assigned to it,
%% it will terminate with an exit exception
-spec start_link(brod:client(), group_input()) -> {ok, pid()} | {error, any()}.
start_link(Client, GroupInput) ->
  gen_server:start_link(?MODULE, {Client, GroupInput}, []).

%% @doc Stop the process.
-spec stop(pid()) -> ok.
stop(Pid) ->
  Mref = erlang:monitor(process, Pid),
  ok = gen_server:cast(Pid, stop),
  receive
    {'DOWN', Mref, process, Pid, _Reason} ->
      ok
  end.

%% @doc Make a call to the resetter process, the call will be blocked
%% until offsets are committed.
-spec sync(pid()) -> ok.
sync(Pid) ->
  ok = gen_server:call(Pid, sync, infinity).

%%%_* APIs for group coordinator ===============================================

%% @doc Called by group coordinator when there is new assignemnt received.
-spec assignments_received(pid(), member_id(), integer(),
                           brod:received_assignments()) -> ok.
assignments_received(Pid, MemberId, GenerationId, TopicAssignments) ->
  gen_server:cast(Pid, {new_assignments, MemberId,
                        GenerationId, TopicAssignments}).

%% @doc Called by group coordinator before re-joinning the consumer group.
-spec assignments_revoked(pid()) -> ok.
assignments_revoked(Pid) ->
  gen_server:call(Pid, unsubscribe_all_partitions, infinity).

%% @doc This function is called only when `partition_assignment_strategy'
%% is set for `callback_implemented' in group config.
-spec assign_partitions(pid(), [brod:group_member()],
                        [{brod:topic(), brod:partition()}]) ->
        [{member_id(), [brod:partition_assignment()]}].
assign_partitions(Pid, Members, TopicPartitionList) ->
  Call = {assign_partitions, Members, TopicPartitionList},
  gen_server:call(Pid, Call, infinity).

%% @doc Called by group coordinator when initializing the assignments
%% for subscriber.
%% NOTE: this function is called only when it is DISABLED to commit offsets
%%       to kafka. i.e. offset_commit_policy is set to consumer_managed
-spec get_committed_offsets(pid(), [{brod:topic(), brod:partition()}]) ->
        {ok, [{{brod:topic(), brod:partition()}, brod:offset()}]}.
get_committed_offsets(_Pid, _TopicPartitions) -> {ok, []}.

%%%_* gen_server callbacks =====================================================

init({Client, GroupInput}) ->
  ok = brod_utils:assert_client(Client),
  GroupId = proplists:get_value(id, GroupInput),
  ok = brod_utils:assert_group_id(GroupId),
  Topic = proplists:get_value(topic, GroupInput),
  ProtocolName = proplists:get_value(protocol, GroupInput),
  Retention = proplists:get_value(retention, GroupInput),
  Offsets = proplists:get_value(offsets, GroupInput),
  %% use callback_implemented strategy so I know I am elected leader
  %% when `assign_partitions' callback is called.
  Config = [ {partition_assignment_strategy, callback_implemented}
           , {offset_retention_seconds, Retention}
           , {protocol_name, ProtocolName}
           , {rejoin_delay_seconds, 2}
           ],
  {ok, Pid} = brod_group_coordinator:start_link(Client, GroupId, [Topic],
                                                Config, ?MODULE, self()),
  State = #state{ client      = Client
                , groupId     = GroupId
                , coordinator = Pid
                , topic       = Topic
                , offsets     = Offsets
                },
  {ok, State}.

handle_info(Info, State) ->
  log(State, info, "Info discarded:~p", [Info]),
  {noreply, State}.

handle_call(sync, From, State0) ->
  State1 = State0#state{pending_sync = From},
  State = maybe_reply_sync(State1),
  {noreply, State};
handle_call({assign_partitions, Members, TopicPartitions}, _From,
            #state{topic = MyTopic,
                   offsets = Offsets} = State) ->
  log(State, info, "Assigning all topic partitions to self", []),
  MyTP = [{MyTopic, P} || {P, _} <- Offsets],
  %% Assert that my topic partitions are included in
  %% subscriptions collected from ALL members
  Pred = fun(TP) -> not lists:member(TP, TopicPartitions) end,
  case lists:filter(Pred, MyTP) of
    [] ->
      %% all of the give partitions are valid
      ok;
    BadPartitions ->
      PartitionNumbers = [P || {_T, P} <- BadPartitions],
      log(State, error, "Nonexisting partitions in input: ~p",
          [PartitionNumbers]),
      erlang:exit({non_existing_partitions, PartitionNumbers})
  end,
  %% To keep it simple, assign all my topic-partitions to self
  %% but discard all other topic-partitions.
  %% After all, I will leave group as soon as offsets are committed
  Result = assign_all_to_self(Members, MyTP),
  {reply, Result, State#state{is_elected = true}};
handle_call(unsubscribe_all_partitions, _From, #state{} = State) ->
  %% nothing to do, because I do not subscribe to any partition
  {reply, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({new_assignments, _MemberId, GenerationId, Assignments},
            #state{ is_elected = IsLeader
                  , offsets = OffsetsToCommit
                  , coordinator = Pid
                  , topic =  MyTopic
                  } = State) ->
  %% Write a log if I am not a leader,
  %% hope the desired partitions are all assigned to me
  IsLeader orelse log(State, info, "Not elected", []),
  Groupped0 =
    brod_utils:group_per_key(
      fun(#brod_received_assignment{ topic        = Topic
                                   , partition    = Partition
                                   , begin_offset = Offset
                                   }) ->
          {Topic, {Partition, Offset}}
      end, Assignments),
  %% Discard other topics if for whatever reason the group leader assigns
  %% irrelevant topic-partitions to me
  Groupped = lists:filter(fun({Topic, _}) -> Topic =:= MyTopic end, Groupped0),
  log(State, info, "current offsets:\n~p", [Groupped]),
  %% Assert all desired partitions are in assignment
  case Groupped of
    [] ->
      log(State, error, "Topic ~s is not received in assignment", [MyTopic]),
      erlang:exit({bad_topic_assignment, Groupped0});
    [{MyTopic, PartitionOffsetList}] ->
      MyPartitions = [P || {P, _O} <- OffsetsToCommit],
      ReceivedPartitions = [P || {P, _O} <- PartitionOffsetList],
      case MyPartitions -- ReceivedPartitions of
        [] ->
          ok;
        Left ->
          log(State, error,
              "Partitions ~p are not received in assignment, "
              "There is probably another active group member subscribing "
              "to topic ~s, stop it and retry\n", [MyTopic, Left]),
          erlang:exit({unexpected_assignments, Left})
      end
  end,
  %% Stage all offsets in coordinator process
  lists:foreach(
    fun({Partition, Offset}) ->
        %% -1 here, because brod_group_coordinator +1 to commit
        OffsetToCommit = Offset - 1,
        brod_group_coordinator:ack(Pid, GenerationId, MyTopic,
                                   Partition, OffsetToCommit)
    end, OffsetsToCommit),
  %% Now force it to commit
  case brod_group_coordinator:commit_offsets(Pid) of
    ok -> ok;
    {error, Reason} ->
      log(State, error, "Failed to commit, reason:\n~p", [Reason]),
      erlang:exit(commit_failed)
  end,
  {noreply, set_done(State)};
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, #state{}) ->
  ok.

%%%_* Internal Functions =======================================================

set_done(State) ->
  maybe_reply_sync(State#state{is_done = true}).

maybe_reply_sync(#state{is_done = false} = State) ->
  State;
maybe_reply_sync(#state{pending_sync = ?undef} = State) ->
  State;
maybe_reply_sync(#state{pending_sync = From} = State) ->
  gen_server:reply(From, ok),
  log(State, info, "done\n", []),
  State#state{pending_sync = ?undef}.

%% I am the current leader because I am assigning partitions.
%% My member ID should be positioned at the head of the member list.
-spec assign_all_to_self([brod:group_member()], [{topic(), partition()}]) ->
        [{member_id(), [brod:partition_assignment()]}].
assign_all_to_self([{MyMemberId, _} | Members], TopicPartitions) ->
  Groupped = brod_utils:group_per_key(TopicPartitions),
  [ {MyMemberId, Groupped}
  | [{Id, []} || {Id, _MemberMeta} <- Members]
  ].

log(#state{groupId  = GroupId}, Level, Fmt, Args) ->
  brod_utils:log(Level,
                 "Group member (~s,coor=~p):\n" ++ Fmt,
                 [GroupId, self() | Args]).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
