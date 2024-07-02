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

%%% @private
-module(brod_group_subscriber_worker).

-behaviour(brod_topic_subscriber).

-include("brod_int.hrl").

%% brod_topic_subscriber callbacks
-export([init/2, handle_message/3, handle_info/2, terminate/2]).

-type start_options() ::
        #{ group_id     := brod:group_id()
         , topic        := brod:topic()
         , partition    := brod:partition()
         , begin_offset := brod:offset() | ?undef
         , cb_module    := module()
         , cb_config    := term()
         , commit_fun   := brod_group_subscriber_v2:commit_fun()
         , ack_fun      := brod_group_subscriber_v2:ack_fun()
         }.

-record(state,
        { start_options :: start_options()
        , cb_module     :: module()
        , cb_state      :: term()
        , commit_fun    :: brod_group_subscriber_v2:commit_fun()
        }).

-export_type([start_options/0]).

%%%===================================================================
%%% brod_topic_subscriber callbacks
%%%===================================================================

init(Topic, StartOpts) ->
  #{ cb_module    := CbModule
   , cb_config    := CbConfig
   , partition    := Partition
   , begin_offset := BeginOffset
   , commit_fun   := CommitFun
   } = StartOpts,
  InitInfo = maps:with( [topic, partition, group_id, commit_fun, ack_fun]
                      , StartOpts
                      ),
  ?BROD_LOG_INFO("Starting group_subscriber_worker: ~p~n"
                 "Offset: ~p~nPid: ~p~n"
                , [InitInfo, BeginOffset, self()]
                ),
  {ok, CbState} = CbModule:init(InitInfo, CbConfig),
  State = #state{ start_options = StartOpts
                , cb_module     = CbModule
                , cb_state      = CbState
                , commit_fun    = CommitFun
                },
  CommittedOffsets = resolve_committed_offsets(Topic, Partition, BeginOffset),
  {ok, CommittedOffsets, State}.


handle_message(_Partition, Msg, State) ->
  #state{ cb_module  = CbModule
        , cb_state   = CbState
        , commit_fun = Commit
        } = State,
  case CbModule:handle_message(Msg, CbState) of
    {ok, commit, NewCbState} ->
      NewState = State#state{cb_state = NewCbState},
      Commit(get_last_offset(Msg)),
      {ok, ack, NewState};
    {ok, ack, NewCbState} ->
      %% Unlike the old group_subscriber here `ack' means just `ack'
      %% without commit
      NewState = State#state{cb_state = NewCbState},
      {ok, ack, NewState};
    {ok, NewCbState} ->
      NewState = State#state{cb_state = NewCbState},
      {ok, NewState}
  end.

handle_info(Info, #state{cb_module = CbModule , cb_state = CbState} = State) ->
  %% Any unhandled messages are forwarded to the callback module to
  %% support arbitrary message-passing.
  %% Only the {noreply, State} return value is supported.
  case brod_utils:optional_callback(CbModule, handle_info, [Info, CbState], {noreply, CbState}) of
    {noreply, NewCbState} ->
      {noreply, State#state{cb_state = NewCbState}}
  end.

terminate(Reason, #state{cb_module = CbModule, cb_state = State}) ->
  brod_utils:optional_callback(CbModule, terminate, [Reason, State], ok).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_last_offset(brod:message() | brod:message_set()) ->
                         brod:offset().
get_last_offset(#kafka_message{offset = Offset}) ->
  Offset;
get_last_offset(#kafka_message_set{messages = Messages}) ->
  #kafka_message{offset = Offset} = lists:last(Messages),
  Offset.


resolve_committed_offsets(_T, _P, ?undef) ->
  %% the default begin offset in consumer config will be used
  [];
resolve_committed_offsets(_T, Partition, Offset) when ?IS_SPECIAL_OFFSET(Offset) ->
  [{Partition, Offset}];
resolve_committed_offsets(_T, Partition, Offset) when is_integer(Offset) andalso Offset >= 0 ->
  %% Note: brod_topic_subscriber expects
  %% _acked_ offset rather than _begin_ offset
  %% in `init' callback return. In order to get
  %% begin offset it increments the value,
  %% which we don't want, hence decrement.
  [{Partition, Offset - 1}];
resolve_committed_offsets(Topic, Partition, Offset) ->
  ?BROD_LOG_WARNING("Discarded invalid committed offset ~p for: ~s:~p~n",
                    [Topic, Partition, Offset]),
  [].
