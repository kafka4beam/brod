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

%%%=============================================================================
%%% @doc brod producers supervisor
%%%=============================================================================

-module(brod_producers_sup).
-behaviour(supervisor3).

-export([ init/1
        , post_init/1
        , start_link/0
        , find_producer/3
        , start_producer/4
        , stop_producer/2
        ]).

-include("brod_int.hrl").

-define(TOPICS_SUP, brod_producers_sup).
-define(PARTITIONS_SUP, brod_producers_sup2).

%% Minimum delay seconds to work with supervisor3
-define(MIN_SUPERVISOR3_DELAY_SECS, 1).

%% By default, restart ?PARTITIONS_SUP after a 10-seconds delay
-define(DEFAULT_PARTITIONS_SUP_RESTART_DELAY, 10).

%% By default, restart partition producer worker process after a 5-seconds delay
-define(DEFAULT_PRODUCER_RESTART_DELAY, 5).

%%%_* APIs =====================================================================

%% @doc Start a root producers supervisor.
%% For more details: @see brod_producer:start_link/4
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor3:start_link(?MODULE, ?TOPICS_SUP).

%% @doc Dynamically start a per-topic supervisor
-spec start_producer(pid(), pid(), brod:topic(), brod:producer_config()) ->
                        {ok, pid()} | {error, any()}.
start_producer(SupPid, ClientPid, TopicName, Config) ->
  Spec = producers_sup_spec(ClientPid, TopicName, Config),
  supervisor3:start_child(SupPid, Spec).

%% @doc Dynamically stop a per-topic supervisor
-spec stop_producer(pid(), brod:topic()) -> ok | {}.
stop_producer(SupPid, TopicName) ->
  supervisor3:terminate_child(SupPid, TopicName).

%% @doc Find a brod_producer process pid running under ?PARTITIONS_SUP.
-spec find_producer(pid(), brod:topic(), brod:partition()) ->
                       {ok, pid()} | {error, Reason} when
        Reason :: {producer_not_found, brod:topic()}
                | {producer_not_found, brod:topic(), brod:partition()}
                | {producer_down, noproc}.
find_producer(SupPid, Topic, Partition) ->
  case supervisor3:find_child(SupPid, Topic) of
    [] ->
      %% no such topic worker started,
      %% check sys.config or brod:start_link_client args
      {error, {producer_not_found, Topic}};
    [PartitionsSupPid] ->
      try
        case supervisor3:find_child(PartitionsSupPid, Partition) of
          [] ->
            %% no such partition?
            {error, {producer_not_found, Topic, Partition}};
          [Pid] ->
            {ok, Pid}
        end
      catch exit : {noproc, _} ->
        {error, {producer_down, noproc}}
      end
  end.

%% @doc supervisor3 callback.
init(?TOPICS_SUP) ->
  {ok, {{one_for_one, 0, 1}, []}};
init({?PARTITIONS_SUP, _ClientPid, _Topic, _Config}) ->
  post_init.

post_init({?PARTITIONS_SUP, ClientPid, Topic, Config}) ->
  case brod_client:get_partitions_count(ClientPid, Topic) of
    {ok, PartitionsCnt} ->
      Children = [ producer_spec(ClientPid, Topic, Partition, Config)
                 || Partition <- lists:seq(0, PartitionsCnt - 1) ],
      %% Producer may crash in case of exception in case of network failure,
      %% or error code received in produce response (e.g. leader transition)
      %% In any case, restart right away will erry likely fail again.
      %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
      %% before supervisor tries to restart it.
      {ok, {{one_for_one, 0, 1}, Children}};
    {error, Reason} ->
      {error, Reason}
  end.

producers_sup_spec(ClientPid, TopicName, Config0) ->
  {Config, DelaySecs} =
    take_delay_secs(Config0, topic_restart_delay_seconds,
                    ?DEFAULT_PARTITIONS_SUP_RESTART_DELAY),
  Args = [?MODULE, {?PARTITIONS_SUP, ClientPid, TopicName, Config}],
  { _Id       = TopicName
  , _Start    = {supervisor3, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = infinity
  , _Type     = supervisor
  , _Module   = [?MODULE]
  }.

producer_spec(ClientPid, Topic, Partition, Config0) ->
  {Config, DelaySecs} =
    take_delay_secs(Config0, partition_restart_delay_seconds,
                    ?DEFAULT_PRODUCER_RESTART_DELAY),
  Args      = [ClientPid, Topic, Partition, Config],
  { _Id       = Partition
  , _Start    = {brod_producer, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_producer]
  }.

%%%_* Internal Functions =======================================================

-spec take_delay_secs(brod:producer_config(), atom(), integer()) ->
        {brod:producer_config(), integer()}.
take_delay_secs(Config, Name, DefaultValue) ->
  Secs =
    case proplists:get_value(Name, Config) of
      N when is_integer(N) andalso N >= ?MIN_SUPERVISOR3_DELAY_SECS ->
        N;
      _ ->
        DefaultValue
    end,
  {proplists:delete(Name, Config), Secs}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
