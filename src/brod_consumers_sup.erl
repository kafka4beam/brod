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
%%% @doc brod consumers supervisor
%%%=============================================================================

-module(brod_consumers_sup).
-behaviour(supervisor3).

-export([ init/1
        , post_init/1
        , start_link/0
        , find_consumer/3
        , start_consumer/4
        , stop_consumer/2
        ]).

-include("brod_int.hrl").

-define(TOPICS_SUP, brod_consumers_sup).
-define(PARTITIONS_SUP, brod_consumers_sup2).

%% By default, restart ?PARTITIONS_SUP after a 10-seconds delay
-define(DEFAULT_PARTITIONS_SUP_RESTART_DELAY, 10).

%% By default, restart partition consumer worker process after a 2-seconds delay
-define(DEFAULT_CONSUMER_RESTART_DELAY, 2).

%%%_* APIs =====================================================================

%% @doc Start a root consumers supervisor.
-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor3:start_link(?MODULE, ?TOPICS_SUP).

%% @doc Dynamically start a per-topic supervisor.
-spec start_consumer(pid(), pid(), brod:topic(), brod:consumer_config()) ->
                        {ok, pid()} | {error, any()}.
start_consumer(SupPid, ClientPid, TopicName, Config) ->
  Spec = consumers_sup_spec(ClientPid, TopicName, Config),
  supervisor3:start_child(SupPid, Spec).


%% @doc Dynamically stop a per-topic supervisor.
-spec stop_consumer(pid(), brod:topic()) -> ok | {error, any()}.
stop_consumer(SupPid, TopicName) ->
  supervisor3:terminate_child(SupPid, TopicName).

%% @doc Find a brod_consumer process pid running under ?PARTITIONS_SUP
-spec find_consumer(pid(), brod:topic(), brod:partition()) ->
                       {ok, pid()} | {error, Reason} when
        Reason :: {consumer_not_found, brod:topic()}
                | {consumer_not_found, brod:topic(), brod:partition()}
                | {consumer_down, noproc}.
find_consumer(SupPid, Topic, Partition) ->
  case supervisor3:find_child(SupPid, Topic) of
    [] ->
      %% no such topic worker started,
      %% check sys.config or brod:start_link_client args
      {error, {consumer_not_found, Topic}};
    [PartitionsSupPid] ->
      try
        case supervisor3:find_child(PartitionsSupPid, Partition) of
          [] ->
            %% no such partition?
            {error, {consumer_not_found, Topic, Partition}};
          [Pid] ->
            {ok, Pid}
        end
      catch exit : {noproc, _} ->
        {error, {consumer_down, noproc}}
      end
  end.

%% @doc supervisor3 callback.
init(?TOPICS_SUP) ->
  {ok, {{one_for_one, 0, 1}, []}};
init({?PARTITIONS_SUP, _ClientPid, _Topic, _Config}) ->
  post_init.

post_init({?PARTITIONS_SUP, ClientPid, Topic, Config}) ->
  %% spawn consumer process for every partition
  %% in a topic if partitions are not set explicitly
  %% in the config
  %% TODO: make it dynamic when consumer groups API is ready
  case get_partitions(ClientPid, Topic, Config) of
    {ok, Partitions} ->
      Children = [ consumer_spec(ClientPid, Topic, Partition, Config)
                 || Partition <- Partitions ],
      {ok, {{one_for_one, 0, 1}, Children}};
    Error ->
      Error
  end;
post_init(_) ->
  ignore.

get_partitions(ClientPid, Topic, Config) ->
  case proplists:get_value(partitions, Config, []) of
    [] ->
      get_all_partitions(ClientPid, Topic);
    [_|_] = List ->
      {ok, List}
  end.

get_all_partitions(ClientPid, Topic) ->
  case brod_client:get_partitions_count(ClientPid, Topic) of
    {ok, PartitionsCnt} ->
      {ok, lists:seq(0, PartitionsCnt - 1)};
    {error, _} = Error ->
      Error
  end.

consumers_sup_spec(ClientPid, TopicName, Config0) ->
  DelaySecs = proplists:get_value(topic_restart_delay_seconds, Config0,
                                  ?DEFAULT_PARTITIONS_SUP_RESTART_DELAY),
  Config    = proplists:delete(topic_restart_delay_seconds, Config0),
  Args      = [?MODULE, {?PARTITIONS_SUP, ClientPid, TopicName, Config}],
  { _Id       = TopicName
  , _Start    = {supervisor3, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = infinity
  , _Type     = supervisor
  , _Module   = [?MODULE]
  }.

consumer_spec(ClientPid, Topic, Partition, Config0) ->
  DelaySecs = proplists:get_value(partition_restart_delay_seconds, Config0,
                                  ?DEFAULT_CONSUMER_RESTART_DELAY),
  Config = proplists:delete(partition_restart_delay_seconds, Config0),
  Args = [ClientPid, Topic, Partition, Config],
  { _Id       = Partition
  , _Start    = {brod_consumer, start_link, Args}
  , _Restart  = {transient, DelaySecs} %% restart only when not normal exit
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_consumer]
  }.

%%%_* Internal Functions =======================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
