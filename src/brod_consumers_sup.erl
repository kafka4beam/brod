%%%
%%%   Copyright (c) 2015 Klarna AB
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
%%%
%%% @copyright 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_consumers_sup).
-behaviour(supervisor3).

-export([ init/1
        , post_init/1
        , start_link/2
        , find_consumer/3
        ]).

-include("brod_int.hrl").

-define(SUP, brod_consumers_sup).
-define(SUP2, brod_consumers_sup2).

%% By default, restart sup2 after a 10-seconds delay
-define(DEFAULT_SUP2_RESTART_DELAY, 10).

%% By default, restart partition consumer worker process after a 2-seconds delay
-define(DEFAULT_CONSUMER_RESTART_DELAY, 2).

%%%_* APIs =====================================================================

%% @doc Start a root consumers supervisor,
%%      per-topic supervisors and per-partition consumer workers.
%%      The config is passed down to the consumers.
%% @end
-spec start_link(client_id(), [{topic(), consumer_config()}]) -> {ok, pid()}.
start_link(ClientId, Consumers) ->
  supervisor3:start_link(?MODULE, {?SUP, ClientId, Consumers}).

%% @doc Find a brod_consumer process pid running under sup2
%% @end
-spec find_consumer(pid(), topic(), partition()) ->
                       {ok, pid()} | {error, any()}.
find_consumer(SupPid, Topic, Partition) ->
  case supervisor3:find_child(SupPid, Topic) of
    [] ->
      %% no such topic worker started,
      %% check sys.config or brod:start_link_client args
      {error, {not_found, Topic}};
    [Sup2Pid] ->
      case brod_utils:is_pid_alive(Sup2Pid) of
        true ->
          case supervisor3:find_child(Sup2Pid, Partition) of
            [] ->
              %% no such partition?
              {error, {not_found, Topic, Partition}};
            [Pid] ->
              case brod_utils:is_pid_alive(Pid) of
                true  -> {ok, Pid};
                false -> {error, restarting}
              end
          end;
        false ->
          {error, restarting}
      end
  end.

%% @doc supervisor3 callback.
init({?SUP, ClientId, Consumers}) ->
  Children = [ consumers_sup_spec(ClientId, TopicName, Config)
             || {TopicName, Config} <- Consumers ],
  {ok, {{one_for_one, 0, 1}, Children}};
init({?SUP2, _ClientId, _Topic, _Config}) ->
  post_init.

post_init({?SUP2, ClientId, Topic, Config}) ->
  %% spawn consumer process for every partition
  %% in a topic if partitions are not set explicitly
  %% in the config
  %% TODO: make it dynamic when consumer groups API is ready
  Partitions =
    case proplists:get_value(partitions, Config, []) of
      [] ->
        {ok, List} = brod_client:get_partitions(ClientId, Topic),
        List;
      [_|_] = List ->
        List
    end,
  Children = [ consumer_spec(ClientId, Topic, Partition, Config)
             || Partition <- Partitions ],
  {ok, {{one_for_one, 0, 1}, Children}};
post_init(_) ->
  ignore.

consumers_sup_spec(ClientId, TopicName, Config0) ->
  DelaySecs = proplists:get_value(topic_restart_delay_seconds, Config0,
                                  ?DEFAULT_SUP2_RESTART_DELAY),
  Config    = proplists:delete(topic_restart_delay_seconds, Config0),
  Args      = [?MODULE, {?SUP2, ClientId, TopicName, Config}],
  { _Id       = TopicName
  , _Start    = {supervisor3, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = infinity
  , _Type     = supervisor
  , _Module   = [?MODULE]
  }.

consumer_spec(ClientId, Topic, Partition, Config0) ->
  DelaySecs = proplists:get_value(partition_restart_delay_seconds, Config0,
                                  ?DEFAULT_CONSUMER_RESTART_DELAY),
  Config1 = proplists:delete(partition_restart_delay_seconds, Config0),
  {value, {_, CbMod}, Config} = lists:keytake(cb_mod, 1, Config1),
  Args = [CbMod, ClientId, Topic, Partition, Config],
  { _Id       = Partition
  , _Start    = {brod_consumer, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
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
