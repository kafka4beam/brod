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
%%% @doc brod producers supervisor
%%%
%%% @copyright 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producers_sup).
-behaviour(supervisor3).

-export([ init/1
        , post_init/1
        , start_link/2
        , find_producer/3
        ]).

-include("brod_int.hrl").

-define(SUP, brod_producers_sup).
-define(SUP2, brod_producers_sup2).

%% By default, restart sup2 after a 10-seconds delay
-define(DEFAULT_SUP2_RESTART_DELAY, 10).

%% By default, restart partition producer worker process after a 2-seconds delay
-define(DEFAULT_PRODUCER_RESTART_DELAY, 2).

%%%_* APIs =====================================================================

%% @doc Start a root producers supervisor,
%%      per-topic supervisors and per-partition producer workers.
%%      The config is passed down to the producers.
%% For more details: @see brod_producer:start_link/4
%% @end
-spec start_link(client_id(), [{topic(), producer_config()}]) -> {ok, pid()}.
start_link(ClientId, Producers) ->
  supervisor3:start_link(?MODULE, {?SUP, ClientId, Producers}).

%% @doc Find a brod_producer process pid running under sup2
%% @end
-spec find_producer(pid(), topic(), partition()) ->
                       {ok, pid()} | {error, any()}.
find_producer(SupPid, Topic, Partition) ->
  case supervisor3:find_child(SupPid, Topic) of
    [] ->
      %% no such topic worker started,
      %% check sys.config or brod:start_link_client args
      {error, {not_found, Topic}};
    [Sup2Pid] ->
      case is_alive(Sup2Pid) of
        true ->
          case supervisor3:find_child(Sup2Pid, Partition) of
            [] ->
              %% no such partition?
              {error, {not_found, Topic, Partition}};
            [Pid] ->
              case is_alive(Pid) of
                true  -> {ok, Pid};
                false -> {error, restarting}
              end
          end;
        false ->
          {error, restarting}
      end
  end.

%% @doc supervisor3 callback.
init({?SUP, ClientId, Producers}) ->
  Children = [ producers_sup_spec(ClientId, TopicName, Config)
             || {TopicName, Config} <- Producers ],
  {ok, {{one_for_one, 0, 1}, Children}};
init({?SUP2, _ClientId, _Topic, _Config}) ->
  post_init.

post_init({?SUP2, ClientId, Topic, Config}) ->
  {ok, Partitions} = brod_client:get_partitions(ClientId, Topic),
  Children = [ producer_spec(ClientId, Topic, Partition, Config)
             || Partition <- Partitions ],
  %% Producer may crash in case of exception in case of network failure,
  %% or error code received in produce response (e.g. leader transition)
  %% In any case, restart right away will erry likely fail again.
  %% Hence set MaxR=0 here to cool-down for a configurable N-seconds
  %% before supervisor tries to restart it.
  {ok, {{one_for_one, 0, 1}, Children}};
post_init(_) ->
  ignore.

producers_sup_spec(ClientId, TopicName, Config0) ->
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

producer_spec(ClientId, Topic, Partition, Config0) ->
  DelaySecs = proplists:get_value(partition_restart_delay_seconds, Config0,
                                  ?DEFAULT_PRODUCER_RESTART_DELAY),
  Config    = proplists:delete(partition_restart_delay_seconds, Config0),
  Args      = [ClientId, Topic, Partition, Config],
  { _Id       = Partition
  , _Start    = {brod_producer, start_link, Args}
  , _Restart  = {permanent, DelaySecs}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_producer]
  }.

%%%_* Internal Functions =======================================================

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
