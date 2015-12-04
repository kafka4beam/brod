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
%%% @doc
%%% @copyright 2015 Klarna AB
%%% @end
%%%=============================================================================
-module(brod_sup).
-behaviour(brod_supervisor).

-export([ start_link/0
        , start_link_topic_sup/1
        , start_link_connection_sup/0
        , init/1
        ]).

-define(DEFAULT_PRODUCER_RESTART_DELAY_SECONDS, 10).

%%%_* APIs ---------------------------------------------------------------------

%% @doc Start supervisor of per-topic permanent producer supervisors.
%%
%% Permanent producers can be configured in app env (sys.config).
%% A minimal example of app env with permanent producer args:
%%    [ { producers
%%      , [ { <<"test-topic">>
%%          , [ {hosts, [{"localhost", 9092}]}
%%            , {required_acks, 1}
%%            , {client_id, <<"brod-at-localhost">>}
%%            ]
%%          }
%%        ]
%%      }
%%    ].
%% @see brod:start_link_producer/2 for more info about other producer args
%% @end
-spec start_link() -> {ok, pid()}.
start_link() ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, root).

%% @doc Start brod connection supervisor.
-spec start_link_connection_sup() -> {ok, pid()}.
start_link_connection_sup() ->
  brod_supervisor:start_link({local, brod_connection_sup}, ?MODULE, connection).

%% @doc Start per-topic supervisor.
-spec start_link_topic_sup(binary()) -> {ok, pid()}.
start_link_topic_sup(TopicName) ->
  brod_supervisor:start_link(?MODULE, {topic, TopicName}).

%% @doc brod_supervisor callback.
init(root) ->
  init({root, application:get_env(brod, producers, _Default = [])});
init({root, Producers}) ->
  ConnectionSupSpec =
    { _Id       = brod_connection_sup
    , _Start    = {?MODULE, start_link_connection_sup, []}
    , _Restart  = permanent
    , _Shutdown = 5000
    , _Type     = supervisor
    , _Module   = [?MODULE]
    },
  TopicSupFun =
    fun(TopicName) ->
      { _Id       = {topic_sup, TopicName}
      , _Start    = {?MODULE, start_link_topic_sup, [TopicName]}
      , _Restart  = permanent
      , _Shutdown = 5000
      , _Type     = supervisor
      , _Module   = [?MODULE]
      }
    end,
  TopicSupSpecs = [TopicSupFun(TopicName) || {TopicName, _Args} <- Producers],
  ChildrenSupervisors = [ConnectionSupSpec | TopicSupSpecs],
  {ok, {{one_for_one, 0, 1}, ChildrenSupervisors}};
init(connection) ->
  ConnectionWorkerSpec =
    { _Id       = brod_connection
    , _Start    = {brod_connection, start_link, []}
    , _Restart  = {permanent, 10000} %% delay 10 seconds and retry
    , _Shutdown = 1000
    , _Type     = worker
    , _Module   = [brod_connection]
    },
  {ok, {{simple_one_for_one, 0, 1}, [ConnectionWorkerSpec]}};
init({topic, TopicName}) when is_binary(TopicName) ->
  {ok, Producers} = application:get_env(brod, producers),
  {TopicName, Args0} = lists:keyfind(TopicName, Producers),
  KafkaHosts =
    case proplists:get_value(hosts, Args0) of
      undefined ->
        erlang:throw({"mandatory-prop-missing", {hosts, Args0}});
      L when is_list(L) ->
        L;
      ClusterName ->
        %% in case of reference, look it up in app env
        {ok, Clusters} = application:get_env(brod, kafka_clusters),
        {ClusterName, Hosts} = lists:keyfind(ClusterName, Clusters),
        Hosts
    end,
  RestartDelay =
    proplists:get_value(restart_delay_seconds, Args0,
                        ?DEFAULT_PRODUCER_RESTART_DELAY_SECONDS),
  Args1 = proplists:delete(hosts, Args0),
  Args  = proplists:delete(restart_delay_seconds, Args1),
  init({topic, TopicName, KafkaHosts, RestartDelay, Args});
init({topic, TopicName, KafkaHosts, RestartDelay, Args}) ->
  PartitionWorkerSpec =
    { _Id       = brod_partition
    , _Start    = {brod_partition, start_link, [TopicName, KafkaHosts, Args]}
    , _Restart  = {permanent, RestartDelay}
    , _Shutdown = 1000
    , _Type     = worker
    , _Module   = [brod_connection]
    },
  {ok, {{simple_one_for_one, 0, 1}, [PartitionWorkerSpec]}}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
