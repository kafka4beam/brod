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
-module(brod_topic_sup).
-behaviour(brod_supervisor).

-export([ start_link/4
        , init/1
        ]).

-include("brod_int.hrl").

%%%_* APIs ---------------------------------------------------------------------
-spec start_link(client(), topic(), [partition()], producer_config()) ->
        {ok, pid()}.
start_link(Client, Topic, Partitions, Config) ->
  TopicProducerPid = self(),
  brod_supervisor:start_link(?MODULE, {Client, Topic, Partitions,
                                       Config, TopicProducerPid}).

init({Client, Topic, Partitions, Config, TopicProducerPid}) ->
  SpecFun =
    fun(Partition) ->
      { _Id       = Partition
      , _Start    = {brod_partition_producer, start_link,
                     [Client, Topic, Partition, Config, TopicProducerPid]}
      , _Restart  = {permanent, 1} %% delay one second before restart
      , _Shutdown = 5000
      , _Type     = worker
      , _Module   = [brod_partition_producer]
      }
    end,
  {ok, {{one_for_one, 0, 1}, [SpecFun(P) || P <- Partitions]}}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
