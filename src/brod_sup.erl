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

-export([ start_link/1
        , init/1
        ]).

-define(DEFAULT_PRODUCER_RESTART_DELAY_SECONDS, 10).

%% @doc Start supervisor of permanent producers.
%% @see brod:start_link_producer/2 for more info about all producer args
%% @end
-spec start_link([proplists:proplist()]) -> {ok, pid()}.
start_link(PermanentProducers) ->
  brod_supervisor:start_link({local, ?MODULE}, ?MODULE, PermanentProducers).

init(PermanentProducers) ->
  Children = [permanent_producer(P) || P <- PermanentProducers],
  {ok, {{one_for_one, 0, 1}, Children}}.

permanent_producer({ProducerId0, Args0}) ->
  KafkaHosts = proplists:get_value(hosts, Args0),
  case is_list(KafkaHosts) of
    true  -> ok;
    false -> erlang:throw({mandatory_prop_missing, Args0})
  end,
  RestartDelay =
    proplists:get_value(restart_delay_seconds, Args0,
                        ?DEFAULT_PRODUCER_RESTART_DELAY_SECONDS),
  %% no need of 'hosts' and 'restart_delay_seconds' in producer options
  Args1 = proplists:delete(hosts, Args0),
  Args  = proplists:delete(restart_delay_seconds, Args1),

  %% In case producer_id is not given in args body, use the tag.
  {ProducerId, Opts} =
    case proplists:get_value(producer_id, Args) of
      undefined ->
        {ProducerId0, [{producer_id, ProducerId0} | Args]};
      ID when is_atom(ID) ->
        {ID, Args}
    end,
  { _Id       = ProducerId
  , _Start    = {brod_producer, start_link, [KafkaHosts, Opts]}
  , _Restart  = {permanent, RestartDelay}
  , _Shutdown = 5000
  , _Type     = worker
  , _Module   = [brod_producer]
  }.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
