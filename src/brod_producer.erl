%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).
-behaviour(gen_server).

%% Server API
-export([ start_link/3
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-include("brod_int.hrl").

-record(state, { client         :: client()
               , topic          :: topic()
               , config         :: producer_config()
               , partitions_sup :: pid()
               }).

%%%_* APIs =====================================================================

%% @doc Start a per-topic worker to fetch topic metadata and start
%% per-partition producer workers. the config is passed down to the producers
%% @see brod_producer:start_link/4 for more details.
%% @end
-spec start_link(client_id(), topic(), producer_config()) -> {ok, pid()}.
start_link(ClientId, Topic, Config) ->
  gen_server:start_link(?MODULE, {ClientId, Topic, Config}, []).

%%%_* gen_server callbacks =====================================================

init({ClientId, Topic, Config}) ->
  self() ! init,
  {ok, #state{ client = ClientId
             , topic  = Topic
             , config = Config
             }}.

handle_info(init, #state{ client = ClientId
                        , topic  = Topic
                        , config = Config
                        } = State) ->
  {ok, Partitions} = brod_client:get_partitions(ClientId, Topic),
  {ok, PartitionsSup} =
    brod_sup:start_link_partition_producers_sup(
      ClientId, Topic, Partitions, Config),
  NewState = State#state{partitions_sup = PartitionsSup},
  {noreply, NewState};
handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected info: ~p", [Info]),
  {noreply, State}.

handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(_Cast, State) ->
  {noreply, State}.

terminate(_Reason, #state{}) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal Functions =======================================================

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
