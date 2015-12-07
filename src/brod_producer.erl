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

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------

-record(state, { client_id  :: client_id()
               , topic      :: topic()
               , config     :: producer_config()
               , partitions :: [{partition(), pid()}]
               }).

%%%_* API ----------------------------------------------------------------------
-spec start_link(client_id(), topic(), producer_config()) -> {ok, pid()}.
start_link(ClientId, Topic, Config) ->
  gen_server:start_link(?MODULE, {ClientId, Topic, Config}, []).

%%%_* gen_server callbacks -----------------------------------------------------
init({_ClientId, _Topic, _Config}) ->
  #state{}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

%% TODO: handle produce req
handle_cast(_Cast, State) ->
  {noreply, State}.

%% TODO: handle partition producer exits
handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected info: ~p", [Info]),
  {noreply, State}.

terminate(_Reason, #state{}) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------

%% Tests -----------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
