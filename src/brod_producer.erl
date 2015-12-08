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
        , subscribe/2
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

-record(state, { client          :: client()
               , topic           :: topic()
               , config          :: producer_config()
               , topic_sup       :: pid()
               , partitions = [] :: [{partition(), pid()}]
               , client_mref     :: reference()
               }).

%%%_* API ----------------------------------------------------------------------
-spec start_link(client(), topic(), producer_config()) -> {ok, pid()}.
start_link(Client, Topic, Config) ->
  gen_server:start_link(?MODULE, {Client, Topic, Config}, []).

-spec subscribe(pid(), partition()) -> ok.
subscribe(TopicProducerPid, Partition) ->
  Msg = {subscribe, Partition, _PartitionProducerPid = self()},
  gen_server:cast(TopicProducerPid, Msg).

%%%_* gen_server callbacks -----------------------------------------------------
init({Client, Topic, Config}) ->
  self() ! init,
  #state{ client = Client
        , topic  = Topic
        , config = Config
        }.

handle_info(init, #state{ client = Client
                        , topic  = Topic
                        , config = Config
                        } = State) ->
  {ok, Metadata} = brod_client:get_metadata(Client, Topic),
  #metadata_response{topics = [TopicMetadata]} = Metadata,
  #topic_metadata{ error_code = TopicErrorCode
                 , partitions = PartitionsMetadataList
                 } = TopicMetadata,
  brod_kakfa:is_error(TopicErrorCode) orelse
    erlang:throw({"topic metadata error", TopicErrorCode}),
  Partitions = lists:map(fun(#partition_metadata{id = Id}) -> Id end,
                         PartitionsMetadataList),
  {ok, TopicSup} = brod_topic_sup:start_link(Client, Partitions, Config),
  Mref = monitor_client(Client),
  NewState = State#state{ topic_sup   = TopicSup
                        , partitions  = []
                        , client_mref = Mref
                        },
  {noreply, NewState};
handle_info({'DOWN', Mref, process, _Pid, Reason},
            #state{client_mref = Mref} = State) ->
  {stop, {client_down, Reason}, State};
handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected info: ~p", [Info]),
  {noreply, State}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({subscribe, Partition, PartitionProducerPid},
            #state{partitions = Workers} = State) ->
  NewWorkers =
    lists:keystore(Partition, 1, Workers, {Partition, PartitionProducerPid}),
  {noreply, State#state{partitions = NewWorkers}};
handle_cast({produce, _Caller, _Key, _Value}, State) ->
  %% TODO round-robin, random, etc.
  {noreply, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

terminate(_Reason, #state{}) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------

monitor_client(ClientId) when is_atom(ClientId) ->
  ClientPid = erlang:whereis(ClientId),
  true = is_pid(ClientPid), %% assert
  monitor_client(ClientPid);
monitor_client(ClientPid) ->
  erlang:monitor(process, ClientPid).

%% Tests -----------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
