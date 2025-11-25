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

%% @doc A `brod_partitions_sync' is a `gen_server' that is responsible for fetching
%% the latest partition counts for a client and ensuring external changes to partitions
%% are propogated to the clients and starts a producer for the partition if not present
-module(brod_partitions_sync).
-behaviour(gen_server).

-export([
    start_link/3
]).

-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-include("brod_int.hrl").

-define(DEFAULT_SYNC_PARTITIONS_INTERVAL_SECONDS, 60).

-record(state, {
    client_id :: pid(),
    producers_sup :: pid(),
    interval :: non_neg_integer(),
    config :: brod_client:config()
}).

-type state() :: #state{}.

-spec start_link(atom(), pid(), brod_client:config()) ->
    {ok, pid()} | {error, any()}.
start_link(ClientId, ProducersSup, Config) ->
    gen_server:start_link(?MODULE, {ClientId, ProducersSup, Config}, []).

-spec init({pid(), pid(), brod_client:config()}) ->
    {ok, state()}.
init({ClientId, ProducersSup, Config}) ->
    Interval = sync_partition_interval(Config),
    schedule_sync(Interval),
    State = #state{
        client_id = ClientId,
        producers_sup = ProducersSup,
        interval = Interval,
        config = Config
    },

    {ok, State}.

%% @private
handle_info(
    sync,
    #state{
        client_id = Client,
        config = Config,
        producers_sup = Sup,
        interval = Interval
    } = State
) ->
    sync_partitions(Client, Sup, Config),
    sync_client(Client),
    schedule_sync(Interval),
    {noreply, State};
handle_info(_Info, #state{} = State) ->
    {noreply, State}.

%% @private
handle_call(Call, _From, #state{} = State) ->
    {reply, {error, {unsupported_call, Call}}, State}.

%% @private
handle_cast(_Cast, #state{} = State) ->
    {noreply, State}.

%% @private
code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
sync_client(Client) ->
    case brod_client:get_metadata_safe(Client, []) of
        {ok, _} ->
            ok;
        {error, Error} ->
            ?BROD_LOG_ERROR("Partitions Sync Client MetaData Error: ~p", [Error]),
            ok
    end.

%% @private
sync_partitions(Client, Sup, Config) ->
    Producers = brod_supervisor3:which_children(Sup),
    TopicsList = lists:map(fun({Topic, ProducerPid, _, _}) -> {Topic, ProducerPid} end, Producers),
    TopicsMap = maps:from_list(TopicsList),
    MetaData = get_metadata_for_topics(Client, maps:keys(TopicsMap)),
    lists:foreach(
        fun(#{name := Topic, partitions := Partitions}) ->
            ProducerPid = maps:get(Topic, TopicsMap),

            lists:foreach(
                fun(#{partition_index := Partition}) ->
                    sync_partition(ProducerPid, Client, Topic, Partition, Config)
                end,
                Partitions
            )
        end,
        MetaData
    ),
    ok.

%% @private
schedule_sync(Interval) ->
    erlang:send_after(Interval, self(), sync).

%% @private
sync_partition(Sup, Client, Topic, Partition, Config) ->
    case brod_producers_sup:find_producer(Sup, Topic, Partition) of
        {ok, Pid} ->
            {ok, Pid};
        {error, _} ->
            brod_producers_sup:start_producer(Sup, Client, Topic, Partition, Config)
    end.

%% @private
sync_partition_interval(Config) ->
    T = proplists:get_value(
        sync_partitions_interval_seconds, Config, ?DEFAULT_SYNC_PARTITIONS_INTERVAL_SECONDS
    ),
    timer:seconds(T).

%% @private
get_metadata_for_topics(_Client, []) ->
    [];
%% @private
get_metadata_for_topics(Client, Topics) ->
    case brod_client:get_bootstrap(Client) of
        {ok, {Hosts, Conn}} ->
            case brod_utils:get_metadata(Hosts, Topics, Conn) of
                {ok, #{topics := MetaData}} ->
                    MetaData;
                {error, Error} ->
                    ?BROD_LOG_ERROR("Partitions Sync MetaData Error: ~p", [Error]),
                    []
            end;
        {error, Error} ->
            ?BROD_LOG_ERROR("Partitions Sync Bootstrap Error: ~p", [Error]),
            []
    end.