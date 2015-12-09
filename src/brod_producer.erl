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
-export([ get_partition_producer/2
        , start_link/3
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

-type partition_worker() :: {partition(), waiting | pid()}.

-record(state, { client          :: client()
               , topic           :: topic()
               , config          :: producer_config()
               , topic_sup       :: pid()
               , client_mref     :: reference()
               , partitionner    :: brod_partitionner()
               , partitions = [] :: [partition_worker()]
               }).

%%%_* API ----------------------------------------------------------------------
-spec start_link(client(), topic(), producer_config()) -> {ok, pid()}.
start_link(Client, Topic, Config) ->
  gen_server:start_link(?MODULE, {Client, Topic, Config}, []).

-spec subscribe(pid(), partition()) -> ok.
subscribe(TopicProducerPid, Partition) ->
  Msg = {subscribe, Partition, _PartitionProducerPid = self()},
  gen_server:cast(TopicProducerPid, Msg).

-spec get_partition_producer(pid(), binary()) -> {ok, pid()} | {error, any()}.
get_partition_producer(Pid, KafkaMsgKey) ->
  case erlang:process_info(Pid, dictionary) of
    {dictionary, ProccessDict} ->
      case proplists:get_value(brod_producer_type, ProccessDict) of
        brod_partition_producer ->
          %% the pid itself is a partition worker
          {ok, Pid};
        ?MODULE ->
          %% the pid is a topic worker, get it from
          gen_server:call(Pid, {get_partition_producer, KafkaMsgKey}, infinity)
      end;
    undefined ->
      {error, {producer_down, noproc}}
  end.

%%%_* gen_server callbacks -----------------------------------------------------
init({Client, Topic, Config}) ->
  erlang:put(brod_producer_type, ?MODULE),
  random:seed(os:timestamp()),
  self() ! init,
  #state{ client       = Client
        , topic        = Topic
        , config       = Config
        , partitionner = get_partitionner(Config)
        }.

handle_info(init, #state{ client       = Client
                        , topic        = Topic
                        , config       = Config
                        , partitionner = Partitionner
                        } = State) ->
  {ok, Metadata} = brod_client:get_metadata(Client, Topic),
  #metadata_response{topics = [TopicMetadata]} = Metadata,
  #topic_metadata{ error_code = TopicErrorCode
                 , partitions = PartitionsMetadataList
                 } = TopicMetadata,
  brod_kakfa:is_error(TopicErrorCode) orelse
    erlang:throw({"topic metadata error", TopicErrorCode}),
  Partitions0 = lists:map(fun(#partition_metadata{id = Id}) -> Id end,
                          PartitionsMetadataList),
  {ok, TopicSup} = brod_topic_sup:start_link(Client, Partitions0, Config),
  Mref = monitor_client(Client),
  Partitions =
    case Partitionner =:= roundrobin of
      true  -> init_roundrobin_state(Partitions0);
      false -> Partitions0
    end,
  NewState =
    State#state{ topic_sup   = TopicSup
               , partitions  = [{P, waiting} || P <- Partitions]
               , client_mref = Mref
               },
  {noreply, NewState};
handle_info({'DOWN', Mref, process, _Pid, Reason},
            #state{client_mref = Mref} = State) ->
  {stop, {client_down, Reason}, State};
handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected info: ~p", [Info]),
  {noreply, State}.

handle_call({get_partition_producer, KafkaMsgKey}, _From, State) ->
  {Result, NewState} = do_get_partition_worker(KafkaMsgKey, State),
  {reply, Result, NewState};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast({subscribe, Partition, PartitionProducerPid},
            #state{partitions = Workers} = State) ->
  NewWorkers =
    lists:keystore(Partition, 1, Workers, {Partition, PartitionProducerPid}),
  {noreply, State#state{partitions = NewWorkers}};
handle_cast(_Cast, State) ->
  {noreply, State}.

terminate(_Reason, #state{}) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------

%% @private Randomize the fist candidate of roundrobin list.
%% This is to avoid always producing to the same partition
%% in case of restart/retry repeatedly.
%% @end
-spec init_roundrobin_state([partition()]) -> [partition()].
init_roundrobin_state(Partitions) ->
  N = random:uniform(length(Partitions)),
  {Head, Tail} = lists:split(N, Partitions),
  Tail ++ Head.

monitor_client(ClientId) when is_atom(ClientId) ->
  ClientPid = erlang:whereis(ClientId),
  true = is_pid(ClientPid), %% assert
  monitor_client(ClientPid);
monitor_client(ClientPid) ->
  erlang:monitor(process, ClientPid).

-spec get_partitionner(producer_config()) -> brod_partitionner().
get_partitionner(Config) ->
  proplists:get_value(partitionner, Config, random).

-spec do_get_partition_worker(binary(), #state{}) ->
        {Result, #state{}} when
          Result :: {ok, pid()} | {error, any()}.
do_get_partition_worker(_KafkaMsgKey, #state{ partitionner = random
                                            , partitions   = Partitions
                                            } = State) ->
  AlivePids =
    lists:filtermap(
      fun({_Partition, Pid}) ->
        is_alive(Pid) andalso {true, Pid}
      end, Partitions),
  Result =
    case length(AlivePids) of
      0 -> {error, no_alive_partition_producer};
      N -> {ok, lists:nth(erlang:uniform(N), AlivePids)}
    end,
  {Result, State};
do_get_partition_worker(_KafkaMsgKey, #state{ partitionner = roundrobin
                                            , partitions   = Partitions
                                            } = State) ->
  {Result, NewPartitions} = roundrobin_next(Partitions, _Acc = []),
  {Result, State#state{partitions = NewPartitions}};
do_get_partition_worker(KafkaMsgKey, #state{ partitionner = F
                                           , partitions   = Partitions
                                           } = State) when is_function(F, 2) ->
  %% Caller defined partitionner
  NrOfPartitions = length(Partitions),
  PartitionResult =
    try
      {ok, F(KafkaMsgKey, NrOfPartitions)}
    catch C : E ->
      {error, {partitionner_error, {C, E, erlang:get_stacktrace()}}}
    end,
  Result =
    case PartitionResult of
      {ok, Partition} when is_integer(Partition) andalso
                           Partition >= 0        andalso
                           Partition =< NrOfPartitions ->
        {Partition, Pid} = lists:keyfind(Partition, 1, Partitions),
        case is_alive(Pid) of
          true ->
            {ok, Pid};
          false ->
            {error, {partition_producer_not_alive, Partition}}
        end;
      {ok, Partition} ->
        {error, {bad_partitionner_result, Partition}};
      {error, Reason} ->
        {error, Reason}
  end,
  {Result, State}.

-spec roundrobin_next([partition_worker()], [partition_worker()]) ->
        {Result, [partition_worker()]} when
          Result :: {ok, pid()} | {error, any()}.
roundrobin_next([], Acc) ->
  Result = {error, no_alive_partition_producer},
  {Result, lists:reverse(Acc)};
roundrobin_next([{Partition, Pid} | Rest], Acc) ->
  case is_alive(Pid) of
    true ->
      Result = {ok, Pid},
      {Result, Rest ++ lists:reverse([{Partition, Pid} | Acc])};
    false ->
      %% not alive, continue try the next one
      roundrobin_next(Rest, [{Partition, Pid} | Acc])
  end.

is_alive(Pid) ->
  is_pid(Pid) andalso is_process_alive(Pid).

%% Tests -----------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
