%%%
%%%   Copyright (c) 2017 Klarna AB
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

-module(brod_kafka_apis).

-export([ pick_version/2
        , start_link/0
        , stop/0
        , versions_received/3
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-export_type([ api/0
             , vsn/0
             ]).

-define(SERVER, ?MODULE).
-define(ETS, ?MODULE).

-record(state, {}).

-type vsn() :: non_neg_integer().
-type range() :: {vsn(), vsn()}.
-type api() :: kpro:req_tag().
-type client_id() :: brod:client_id().

%% @doc Start process.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
  gen_server:call(?SERVER, stop, infinity).

%% @doc Report API version ranges for a given `brod_sock' pid.
-spec versions_received(client_id(), pid(), [{atom(), range()}]) -> ok.
versions_received(ClientId, SockPid, Versions) ->
  case resolve_version_ranges(ClientId, Versions, []) of
    [] ->
      ok;
    Vsns ->
      gen_server:call(?SERVER, {versions_received, SockPid, Vsns}, infinity)
  end.

%% @doc Pick API version for the given API.
-spec pick_version(pid(), api()) -> vsn().
pick_version(SockPid, API) ->
  do_pick_version(SockPid, API, supported_versions(API)).

init([]) ->
  ets:new(?ETS, [named_table, protected]),
  {ok, #state{}}.

handle_info({'DOWN', _Mref, process, Pid, _Reason}, State) ->
  ets:delete(?ETS, Pid),
  {noreply, State};
handle_info(Info, State) ->
  error_logger:error_msg("unknown info ~p", [Info]),
  {noreply, State}.

handle_cast(Cast, State) ->
  error_logger:error_msg("unknown cast ~p", [Cast]),
  {noreply, State}.

handle_call(stop, From, State) ->
  gen_server:reply(From, ok),
  {stop, normal, State};
handle_call({versions_received, SockPid, Versions}, _From, State) ->
  _ = erlang:monitor(process, SockPid),
  ets:insert(?ETS, {SockPid, Versions}),
  {reply, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%% @private
-spec do_pick_version(pid(), api(), range()) -> vsn().
do_pick_version(_SockPid, _API, {Vsn, Vsn}) ->
  %% only one version supported, no need to lookup
  Vsn;
do_pick_version(SockPid, API, {Min, _Max}) ->
  %% query the highest supported version
  case lookup_version(SockPid, API) of
    none -> Min; %% no version received from kafka, use min
    Vsn  -> Vsn  %% use max supported version
  end.

%% @private Lookup API from cache, return default if not found.
-spec lookup_version(pid(), api()) -> vsn() | none.
lookup_version(SockPid, API) ->
  case ets:lookup(?ETS, SockPid) of
    [] -> none;
    [{SockPid, Versions}] ->
      case lists:keyfind(API, 1, Versions) of
        {API, Vsn} -> Vsn;
        false -> none
      end
  end.

%% @private
-spec resolve_version_ranges(client_id(), [{api(), range()}], Acc) -> Acc
        when Acc :: [{api(), vsn()}].
resolve_version_ranges(_ClientId, [], Acc) -> lists:reverse(Acc);
resolve_version_ranges(ClientId, [{API, {MinKafka, MaxKafka}} | Rest], Acc) ->
  case resolve_version_range(ClientId, API, MinKafka, MaxKafka,
                             supported_versions(API)) of
    none -> resolve_version_ranges(ClientId, Rest, Acc);
    Max -> resolve_version_ranges(ClientId, Rest, [{API, Max} | Acc])
  end.

%% @private
-spec resolve_version_range(client_id(), api(), vsn(), vsn(),
                            range() | none()) -> vsn() | none.
resolve_version_range(_ClientId, _API, _MinKafka, _MaxKafka, none) ->
  %% API not implemented by brod
  none;
resolve_version_range(ClientId, API, MinKafka, MaxKafka, {MinBrod, MaxBrod}) ->
  Min = max(MinBrod, MinKafka),
  Max = min(MaxBrod, MaxKafka),
  case Min =< Max of
    true when MinBrod =:= MaxBrod ->
      %% if brod supports only one version
      %% no need to store the range in ETS
      none;
    true ->
      Max;
    false ->
      log_unsupported_api(ClientId, API,
                          {MinBrod, MaxBrod}, {MinKafka, MaxKafka}),
      none
  end.

%% @private
-spec log_unsupported_api(client_id(), api(), range(), range()) -> ok.
log_unsupported_api(ClientId, API, BrodRange, KafkaRange) ->
  error_logger:error_msg("Can not support API ~p for client ~p, "
                         "brod versions: ~p, kafka versions: ~p",
                         [API, ClientId, BrodRange, KafkaRange]),
  ok.

%% @private Do not change range without verification.
%%% Fixed (hardcoded) version APIs
%% sasl_handshake_request: 0
%% api_versions_request: 0

%%% Missing features
%% {create_topics_request, 0, 0}
%% {delete_topics_request, 0, 0}

%%% Will not support
%% leader_and_isr_request
%% stop_replica_request
%% update_metadata_request
%% controlled_shutdown_request
%% @end
supported_versions(API) ->
  case API of
    produce_request           -> {0, 2};
    fetch_request             -> {0, 3};
    offsets_request           -> {0, 1};
    metadata_request          -> {0, 0};
    offset_commit_request     -> {2, 2};
    offset_fetch_request      -> {1, 1};
    group_coordinator_request -> {0, 0};
    join_group_request        -> {0, 0};
    heartbeat_request         -> {0, 0};
    leave_group_request       -> {0, 0};
    sync_group_request        -> {0, 0};
    describe_groups_request   -> {0, 0};
    list_groups_request       -> {0, 0};
    _                         -> none
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
