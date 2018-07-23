%%%
%%%   Copyright (c) 2017-2018 Klarna Bank AB (publ)
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

%% Version ranges are cached per host and per connection pid in ets

-module(brod_kafka_apis).

-export([ default_version/1
        , pick_version/2
        , start_link/0
        , stop/0
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

-type vsn() :: kpro:vsn().
-type range() :: {vsn(), vsn()}.
-type api() :: kpro:api().
-type conn() :: kpro:connection().

%% @doc Start process.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
  gen_server:call(?SERVER, stop, infinity).

%% @doc Get default supported version for the given API.
-spec default_version(api()) -> vsn().
default_version(API) ->
  {Min, _Max} = supported_versions(API),
  Min.

%% @doc Pick API version for the given API.
-spec pick_version(conn(), api()) -> vsn().
pick_version(Conn, API) ->
  do_pick_version(Conn, API, supported_versions(API)).

%%%_* gen_server callbacks =====================================================

init([]) ->
  ?ETS = ets:new(?ETS, [named_table, public]),
  {ok, #state{}}.

handle_info({'DOWN', _Mref, process, Conn, _Reason}, State) ->
  _ = ets:delete(?ETS, Conn),
  {noreply, State};
handle_info(Info, State) ->
  error_logger:error_msg("unknown info ~p", [Info]),
  {noreply, State}.

handle_cast({monitor_connection, Conn}, State) ->
  erlang:monitor(process, Conn),
  {noreply, State};
handle_cast(Cast, State) ->
  error_logger:error_msg("unknown cast ~p", [Cast]),
  {noreply, State}.

handle_call(stop, From, State) ->
  gen_server:reply(From, ok),
  {stop, normal, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

%%%_* Internals ================================================================

-spec do_pick_version(conn(), api(), range()) -> vsn().
do_pick_version(_Conn, _API, {V, V}) -> V;
do_pick_version(Conn, API, {Min, Max} = MyRange) ->
  case lookup_vsn_range(Conn, API) of
    none ->
      Min; %% no version received from kafka, use min
    {KproMin, KproMax} = Range when KproMin > Max orelse KproMax < Min ->
      erlang:error({unsupported_vsn_range, API, MyRange, Range});
    {_, KproMax} ->
      min(KproMax, Max) %% try to use highest version
  end.

%% Lookup API from cache, return 'none' if not found.
-dialyzer([{nowarn_function, [lookup_vsn_range/2]}]).
-spec lookup_vsn_range(conn(), api()) -> {vsn(), vsn()} | none.
lookup_vsn_range(Conn, API) ->
  case ets:lookup(?ETS, Conn) of
    [] ->
      case kpro:get_api_versions(Conn) of
        {ok, Versions} when is_map(Versions) ->
          %% public ets, insert it by caller
          ets:insert(?ETS, {Conn, Versions}),
          %% tell ?SERVER to monitor the connection
          %% so to delete it from cache when 'DOWN' is received
          ok = monitor_connection(Conn),
          maps:get(API, Versions, none);
        {error, _Reason} ->
          none %% connection died, ignore
      end;
    [{Conn, Vsns}] ->
      maps:get(API, Vsns, none)
  end.

%% Do not change range without verification.
supported_versions(API) ->
  case API of
    produce          -> {0, 5};
    fetch            -> {0, 7};
    list_offsets     -> {0, 2};
    metadata         -> {0, 2};
    offset_commit    -> {2, 2};
    offset_fetch     -> {1, 2};
    find_coordinator -> {0, 0};
    join_group       -> {0, 0};
    heartbeat        -> {0, 0};
    leave_group      -> {0, 0};
    sync_group       -> {0, 0};
    describe_groups  -> {0, 0};
    list_groups      -> {0, 0};
    _                -> erlang:error({unsupported_api, API})
  end.

monitor_connection(Conn) ->
  gen_server:cast(?SERVER, {monitor_connection, Conn}).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
