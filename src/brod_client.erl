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

-module(brod_client).
-behaviour(gen_server).

-export([ get_connection/3
        , get_metadata/2
        , get_partitions/2
        , get_producer/3
        , register_producer/3
        , start_link/4
        , stop/1
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-define(DEFAULT_RECONNECT_COOL_DOWN_SECONDS, 1).
-define(DEFAULT_GET_METADATA_TIMEOUT_SECONDS, 5).

-define(dead_since(TS, REASON), {dead_since, TS, REASON}).
-type dead_socket() :: ?dead_since(erlang:timestamp(), any()).

%% ClientId as ets table name.
-define(ETS(ClientId), ClientId).
-define(PRODUCER_KEY(Topic, Partition),
        {producer, Topic, Partition}).
-define(PRODUCER(Topic, Partition, Pid),
        {?PRODUCER_KEY(Topic, Partition), Pid}).

-record(sock,
        { endpoint :: endpoint()
        , sock_pid :: pid() | dead_socket()
        }).

-record(state,
        { client_id     :: client_id()
        , endpoints     :: [endpoint()]
        , meta_sock     :: pid()
        , sockets = []  :: [#sock{}]
        , producers_sup :: pid()
        , config        :: client_config()
        }).

%%%_* APIs =====================================================================

-spec start_link(client_id(), [endpoint()], client_config(),
                 [{topic(), producer_config()}]) -> {ok, pid()}.
start_link(ClientId, Endpoints, Config, Producers) when is_atom(ClientId) ->
  gen_server:start_link({local, ClientId}, ?MODULE,
                        {ClientId, Endpoints, Config, Producers}, []).

-spec stop(client()) -> ok.
stop(Client) ->
  gen_server:call(Client, stop).

-spec get_metadata(client_id(), topic()) -> {ok, #metadata_response{}}.
get_metadata(Client, Topic) ->
  gen_server:call(Client, {get_metadata, Topic}, infinity).

%% @doc Get the connection to kafka broker at Host:Port.
%% If there is no such connection established yet, try to establish a new one.
%% If the connection is already established per request from another producer
%% the same socket is returned.
%% If the old connection was dead less than a configurable N seconds ago,
%% {error, LastReason} is returned.
%% @end
-spec get_connection(client(), hostname(), portnum()) ->
        {ok, pid()} | {error, any()}.
get_connection(Client, Host, Port) ->
  gen_server:call(Client, {get_connection, Host, Port}, infinity).

%% @doc Get all partition numbers of a given topic.
-spec get_partitions(client(), topic()) -> {ok, [partition()]} | {error, any()}.
get_partitions(Client, Topic) ->
  case get_metadata(Client, Topic) of
    {ok, #metadata_response{topics = TopicsMetadata}} ->
      [TopicMetadata] = TopicsMetadata,
      try
        {ok, do_get_partitions(TopicMetadata)}
      catch
        throw:Reason ->
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Register self() as a partition producer the pid is registered in an ETS
%% table, then the callers may lookup a producer pid from the table and make
%% produce requests to the producer process directly.
%% @end
-spec register_producer(client(), topic(), partition()) -> ok.
register_producer(Client, Topic, Partition) ->
  Producer = self(),
  gen_server:cast(Client, {register_producer, Topic, Partition, Producer}).

%% @doc Get producer of a given topic-partition.
-spec get_producer(client(), topic(), partition()) ->
        {ok, pid()} | {error, Reason}
          when Reason :: client_down
                       | restarting
                       | {not_found, topic()}
                       | {not_found, topic(), partition()}.
get_producer(ClientPid, Topic, Partition) when is_pid(ClientPid) ->
  case erlang:process_info(ClientPid, registered_name) of
    {registered_name, ClientId} ->
      get_producer(ClientId, Topic, Partition);
    undefined ->
      {error, client_down}
  end;
get_producer(ClientId, Topic, Partition) when is_atom(ClientId) ->
  try
    case ets:lookup(?ETS(ClientId), ?PRODUCER_KEY(Topic, Partition)) of
      [] ->
        %% not yet registered, 2 possible reasons:
        %% 1. caller is too fast, producers are starting up
        %%    make a synced call all the way down the supervision tree
        %%    to the partition producer sup should resolve the race
        %% 2. bad argument, no such worker started, supervisors should know
        find_producer(ClientId, Topic, Partition);
      [?PRODUCER(Topic, Partition, Pid)] ->
        case is_alive(Pid) of
          true  -> {ok, Pid};
          false -> {error, restarting}
        end
    end
  catch error:badarg ->
    {error, client_down}
  end.

%%%_* gen_server callbacks =====================================================

init({ClientId, Endpoints, Config, Producers}) ->
  erlang:process_flag(trap_exit, true),
  ets:new(?ETS(ClientId), [named_table, protected, {read_concurrency, true}]),
  self() ! {init, Producers},
  {ok, #state{ client_id = ClientId
             , endpoints = Endpoints
             , config    = Config
             }}.

%% TODO: maybe add a timer to clean up very old ?dead_since sockets
handle_info({init, Producers}, #state{ client_id = ClientId
                                     , endpoints = Endpoints
                                     } = State) ->
  Sock = start_metadata_socket(Endpoints),
  {ok, Pid} = brod_sup:start_link_producers_sup(ClientId, Producers),
  {noreply, State#state{ meta_sock     = Sock
                       , producers_sup = Pid
                       }};
handle_info({'EXIT', Pid, _Reason}, #state{ meta_sock = #sock{sock_pid = Pid}
                                          , endpoints = Endpoints
                                          } = State) ->
  NewSock = start_metadata_socket(Endpoints),
  {noreply, State#state{meta_sock = NewSock}};
handle_info({'EXIT', Pid, Reason}, State) ->
  {ok, NewState} = handle_socket_down(State, Pid, Reason),
  {noreply, NewState};
handle_info(_Info, State) ->
  {noreply, State}.

handle_call({get_topic_worker, Topic}, _From, State) ->
  Result = do_get_topic_worker(State, Topic),
  {reply, Result, State};
handle_call({get_metadata, Topic}, _From, State) ->
  Result = do_get_metadata(Topic, State),
  {reply, Result, State};
handle_call({get_connection, Host, Port}, _From, State) ->
  {NewState, Result} = do_get_connection(State, Host, Port),
  {reply, Result, NewState};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

handle_cast({register_producer, Topic, Partition, ProducerPid},
            #state{client_id = ClientId} = State) ->
  Tab = ?ETS(ClientId),
  ets:insert(Tab, ?PRODUCER(Topic, Partition, ProducerPid)),
  {noreply, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, #state{meta_sock = MetaSock, sockets = Sockets}) ->
  lists:foreach(
    fun(#sock{sock_pid = Pid}) ->
      case is_pid(Pid) andalso is_process_alive(Pid) of
        true  -> exit(Pid, shutdown);
        false -> ok
      end
    end, [MetaSock | Sockets]).

%%%_* Internal Functions =======================================================

find_producer(ClientId, Topic, Partition) ->
  case gen_server:call(ClientId, {get_topic_worker, Topic}, infinity) of
    {ok, Pid} ->
      brod_producers:get_producer(Pid, Partition);
    {error, Reason} ->
      {error, Reason}
  end.

do_get_topic_worker(#state{producers_sup = Sup}, Topic) ->
  case brod_supervisor:find_child(Sup, Topic) of
    [] ->
      %% no such topic worker started,
      %% check sys.config or brod:start_link_client args
      {error, {not_found, Topic}};
    [Pid] ->
      case is_alive(Pid) of
        true  -> {ok, Pid};
        false -> {error, restarting}
      end
  end.

-spec do_get_partitions(#topic_metadata{}) -> [partition()].
do_get_partitions(#topic_metadata{ error_code = TopicErrorCode
                                 , partitions = Partitions}) ->
  brod_kafka:is_error(TopicErrorCode) andalso
    erlang:throw({"topic metadata error", TopicErrorCode}),
  lists:map(
    fun(#partition_metadata{ error_code = PartitionErrorCode
                           , id         = Partition
                           }) ->
      brod_kafka:is_error(PartitionErrorCode) andalso
        erlang:throw({"partition metadata error", PartitionErrorCode}),
      Partition
    end, Partitions).

-spec do_get_metadata(topic(), #state{}) ->
        {ok, #metadata_response{}} | {error, any()}.
do_get_metadata(Topic, #state{ meta_sock = #sock{sock_pid = SockPid}
                             , config    = Config
                             }) ->
  Request = #metadata_request{topics = [Topic]},
  Timeout = proplists:get_value(get_metadata_timout_seconds, Config,
                                ?DEFAULT_GET_METADATA_TIMEOUT_SECONDS),
  brod_sock:send_sync(SockPid, Request, timer:seconds(Timeout)).

-spec do_get_connection(#state{}, hostname(), portnum()) ->
        {#state{}, Result} when Result :: {ok, pid()} | {error, any()}.
do_get_connection(#state{} = State, Host, Port) ->
  case find_socket(State, Host, Port) of
    {ok, Pid} ->
      {State, {ok, Pid}};
    {error, Reason} ->
      maybe_connect(State, Host, Port, Reason)
  end.

-spec maybe_connect(#state{}, hostname(), portnum(), Reason) ->
        {#state{}, Result} when
          Reason :: not_found | dead_socket(),
          Result :: {ok, pid()} | {error, any()}.
maybe_connect(State, Host, Port, not_found) ->
  %% connect for the first time
  connect(State, Host, Port);
maybe_connect(#state{client_id = ClientId} = State,
                Host, Port, ?dead_since(Ts, Reason)) ->
  case is_cooled_down(Ts, State) of
    true ->
      connect(State, Host, Port);
    false ->
      error_logger:error_msg("~p (re)connect to ~p:~p~n aborted, "
                             "last failure reason:~p",
                             [ClientId, Host, Port, Reason]),
     {State, {error, Reason}}
  end.

-spec connect(#state{}, hostname(), portnum()) -> {#state{}, Result}
        when Result :: {ok, pid()} | {error, any()}.
connect(#state{ client_id = ClientId
                , sockets = Sockets
                } = State, Host, Port) ->
  case brod_sock:start_link(self(), Host, Port, ClientId, []) of
    {ok, Pid} ->
      S = #sock{ endpoint = {Host, Port}
               , sock_pid = Pid
               },
      error_logger:info_msg("~p connected to ~p:~p~n", [ClientId, Host, Port]),
      NewSockets = lists:keystore({Host, Port}, #sock.endpoint, Sockets, S),
      {State#state{sockets = NewSockets}, {ok, Pid}};
    {error, Reason} ->
      error_logger:error_msg("~p failed to connect to ~p:~p~n, reason:~p",
                             [ClientId, Host, Port, Reason]),
      {ok, NewState} = mark_socket_dead(State, {Host, Port}, Reason),
      {NewState, {error, Reason}}
  end.

%% @private Handle socket pid EXIT event, keep the timestamp.
%% But do not restart yet. Connection will be re-established when a
%% per-partition producer restarts and requests for a connection after
%% it is cooled down.
%% @end
-spec handle_socket_down(#state{}, pid(), any()) -> {ok, #state{}}.
handle_socket_down(#state{ client_id = ClientId
                         , sockets = Sockets
                         } = State, Pid, Reason) ->
  case lists:keyfind(Pid, #sock.sock_pid, Sockets) of
    #sock{endpoint = {Host, Port} = Endpoint} ->
      error_logger:info_msg("~p socket down ~p:~p", [ClientId, Host, Port]),
      mark_socket_dead(State, Endpoint, Reason)
  end.

-spec mark_socket_dead(#state{}, endpoint(), any()) -> {ok, #state{}}.
mark_socket_dead(#state{sockets = Sockets} = State, Endpoint, Reason) ->
  Conn = #sock{ endpoint = Endpoint
              , sock_pid = ?dead_since(os:timestamp(), Reason)
              },
  NewSockets = lists:keystore(Endpoint, #sock.endpoint, Sockets, Conn),
  {ok, State#state{sockets = NewSockets}}.

-spec find_socket(#state{}, hostname(), portnum()) ->
        {ok, pid()} %% normal case
      | {error, not_found} %% first call
      | {error, dead_socket()}.
find_socket(#state{sockets = Sockets}, Host, Port) ->
  case lists:keyfind({Host, Port}, #sock.endpoint, Sockets) of
    #sock{sock_pid = Pid} when is_pid(Pid)         -> {ok, Pid};
    #sock{sock_pid = ?dead_since(_, _) = NotAlive} -> {error, NotAlive};
    false                                          -> {error, not_found}
  end.

%% @private Check if the socket is down for long enough to retry.
is_cooled_down(Ts, #state{config = Config}) ->
  Threshold = proplists:get_value(reconnect_cool_down_seconds, Config,
                                  ?DEFAULT_RECONNECT_COOL_DOWN_SECONDS),
  Now = os:timestamp(),
  case timer:now_diff(Now, Ts) div 1000000 of
    Diff when Diff > Threshold -> true;
    _                          -> false
  end.

%% @private Establish a dedicated socket to kafka cluster endpoint(s) for
%%          metadata retrievals.
%% NOTE: This socket is not intended for kafka payload. This is to avoid
%%       burst of connection usage when many partition producers (re)start
%%       at same time, if we use brod_util:get_metadata/2 to fetch metadata.
%% NOTE: crash in case failed to connect to all of the endpoints.
%%       should be restarted by supervisor.
%% @end
-spec start_metadata_socket([endpoint()]) -> pid() | no_return().
start_metadata_socket([_|_] = Endpoints) ->
  start_metadata_socket(Endpoints, ?undef).

start_metadata_socket([], Reason) ->
  erlang:error(Reason);
start_metadata_socket([Endpoint | Endpoints], _Reason) ->
  case brod_utils:try_connect([Endpoint]) of
    {ok, Pid}       -> #sock{ endpoint = Endpoint
                            , sock_pid = Pid
                            };
    {error, Reason} -> start_metadata_socket(Endpoints, Reason)
  end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
