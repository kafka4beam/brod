%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).

-behaviour(gen_server).

%% Server API
-export([ start_link/3
        , stop/1
        ]).

%% Kafka API
-export([ produce/5
        ]).

%% Debug API
-export([ debug/2
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
-record(state, { hosts                :: [{string(), integer()}]
               , acks                 :: integer()
               , timeout              :: integer()
               , brokers              :: dict() % Id -> pid
               , leaders              :: dict() % {Topic, Partition} -> pid
               , buffers = dict:new() :: dict() % pid -> dict()
               , unacked = dict:new() :: dict() % pid -> dict()

               , buffering = false    :: boolean()
               }).

%%%_* Macros -------------------------------------------------------------------
-define(empty_dict, dict:new()).

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], integer(), integer()) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, RequiredAcks, Timeout) ->
  gen_server:start_link(?MODULE, [Hosts, RequiredAcks, Timeout], []).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 ok | {error, any()}.
produce(Pid, Topic, Partition, Key, Value) ->
  gen_server:call(Pid, {produce, Topic, Partition, Key, Value}).

-spec debug(pid(), [sys:dbg_opt()]) -> ok.
%% @doc Enable/disabling debugging on brod_srv and on all connections
%%      to brokers managed by the given brod_srv instance.
%%      Enable:
%%        brod_srv:debug(Conn, {log, print}).
%%        brod_srv:debug(Conn, {trace, true}).
%%      Disable:
%%        brod_srv:debug(Conn, no_debug).
debug(Pid, Debug) ->
  ok = gen_server:call(Pid, {debug, Debug}),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, RequiredAcks, Timeout]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Metadata} = brod_utils:fetch_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  %% connect to all known nodes which are alive and map node id to connection
  BrokersMap = lists:foldl(
                 fun(#broker_metadata{node_id = Id, host = H, port = P}, D) ->
                     %% keep alive nodes only
                     case brod_sock:start_link(self(), H, P, []) of
                       {ok, Pid} -> dict:store(Id, Pid, D);
                       _         -> D
                     end
                 end, dict:new(), Brokers),
  %% map {Topic, Partition} to connection, crash if a node which must be
  %% a leader is not alive
  LeadersMap =
    lists:foldl(
      fun(#topic_metadata{name = Topic, partitions = Ps}, D) ->
          lists:foldl(
            fun(#partition_metadata{id = Id, leader_id = LeaderId}, Dd) ->
                Pid = dict:fetch(LeaderId, BrokersMap),
                dict:store({Topic, Id}, Pid, Dd)
            end, D, Ps)
      end, dict:new(), Topics),
  {ok, #state{ hosts   = Hosts
             , acks    = RequiredAcks
             , timeout = Timeout
             , brokers = BrokersMap
             , leaders = LeadersMap}}.

handle_call({produce, Topic, Partition, Key, Value}, _From, State0) ->
  %% lookup a leader for the Partition
  Leader = dict:fetch({Topic, Partition}, State0#state.leaders),
  %% save data in a buffer for partition leader
  Topics0 = try_fetch(Leader, State0#state.buffers, ?empty_dict),
  Partitions0 = try_fetch(Topic, Topics0, ?empty_dict),
  Partitions = dict:append(Partition, {Key, Value}, Partitions0),
  Topics = dict:store(Topic, Partitions, Topics0),
  Buffers = dict:store(Leader, Topics, State0#state.buffers),
  %% may be flush buffers
  {ok, State} = maybe_send(State0#state{buffers = Buffers}),
  {reply, ok, State};
handle_call(stop, _From, State) ->
  %% TODO: close all sockets
  {stop, normal, ok, State};
handle_call({debug, Debug}, _From, State) ->
  Pids = [Pid || {_, Pid} <- dict:to_list(State#state.brokers)],
  lists:foreach(fun(Pid) ->
                    {ok, _} = gen:call(Pid, system, {debug, Debug})
                end, Pids),
  {reply, ok, State};
handle_call(Request, _From, State) ->
  {stop, {unsupported_call, Request}, State}.

handle_cast(Msg, State) ->
  {stop, {unsupported_cast, Msg}, State}.

handle_info({msg, Pid, CorrId, #produce_response{}}, State0) ->
  %% TODO: keep offsets?
  UnackedTopics0 = dict:fetch(Pid, State0#state.unacked),
  UnackedTopics = dict:erase(CorrId, UnackedTopics0),
  Unacked = dict:store(Pid, UnackedTopics, State0#state.unacked),
  {ok, State} = maybe_send(State0#state{unacked = Unacked}),
  {noreply, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
  %% TODO: reconnect and re-send
  {stop, {socket_down, Reason}, State};
handle_info(Info, State) ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, _State) ->
  %% TODO: close connections
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
maybe_send(#state{buffering = true} = State) ->
  {ok, State};
maybe_send(#state{buffers = Buffers, unacked = Unacked} = State) ->
  Pids = dict:fetch_keys(Buffers),
  F = fun(Pid, {NewBuffers, NewUnacked}) ->
          UnackedTopics0 = try_fetch(Pid, NewUnacked, ?empty_dict),
          case dict:size(UnackedTopics0) of
            0 ->
              Topics = dict:fetch(Pid, NewBuffers),
              Produce = #produce_request{ acks = State#state.acks
                                        , timeout = State#state.timeout
                                        , topics = Topics},
              {ok, CorrId} = brod_sock:send(Pid, Produce),
              UnackedTopics = dict:store(CorrId, Topics, UnackedTopics0),
              { dict:erase(Pid, NewBuffers)
              , dict:store(Pid, UnackedTopics, NewUnacked)};
            _N ->
              %% do not send new message if there are still unacked for
              %% this broker
              %% TODO: use configurable size of max message buffer instead?
              {NewBuffers, NewUnacked}
          end
      end,
  {NewBuffers, NewUnacked} = lists:foldl(F, {Buffers, Unacked}, Pids),
  {ok, State#state{buffers = NewBuffers, unacked = NewUnacked}}.

try_fetch(Key, Dict, Default) ->
  case dict:find(Key, Dict) of
    {ok, Value} -> Value;
    error       -> Default
  end.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
