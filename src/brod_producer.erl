%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).

-behaviour(gen_server).

%% Server API
-export([ start_link/2
        , start_link/3
        , stop/1
        ]).

%% Kafka API
-export([ produce/5
        ]).

%% Debug API
-export([ debug/2
        , get_sockets/1
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
               , flush_threshold      :: integer()
               , flush_timeout        :: integer()
               , sockets              :: [#socket{}]
               , leaders              :: [{{binary(), integer()}, pid()}]
               , buffer  = dict:new() :: dict() % {Topic, Partition} -> [msg]
               , unacked = []         :: [{integer(), term()}]
               , buffering = false    :: boolean()
               }).

%%%_* Macros -------------------------------------------------------------------
-define(acks, 1).       % default required acks
-define(timeout, 1000). % default broker timeout to wait for required acks

-define(flush_threshold, 100000). % default buffer limit in bytes
-define(flush_timeout, 100).      % default buffer flush timeout

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options) ->
  start_link(Hosts, Options, []).

-spec start_link([{string(), integer()}], [term()], [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options, Debug) ->
  gen_server:start_link(?MODULE, [Hosts, Options, Debug], [{debug, Debug}]).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 ok | {error, any()}.
produce(Pid, Topic, Partition, Key, Value) ->
  gen_server:call(Pid, {produce, Topic, Partition, Key, Value}).

-spec debug(pid(), sys:dbg_opt()) -> ok.
%% @doc Enable/disabling debugging on brod_producer and on all connections
%%      to brokers managed by the given brod_producer instance.
%%      Enable:
%%        brod_producer:debug(Conn, {log, print}).
%%        brod_producer:debug(Conn, {trace, true}).
%%      Disable:
%%        brod_producer:debug(Conn, no_debug).
debug(Pid, Debug) ->
  {ok, Sockets} = get_sockets(Pid),
  lists:foreach(
    fun(#socket{pid = SocketPid}) ->
        {ok, _} = gen:call(SocketPid, system, {debug, Debug})
    end, Sockets),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

-spec get_sockets(pid()) -> {ok, [#socket{}]}.
get_sockets(Pid) ->
  gen_server:call(Pid, get_sockets).

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, Options, Debug]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Metadata} = brod_utils:fetch_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  %% connect to all known nodes which are alive and map node id to connection
  Sockets = lists:foldl(
              fun(#broker_metadata{node_id = Id, host = H, port = P}, Acc) ->
                  %% keep alive nodes only
                  case brod_sock:start_link(self(), H, P, Debug) of
                    {ok, Pid} ->
                      [#socket{ pid = Pid
                              , host = H
                              , port = P
                              , node_id = Id} | Acc];
                    _ ->
                      Acc
                  end
              end, [], Brokers),
  %% map {Topic, Partition} to connection, crash if a node which must be
  %% a leader is not alive
  Leaders =
    lists:foldl(
      fun(#topic_metadata{name = Topic, partitions = Ps}, AccExt) ->
          lists:foldl(
            fun(#partition_metadata{id = Id, leader_id = LeaderId}, AccInt) ->
                #socket{pid = Pid} =
                  lists:keyfind(LeaderId, #socket.node_id, Sockets),
                [{{Topic, Id}, Pid} | AccInt]
            end, AccExt, Ps)
      end, [], Topics),
  {ok, #state{ hosts   = Hosts
             , acks    = proplists:get_value(required_acks, Options, ?acks)
             , timeout = proplists:get_value(timeout, Options, ?timeout)
             , flush_threshold =
                 proplists:get_value(flush_threshold, Options, ?flush_threshold)
             , flush_timeout =
                 proplists:get_value(flush_timeout, Options, ?flush_timeout)
             , sockets = Sockets
             , leaders = Leaders}}.

handle_call({produce, Topic, Partition, Key, Value}, _From, State0) ->
  Buffer0 = State0#state.buffer,
  Buffer = dict:append({Topic, Partition}, {Key, Value}, Buffer0),
  %% may be flush buffer
  {ok, State} = maybe_send(State0#state{buffer = Buffer}),
  {reply, ok, State, State#state.flush_timeout};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_sockets, _From, State) ->
  {reply, {ok, State#state.sockets}, State};
handle_call(Request, _From, State) ->
  {stop, {unsupported_call, Request}, State}.

handle_cast(Msg, State) ->
  {stop, {unsupported_cast, Msg}, State}.

handle_info({msg, Pid, CorrId, #produce_response{}}, State0) ->
  %% TODO: keep offsets?
  Unacked = lists:keydelete({Pid, CorrId}, 1, State0#state.unacked),
  {ok, State} = maybe_send(State0#state{unacked = Unacked}),
  {noreply, State};
handle_info(timeout, State0) ->
  {ok, State} = case dict:size(State0#state.buffer) of
                  X when X > 0 -> do_send(State0);
                  _            -> {ok, State0}
                end,
  {noreply, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
  %% TODO: reconnect and re-send
  {stop, {socket_down, Reason}, State};
handle_info(Info, State) ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, State) ->
  F = fun(#socket{pid = Pid}) -> brod_sock:stop(Pid) end,
  lists:foreach(F, State#state.sockets).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
maybe_send(#state{buffering = true} = State) ->
  {ok, State};
maybe_send(#state{buffer = Buffer} = State) ->
  case byte_size(term_to_binary(Buffer)) of
    X when X >= State#state.flush_threshold -> do_send(State);
    _                                       -> {ok, State}
  end.

do_send(#state{buffer = Buffer, unacked = Unacked0} = State) ->
  %% distribute buffered data on corresponding leaders
  F1 = fun({_Topic, _Partition} = Key, Msgs, Dict) ->
          {_, Pid} = lists:keyfind(Key, 1, State#state.leaders),
          dict:append(Pid, {Key, Msgs}, Dict)
      end,
  LeadersData = dict:fold(F1, dict:new(), Buffer),
  %% send out buffered data
  F2 = fun(Pid, Data, Acc) ->
           Produce = #produce_request{ acks = State#state.acks
                                     , timeout = State#state.timeout
                                     , data = Data},
           {ok, CorrId} = brod_sock:send(Pid, Produce),
           [{{Pid, CorrId}, Data} | Acc]
       end,
  Unacked = dict:fold(F2, Unacked0, LeadersData),
  {ok, State#state{buffer = dict:new(), unacked = Unacked}}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
