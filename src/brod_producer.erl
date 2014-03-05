%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_producer).

-behaviour(gen_server).

%% Server API
-export([ start_link/2
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
               , flush_threshold      :: integer()
               , flush_timeout        :: integer()
               , brokers              :: [{integer(), pid()}]
               , leaders              :: [{{binary(), integer()}, pid()}]
               , buffer  = dict:new() :: dict() % {Topic, Partition} -> [msg]
               , unacked = []         :: [{integer(), term()}]
               , buffering = false    :: boolean()
               }).

%%%_* Macros -------------------------------------------------------------------
-define(empty_dict, dict:new()).
-define(acks, 1).       % default required acks
-define(timeout, 1000). % default broker timeout to wait for required acks

-define(flush_threshold, 100000). % default buffer limit in bytes
-define(flush_timeout, 100).      % default buffer flush timeout

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Options) ->
  gen_server:start_link(?MODULE, [Hosts, Options], []).

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
init([Hosts, Options]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Metadata} = brod_utils:fetch_metadata(Hosts),
  #metadata_response{brokers = BrokersMetadata, topics = Topics} = Metadata,
  %% connect to all known nodes which are alive and map node id to connection
  Brokers = lists:foldl(
                 fun(#broker_metadata{node_id = Id, host = H, port = P}, Acc) ->
                     %% keep alive nodes only
                     case brod_sock:start_link(self(), H, P, []) of
                       {ok, Pid} -> [{Id, Pid} | Acc];
                       _         -> Acc
                     end
                 end, [], BrokersMetadata),
  %% map {Topic, Partition} to connection, crash if a node which must be
  %% a leader is not alive
  Leaders =
    lists:foldl(
      fun(#topic_metadata{name = Topic, partitions = Ps}, AccExt) ->
          lists:foldl(
            fun(#partition_metadata{id = Id, leader_id = LeaderId}, AccInt) ->
                {_, Pid} = lists:keyfind(LeaderId, 1, Brokers),
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
             , brokers = Brokers
             , leaders = Leaders}}.

handle_call({produce, Topic, Partition, Key, Value}, _From, State0) ->
  Buffer0 = State0#state.buffer,
  Buffer = dict:append({Topic, Partition}, {Key, Value}, Buffer0),
  %% may be flush buffer
  {ok, State} = maybe_send(State0#state{buffer = Buffer}),
  {reply, ok, State, State#state.flush_timeout};
handle_call(stop, _From, State) ->
  close_sockets(State#state.brokers),
  {stop, normal, ok, State};
handle_call({debug, Debug}, _From, State) ->
  lists:foreach(fun({_, Pid}) ->
                    {ok, _} = gen:call(Pid, system, {debug, Debug})
                end, State#state.brokers),
  {reply, ok, State};
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
  close_sockets(State#state.brokers),
  ok.

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
          {_, Leader} = lists:keyfind(Key, 1, State#state.leaders),
          dict:append(Leader, {Key, Msgs}, Dict)
      end,
  LeadersData = dict:fold(F1, dict:new(), Buffer),
  %% send out buffered data
  F2 = fun(Leader, Data, Acc) ->
           Produce = #produce_request{ acks = State#state.acks
                                     , timeout = State#state.timeout
                                     , data = Data},
           {ok, CorrId} = brod_sock:send(Leader, Produce),
           [{{Leader, CorrId}, Data} | Acc]
       end,
  Unacked = dict:fold(F2, Unacked0, LeadersData),
  {ok, State#state{buffer = dict:new(), unacked = Unacked}}.

close_sockets(Brokers) ->
  F = fun({_, Pid}) -> brod_sock:stop(Pid) end,
  lists:foreach(F, Brokers).

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
