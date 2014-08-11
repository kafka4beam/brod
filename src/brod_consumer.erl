%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_consumer).

-behaviour(gen_server).

%% Server API
-export([ start_link/3
        , start_link/4
        , stop/1
        ]).

%% Kafka API
-export([ consume/6
        ]).

%% Debug API
-export([ debug/2
        , get_socket/1
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
-record(state, { hosts         :: [{string(), integer()}]
               , socket        :: #socket{}
               , topic         :: binary()
               , partition     :: integer()
               , subscriber    :: pid()
               , offset        :: integer()
               , max_wait_time :: integer()
               , min_bytes     :: integer()
               , max_bytes     :: integer()
               }).

%%%_* Macros -------------------------------------------------------------------

%%%_* API ----------------------------------------------------------------------
-spec start_link([{string(), integer()}], binary(), integer()) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Topic, Partition) ->
  start_link(Hosts, Topic, Partition, []).

-spec start_link([{string(), integer()}], binary(), integer(), [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(Hosts, Topic, Partition, Debug) ->
  Args = [Hosts, Topic, Partition, Debug],
  Options = [{debug, Debug}],
  gen_server:start_link(?MODULE, Args, Options).

-spec stop(pid()) -> ok | {error, any()}.
stop(Pid) ->
  gen_server:call(Pid, stop).

-spec consume(pid(), pid(), integer(), integer(), integer(), integer()) ->
                 ok | {error, any()}.
consume(Pid, Subscriber, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  gen_server:call(Pid, {consume, Subscriber, Offset,
                        MaxWaitTime, MinBytes, MaxBytes}).

-spec debug(pid(), sys:dbg_opt()) -> ok.
%% @doc Enable/disabling debugging on brod_consumer and its connection
%%      to broker.
%%      Enable:
%%        brod_consumer:debug(Pid, {log, print}).
%%        brod_consumer:debug(Pid, {trace, true}).
%%      Disable:
%%        brod_consumer:debug(Pid, no_debug).
debug(Pid, Debug) ->
  {ok, #socket{pid = Sock}} = get_socket(Pid),
  {ok, _} = gen:call(Sock, system, {debug, Debug}),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

-spec get_socket(pid()) -> {ok, #socket{}}.
get_socket(Pid) ->
  gen_server:call(Pid, get_socket).

%%%_* gen_server callbacks -----------------------------------------------------
init([Hosts, Topic, Partition, Debug]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Metadata} = brod_utils:get_metadata(Hosts),
  #metadata_response{brokers = Brokers, topics = Topics} = Metadata,
  try
    %% detect a leader for the given partition and connect it
    Partitions =
      case lists:keyfind(Topic, #topic_metadata.name, Topics) of
        #topic_metadata{partitions = Ps} -> Ps;
        false                            -> throw(topic_not_found)
      end,
    NodeId =
      case lists:keyfind(Partition, #partition_metadata.id, Partitions) of
        #partition_metadata{leader_id = Id} -> Id;
        false                               -> throw(partition_not_found)
      end,
    Broker = lists:keyfind(NodeId, #broker_metadata.node_id, Brokers),
    Host = Broker#broker_metadata.host,
    Port = Broker#broker_metadata.port,
    {ok, Pid} = brod_sock:start_link(self(), Host, Port, Debug),
    Socket = #socket{ pid = Pid
                    , host = Host
                    , port = Port
                    , node_id = NodeId},
    {ok, #state{ hosts     = Hosts
               , socket    = Socket
               , topic     = Topic
               , partition = Partition}}
  catch
    throw:What ->
      {stop, What}
  end.

handle_call({consume, Subscriber, Offset0,
             MaxWaitTime, MinBytes, MaxBytes}, _From, State0) ->
  case get_valid_offset(Offset0, State0) of
    {error, Error} ->
      {reply, {error, Error}, State0};
    {ok, Offset} ->
      State = State0#state{ subscriber = Subscriber
                          , offset = Offset
                          , max_wait_time = MaxWaitTime
                          , min_bytes = MinBytes
                          , max_bytes = MaxBytes},
      ok = send_fetch_request(State),
      {reply, ok, State}
  end;
handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_socket, _From, State) ->
  {reply, {ok, State#state.socket}, State};
handle_call(Request, _From, State) ->
  {stop, {unsupported_call, Request}, State}.

handle_cast(Msg, State) ->
  {stop, {unsupported_cast, Msg}, State}.

handle_info({msg, _Pid, _CorrId, #fetch_response{} = R}, State0) ->
  #fetch_response{topics = [TopicFetchData]} = R,
  #topic_fetch_data{topic = Topic, partitions = [Msgs]} = TopicFetchData,
  ok = maybe_send_subscriber(State0#state.subscriber, Topic, Msgs),
  LastOffset = Msgs#partition_messages.last_offset,
  case Msgs#partition_messages.messages of
    [] ->
      %% do not bump offset when we get nothing, repeat request later
      {noreply, State0, State0#state.max_wait_time};
    _ ->
      Offset = erlang:max(State0#state.offset, LastOffset + 1),
      State = State0#state{offset = Offset},
      ok = send_fetch_request(State),
      {noreply, State}
  end;
handle_info(timeout, State) ->
  ok = send_fetch_request(State),
  {noreply, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
  {stop, {socket_down, Reason}, State};
handle_info(Info, State) ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, #state{socket = Socket}) ->
  brod_sock:stop(Socket#socket.pid).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
get_valid_offset(Offset, _State) when Offset > 0 ->
  {ok, Offset};
get_valid_offset(Time, #state{socket = Socket} = State) ->
  Request = #offset_request{ topic = State#state.topic
                           , partition = State#state.partition
                           , time = Time},
  {ok, Response} = brod_sock:send_sync(Socket#socket.pid, Request, 5000),
  #offset_response{topics = [#offset_topic{} = Topic]} = Response,
  #offset_topic{partitions =
                 [#partition_offsets{offsets = Offsets}]} = Topic,
  case Offsets of
    [Offset] -> {ok, Offset};
    []       -> {error, no_available_offsets}
  end.

send_fetch_request(#state{socket = Socket} = State) ->
  Request = #fetch_request{ max_wait_time = State#state.max_wait_time
                          , min_bytes = State#state.min_bytes
                          , topic = State#state.topic
                          , partition = State#state.partition
                          , offset = State#state.offset
                          , max_bytes = State#state.max_bytes},
  {ok, _} = brod_sock:send(Socket#socket.pid, Request),
  ok.

maybe_send_subscriber(_, _, #partition_messages{messages = []}) ->
  ok;
maybe_send_subscriber(Subscriber, Topic, Msgs) ->
  #partition_messages{ partition = Partition
                     , high_wm_offset = HighWmOffset
                     , messages = Messages} = Msgs,
  MsgSet = #message_set{ topic = Topic
                       , partition = Partition
                       , high_wm_offset = HighWmOffset
                       , messages = Messages},
  Subscriber ! MsgSet,
  ok.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
