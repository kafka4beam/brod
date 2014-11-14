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
        , no_debug/1
        , get_socket/1
        ]).


%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
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

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable debugging on consumer and its connection to a broker
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
debug(Pid, print) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log, print});
debug(Pid, File) when is_list(File) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log_to_file, File}).

-spec no_debug(pid()) -> ok.
%% @doc Disable debugging
no_debug(Pid) ->
  do_debug(Pid, no_debug).

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
        #topic_metadata{} = TM ->
          TM#topic_metadata.partitions;
        false ->
          throw({unknown_topic, Topic})
      end,
    NodeId =
      case lists:keyfind(Partition, #partition_metadata.id, Partitions) of
        #partition_metadata{leader_id = Id} ->
          Id;
        false ->
          throw({unknown_partition, Topic, Partition})
      end,
    #broker_metadata{host = Host, port = Port} =
      lists:keyfind(NodeId, #broker_metadata.node_id, Brokers),
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
  #topic_fetch_data{topic = Topic, partitions = [PM]} = TopicFetchData,
  case has_error(PM) of
    {true, Error} ->
      {stop, {broker_error, Error}, State0};
    false ->
      ok = maybe_send_subscriber(State0#state.subscriber, Topic, PM),
      LastOffset = PM#partition_messages.last_offset,
      case PM#partition_messages.messages of
        [] ->
          %% do not bump offset when we get nothing, repeat request later
          {noreply, State0, State0#state.max_wait_time};
        X when is_list(X) ->
          Offset = erlang:max(State0#state.offset, LastOffset + 1),
          State = State0#state{offset = Offset},
          ok = send_fetch_request(State),
          {noreply, State}
      end
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

format_status(_Opt, [_PDict, State0]) ->
  State = lists:zip(record_info(fields, state), tl(tuple_to_list(State0))),
  [{data, [{"State", State}]}].

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
maybe_send_subscriber(Subscriber, Topic, PM) ->
  #partition_messages{ partition = Partition
                     , high_wm_offset = HighWmOffset
                     , messages = Messages} = PM,
  MsgSet = #message_set{ topic = Topic
                       , partition = Partition
                       , high_wm_offset = HighWmOffset
                       , messages = Messages},
  Subscriber ! MsgSet,
  ok.

has_error(#partition_messages{error_code = ErrorCode}) ->
  case kafka:is_error(ErrorCode) of
    true  -> {true, kafka:error_code_to_atom(ErrorCode)};
    false -> false
  end.

do_debug(Pid, Debug) ->
  {ok, #socket{pid = Sock}} = get_socket(Pid),
  {ok, _} = gen:call(Sock, system, {debug, Debug}),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

%% Tests -----------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
