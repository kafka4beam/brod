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

-module(brod_producer_worker).

-behaviour(gen_server).

%% Server API
-export([ start_link/5
        , start_link/6
        ]).

%% Kafka API
-export([ produce/2
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
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------

-record(state, { parent_pid             :: pid()
               , leader_id              :: leader_id()
               , host                   :: string()
               , port                   :: integer()
               , debug                  :: [term()]
               , client_id              :: client_id()
               , socket_pid             :: pid()
               , acks                   :: integer()
               , ack_timeout            :: integer()
                 %% = how many outstanding kafka acknowledgements
                 %% a worker is allowed to tolerate.
                 %% When this option is > 0 worker will be sending new
                 %% produce requests to a broker even before it
                 %% received acks for already sent ones.
               , max_requests_in_flight :: integer()
                 %% Buffer for incoming requests.
                 %% Requests are buffered when
                 %% len(pending) > max_requests_in_flight.
                 %% Buffer is sent in one produce request as one
                 %% message_set as soon as a worker is allowed to
                 %% send again
               , buffer = []            :: produce_request_data()
                 %% list of requests references in the current buffer
               , request_refs = []      :: [reference()]
                 %% list of requests already sent to broker
               , pending = queue:new()  :: queue:queue()
               }).

-type produce_error() :: {partition(), offset(), error_code()}.

%%%_* Macros -------------------------------------------------------------------
%% default required acks
-define(DEFAULT_ACKS, -1).
%% default ack timeout
-define(DEFAULT_ACK_TIMEOUT, 1000).
%% allow worker to send next request to kafka without waiting
%% for response on the previous one and limit max number
%% of unacked requests
-define(DEFAULT_MAX_REQUESTS_IN_FLIGHT, 5).

%%%_* API ----------------------------------------------------------------------
-spec start_link(pid(), leader_id(), string(),
                 integer(), proplists:proplist()) ->
                    {ok, pid()} | {error, any()}.
start_link(ParentPid, Id, Host, Port, Options) ->
  start_link(ParentPid, Id, Host, Port, Options, []).

-spec start_link(pid(), leader_id(), string(),
                 integer(), proplists:proplist(), [term()]) ->
                    {ok, pid()} | {error, any()}.
start_link(ParentPid, Id, Host, Port, Options, Debug) ->
  gen_server:start_link(?MODULE, [ParentPid, Id, Host, Port, Options, Debug],
                        [{debug, Debug}]).

-spec produce(pid(), reference()) -> ok.
produce(Pid, RequestRef) ->
  gen_server:cast(Pid, {produce, RequestRef}).

-spec debug(pid(), print | string() | none) -> ok.
%% @doc Enable debugging on producer and its connection to a broker
%%      debug(Pid, pring) prints debug info on stdout
%%      debug(Pid, File) prints debug info into a File
debug(Pid, print) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log, print});
debug(Pid, File) when is_list(File) ->
  do_debug(Pid, {trace, true}),
  do_debug(Pid, {log_to_file, File}).

-spec no_debug(pid()) -> oqk.
%% @doc Disable debugging
no_debug(Pid) ->
  do_debug(Pid, no_debug).

-spec get_socket(pid()) -> {ok, #socket{}}.
get_socket(Pid) ->
  gen_server:call(Pid, get_socket).

%%%_* gen_server callbacks -----------------------------------------------------
init([ParentPid, LeaderId, Host, Port, Options, Debug]) ->
  ClientId = ensure_binary(
               proplists:get_value(client_id, Options, ?DEFAULT_CLIENT_ID)),
  RequiredAcks = proplists:get_value(required_acks, Options, ?DEFAULT_ACKS),
  AckTimeout = proplists:get_value(ack_timeout, Options, ?DEFAULT_ACK_TIMEOUT),
  MaxRequestsInFlight = proplists:get_value(max_requests_in_flight, Options,
                                            ?DEFAULT_MAX_REQUESTS_IN_FLIGHT),
  error_logger:info_msg("Starting worker ~p for broker #~B at ~s:~B~n",
                        [self(), LeaderId, Host, Port]),
  %% do not try to connect in init/1
  %% if this operation fails, supervisor and brod_producer will crash
  gen_server:cast(self(), connect),
  {ok, #state{ parent_pid             = ParentPid
             , leader_id              = LeaderId
             , host                   = Host
             , port                   = Port
             , debug                  = Debug
             , client_id              = ClientId
             , acks                   = RequiredAcks
             , ack_timeout            = AckTimeout
             , max_requests_in_flight = MaxRequestsInFlight
             }}.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(get_socket, _From, State) ->
  {reply, {ok, State#state.socket_pid}, State};
handle_call(Request, _From, State) ->
  {reply, {error, {unsupported_call, Request}}, State}.

handle_cast(connect, State) ->
  {ok, SocketPid} = connect( State#state.host
                           , State#state.port
                           , State#state.client_id
                           , State#state.debug),
  ParentPid = State#state.parent_pid,
  LeaderId = State#state.leader_id,
  ok = brod_producer:subscribe(ParentPid, self(), LeaderId),
  {noreply, State#state{socket_pid = SocketPid}};
handle_cast({produce, RequestRef}, #state{acks = 0} = State) ->
  %% send right away
  SocketPid = State#state.socket_pid,
  Acks = 0,
  AckTimeout = State#state.ack_timeout,
  {ok, Request} = brod_producer:get_request(RequestRef),
  Topic = Request#request.topic,
  Partition = Request#request.partition,
  Data0 = Request#request.data,
  Data = [{Topic, dict:append_list(Partition, Data0, dict:new())}],
  %% crash on error
  ok = send_request(SocketPid, Acks, AckTimeout, Data),
  ack_request(State#state.parent_pid, RequestRef),
  {noreply, State};
handle_cast({produce, RequestRef}, State0) ->
  %% add data to the buffer and try to send
  {ok, Request} = brod_producer:get_request(RequestRef),
  Topic = Request#request.topic,
  Partition = Request#request.partition,
  {ok, Request} = brod_producer:get_request(RequestRef),
  Data = Request#request.data,
  {TopicBuffer0, Buffer0} =
    case lists:keytake(Request#request.topic, 1, State0#state.buffer) of
      {value, {_, TopicBuffer_}, List} ->
        {TopicBuffer_, List};
      false ->
        {dict:new(), State0#state.buffer}
    end,
  TopicBuffer = dict:append_list(Partition, Data, TopicBuffer0),
  Buffer = [{Topic, TopicBuffer} | Buffer0],
  RequestRefs = [Request#request.ref | State0#state.request_refs],
  State1 = State0#state{buffer = Buffer, request_refs = RequestRefs},
  {ok, State} = maybe_send(State1),
  {noreply, State};
handle_cast(Msg, State) ->
  error_logger:warning_msg("Unexpected cast in ~p(~p): ~p",
                           [?MODULE, self(), Msg]),
  {noreply, State}.

handle_info({msg, Pid, CorrId, #produce_response{} = R},
            #state{socket_pid = Pid} = State0) ->
  case get_error_codes(R#produce_response.topics) of
    [] ->
      {ok, State} = handle_produce_response(CorrId, State0),
      {noreply, State};
    [_|_] = Errors ->
      %% shutdown on produce errors
      {stop, {broker_errors, Errors}, State0}
  end;
handle_info({'EXIT', Pid, Reason}, #state{socket_pid = Pid} = State) ->
  %% shutdown when socket dies
  {stop, {socket_down, Reason}, State#state{socket_pid = undefined}};
handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected info in ~p: ~p", [?MODULE, Info]),
  {noreply, State}.

terminate(_Reason, #state{socket_pid = Pid}) ->
  case brod_utils:is_pid_alive(Pid) of
    true  -> brod_sock:stop(Pid);
    false -> ok
  end.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%_* Internal functions -------------------------------------------------------
maybe_send(State) ->
  case can_send_now(State) of
    true ->
      SocketPid = State#state.socket_pid,
      Acks = State#state.acks,
      AckTimeout = State#state.ack_timeout,
      Data = State#state.buffer,
      %% crash on error
      {ok, CorrId} = send_request(SocketPid, Acks, AckTimeout, Data),
      Pending = queue:in({CorrId, State#state.request_refs}, State#state.pending),
      {ok, State#state{ buffer = []
                      , request_refs = []
                      , pending = Pending}};
    false ->
      {ok, State}
  end.

can_send_now(State) ->
  queue:len(State#state.pending) < State#state.max_requests_in_flight.

send_request(SocketPid, Acks, AckTimeout, Data) ->
  Produce = #produce_request{ acks    = Acks
                            , timeout = AckTimeout
                            , data    = Data},
  brod_sock:send(SocketPid, Produce).

ack_request(ParentPid, RequestRef) ->
  ParentPid ! {{RequestRef, self()}, ack}.

connect(Host, Port, ClientId, Debug) ->
  brod_sock:start_link(self(), Host, Port, ClientId, Debug).

-spec handle_produce_response(pid(), corr_id()) ->
                                 ok | {error, any()}.
handle_produce_response(CorrId, State) ->
  {{value, {CorrId, RequestRefs}}, Pending} = queue:out(State#state.pending),
  lists:foreach(fun(Ref) ->
                    ack_request(State#state.parent_pid, Ref)
                end, RequestRefs),
   {ok, State#state{pending = Pending}}.

%% @private Flatten the per-topic-partition error codes in produce result.
-spec get_error_codes([#produce_topic{}]) -> [produce_error()].
get_error_codes(Topics) ->
  lists:reverse(
    lists:foldl(
      fun(#produce_topic{offsets = Offsets, topic = Topic}, TopicAcc) ->
        lists:foldl(
          fun(#produce_offset{partition  = Partition,
                              error_code = ErrorCode,
                              offset     = Offset }, OffsetAcc) ->
            case brod_kafka:is_error(ErrorCode) of
              true  ->
                error_logger:error_msg(
                  "Error in produce response\n"
                  "Topic: ~s\n"
                  "Partition: ~B\n"
                  "Offset: ~B\n"
                  "Error code: ~p",
                  [Topic, Partition, Offset, ErrorCode]),
                [{Partition, Offset, ErrorCode} | OffsetAcc];
              false -> OffsetAcc
            end
          end, TopicAcc, Offsets)
      end, [], Topics)).

do_debug(Pid, Debug) ->
  {ok, SocketPid} = get_socket(Pid),
  {ok, _} = gen:call(SocketPid, system, {debug, Debug}),
  {ok, _} = gen:call(Pid, system, {debug, Debug}),
  ok.

ensure_binary(A) when is_atom(A)   -> ensure_binary(atom_to_list(A));
ensure_binary(L) when is_list(L)   -> iolist_to_binary(L);
ensure_binary(B) when is_binary(B) -> B.

%% Tests -----------------------------------------------------------------------
-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

mock_brod_sock() ->
  meck:new(brod_sock, [passthrough]),
  meck:expect(brod_sock, start_link,
              fun(_Parent, "h1", _Port, _ClientId, _Debug) -> {ok, p1};
                 (_Parent, "h2", _Port, _ClientId, _Debug) -> {ok, p2};
                 (_Parent, "h3", _Port, _ClientId, _Debug) -> {ok, p3}
              end),

  meck:expect(brod_sock, start,
              fun(_Parent, "h1", _Port, _ClientId, _Debug) -> {ok, p1};
                 (_Parent, "h2", _Port, _ClientId, _Debug) -> {ok, p2};
                 (_Parent, "h3", _Port, _ClientId, _Debug) -> {ok, p3}
              end),

  meck:expect(brod_sock, stop, 1, ok),
  meck:expect(brod_sock, send, 2, {ok, 1}),

  meck:expect(brod_sock, send_sync,
              fun(p1, #metadata_request{}, _Timeout) ->
                  {ok, #metadata_response{brokers = [], topics = []}};
                 (p2, #metadata_request{}, _Timeout) ->
                  Topics = [#topic_metadata{name = <<"t">>, partitions = []}],
                  {ok, #metadata_response{brokers = [], topics = Topics}};
                 (p3, #metadata_request{}, _Timeout) ->
                  Brokers = [#broker_metadata{ node_id = 1,
                                               host = "h3",
                                               port = 2181}],
                  Partitions = [#partition_metadata{id = 0, leader_id = 1}],
                  Topics = [#topic_metadata{ name = <<"t">>,
                                             partitions = Partitions}],
                  {ok, #metadata_response{brokers = Brokers, topics = Topics}}
              end),
  ok.

validate_mock() ->
  true = meck:validate(brod_sock),
  ok   = meck:unload(brod_sock).

connect_test() ->
  ok = mock_brod_sock(),
  Topic = <<"t">>,
  Partition = 0,
  H1 = {"h1", 2181}, H2 = {"h2", 2181}, H3 = {"h3", 2181},
  State0 = #state{hosts = [H1]},
  Error0 = {error, {unknown_topic, Topic}},
  ?assertEqual(Error0, connect(Topic, Partition, State0)),
  State1 = #state{hosts = [H2]},
  Error1 = {error, {unknown_partition, Topic, Partition}},
  ?assertEqual(Error1, connect(Topic, Partition, State1)),
  State2 = #state{hosts = [H3]},
  Socket = #socket{pid = p3, host = "h3", port = 2181, node_id = 1},
  Leader = {{Topic, Partition}, p3},
  State3 = State2#state{ sockets = [Socket]
                       , leaders = [Leader]},
  ?assertEqual({ok, p3, State3}, connect(Topic, Partition, State2)),
  ok = validate_mock().

maybe_send_test() ->
  ok = mock_brod_sock(),
  State0 = #state{data_buffer = [], pending = queue:new()},
  L1 = erlang:list_to_pid("<0.0.1>"),
  ?assertEqual({ok, State0}, maybe_send(L1, State0)),
  State1 = State0#state{pending = [{L1, 2}]},
  ?assertEqual({ok, State1}, maybe_send(L1, State1)),
  L2 = erlang:list_to_pid("<0.0.2>"),
  SendersBuffer = dict:store(L2, [d], dict:store(L1, [a, b, c], dict:new())),
  State2 = State0#state{ data_buffer = [{L1, foo}, {L2, bar}]
                       , senders_buffer = SendersBuffer},
  ExpectedState = #state{ data_buffer = [{L2, bar}]
                        , senders_buffer = dict:erase(L1, SendersBuffer)
                        , pending = [{L1, {1, [a, b, c]}}]},
  ?assertEqual({ok, ExpectedState}, maybe_send(L1, State2)),
  ok = validate_mock().

handle_produce_test() ->
  ok = mock_brod_sock(),
  T0 = <<"t">>,
  H1 = {"h1", 2181},
  State0 = #state{hosts = [H1]},
  Error0 = {error, {unknown_topic, T0}},
  ?assertEqual(Error0, handle_produce(pid, ref, T0, p0, [{k0, v0}], State0)),

  S1 = s1,
  R1 = r1,
  L1 = 1,
  T1 = t1,
  P1 = p1,
  K1 = k1,
  V1 = v1,
  Leaders0 = [{{T1, P1}, L1}],
  SendersBuffer1 = dict:store(L1, [{s1, r1}], dict:new()),
  TopicBuffer1 = dict:store(P1, [{K1, V1}], dict:new()),
  DataBuffer1 = [{L1, [{T1, TopicBuffer1}]}],
  State1 = #state{leaders = Leaders0},
  {ok, L1, State2} = handle_produce(S1, R1, T1, P1, [{K1, V1}], State1),
  ?assertEqual(Leaders0, State2#state.leaders),
  ?assertEqual(SendersBuffer1, State2#state.senders_buffer),
  ?assertEqual(DataBuffer1, State2#state.data_buffer),

  K2 = k2,
  V2 = v2,
  TopicBuffer2 = dict:append(P1, {K2, V2}, TopicBuffer1),
  DataBuffer2 = [{L1, [{T1, TopicBuffer2}]}],
  SendersBuffer2 = dict:append(L1, {S1, R1}, State2#state.senders_buffer),
  {ok, L1, State3} = handle_produce(S1, R1, T1, P1, [{K2, V2}], State2),
  ?assertEqual(Leaders0, State2#state.leaders),
  ?assertEqual(SendersBuffer2, State3#state.senders_buffer),
  ?assertEqual(DataBuffer2, State3#state.data_buffer),

  S2 = s2,
  R2 = r2,
  L2 = 2,
  P2 = p2,
  K3 = k3,
  V3 = v3,
  SendersBuffer3 = dict:append(L2, {S2, R2}, State3#state.senders_buffer),
  Leaders1 = [{{T1, P2}, L2} | Leaders0],
  State4 = State3#state{leaders = Leaders1},
  TopicBuffer3 = dict:append(P2, {K3, V3}, dict:new()),
  %% ++ due to lists:keystore/4 behaviour
  DataBuffer3 = State4#state.data_buffer ++ [{L2, [{T1, TopicBuffer3}]}],
  {ok, L2, State5} = handle_produce(S2, R2, T1, P2, [{K3, V3}], State4),
  ?assertEqual(Leaders1, State5#state.leaders),
  ?assertEqual(SendersBuffer3, State5#state.senders_buffer),
  ?assertEqual(DataBuffer3, State5#state.data_buffer),

  T2 = t2,
  S3 = s3,
  R3 = r3,
  K4 = k4,
  V4 = v4,
  SendersBuffer5 = dict:append(L1, {S3, R3}, State5#state.senders_buffer),
  Leaders2 = [{{T2, P1}, L1} | Leaders1],
  State6 = State5#state{leaders = Leaders2},
  Topic2Buffer = dict:append(P1, {K4, V4}, dict:new()),
  {value, {L1, [{T1, Topic1Buffer}]}, DataBuffer4} =
    lists:keytake(L1, 1, DataBuffer3),
  DataBuffer5 = [{L1, [{T1, Topic1Buffer}, {T2, Topic2Buffer}]} |
                DataBuffer4],
  {ok, L1, State7} = handle_produce(S3, R3, T2, P1, [{K4, V4}], State6),
  ?assertEqual(Leaders2, State7#state.leaders),
  ?assertEqual(SendersBuffer5, State7#state.senders_buffer),
  ?assertEqual(DataBuffer5, State7#state.data_buffer),
  ok = validate_mock().

-endif. % TEST

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
