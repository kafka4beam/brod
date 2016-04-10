%%%
%%%   Copyright (c) 2016, Klarna AB
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
%%% @copyright 2016 Klarna AB
%%% @end
%%% ============================================================================

%%%_* Module declaration =======================================================
%% @private
-module(brod_sock).

%%%_* Exports ==================================================================

%% API
-export([ start/4
        , start/5
        , start_link/4
        , start_link/5
        , stop/1
        ]).

-export([ request_sync/3
        , request_async/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%%_* Includes =================================================================
-include("brod_int.hrl").

%%%_* Records ==================================================================
-record(state, { client_id   :: binary()
               , socket      :: gen_tcp:socket()
               , tail        :: binary() %% leftover of last data stream
               , requests    :: brod_kafka_requests:requests()
               }).

%%%_* API ======================================================================

-spec start_link(pid(), string(), integer(), client_id() | binary()) -> {'ok', pid()} | {'error', any()}.
start_link(Parent, Host, Port, ClientId) ->
    start_link(Parent, Host, Port, ClientId, []).

-spec start_link(pid(), string(), integer(), client_id() | binary(), list()) -> {'ok', pid()} | {'error', any()}.
start_link(_Parent, Host, Port, ClientId, Debug) ->
    gen_server:start_link(?MODULE, [Host, Port, ClientId], Debug).

-spec start(pid(), string(), integer(), client_id() | binary()) -> {'ok', pid()} | {'error', any()}.
start(Parent, Host, Port, ClientId) ->
    start(Parent, Host, Port, ClientId, []).

-spec start(pid(), string(), integer(), client_id() | binary(), list()) -> {'ok', pid()} | {'error', any()}.
start(_Parent, Host, Port, ClientId, Debug) ->
    gen_server:start(?MODULE, [Host, Port, ClientId], Debug).

-spec stop(pid()) -> 'ok'.
stop(Pid) ->
    try
        gen_server:call(Pid, stop)
    catch
        exit:{noproc, {gen_server, call, _}} ->
            ok
    end.


-spec request_async(pid(), term()) -> 'ok' | {'ok', corr_id()} | {'error', any()}.
request_async(Pid, Request) ->
    try
        gen_server:call(Pid, {send, Request})
    catch
        _Class:Reason ->
            {error, {sock_down, Reason}}
    end.

-spec request_sync(pid(), term(), non_neg_integer()) -> 'ok' | {'ok', corr_id()} | {'error', any()}.
request_sync(Pid, Request, Timeout) ->
    try gen_server:call(Pid, {send, Request}) of
        {ok, CorrId} ->
            wait_for_response(Pid, Request, CorrId, Timeout);
        Error ->
            Error
    catch
        _Class:Reason ->
            {error, {sock_down, Reason}}
    end.

%%%_* gen_server callbacks =====================================================

init([Host, Port, ClientId]) ->
    %% delay_send = flase -> don't queue up messages in erlang driver
    %% nodelay = true -> use TCP_NODELAY socket option for send messages immediately
    case gen_tcp:connect(Host, Port, [{mode, binary}, {reuseaddr, true}, {active, true}, {packet, raw}, {delay_send, false}, {nodelay, true}, {send_timeout, 1000}, {send_timeout_close, true}]) of
        {ok, Socket} ->
            {ok, #state{ client_id = brod_utils:to_binary(ClientId)
                       , socket    = Socket
                       , tail      = <<>>
                       , requests  = brod_kafka_requests:new()
                       }
            };
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({send, Request}, From, #state{client_id = ClientId, socket = Socket, requests = Requests} = State) ->
    {CorrId, NewRequests} = brod_kafka_requests:add(Requests, From),
    RequestBin = kpro:encode_request(ClientId, CorrId, Request),
    case gen_tcp:send(Socket, RequestBin) of
        ok ->
            {reply, {ok, CorrId}, State#state{requests = NewRequests}};
        {error, Reason} = Error ->
            {stop, Reason, Error, State}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, {error, unknown_msg, _Msg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Msg}, #state{socket = Socket, tail = Tail, requests = Requests} = State) ->
    {Responses, NewTail} = kpro:decode_response(<<Tail/binary, Msg/binary>>),
    NewRequests = process_responses(Responses, Requests),
    {noreply, State#state{tail = NewTail, requests = NewRequests}};

handle_info({tcp_closed, Socket}, #state{socket = Socket} = State) ->
    {stop, tcp_closed, State};

handle_info({tcp_error, Socket, Reason}, #state{socket = Socket} = State) ->
    {stop, {tcp_error, Reason}, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{socket = Socket} = _State) ->
    catch gen_tcp:close(Socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%_* Internal functions =======================================================

process_responses([], Requests) ->
    Requests;

process_responses([#kpro_Response{correlationId = CorrId, responseMessage = Response} | Responses], Requests) ->
    {From, _Tag} = brod_kafka_requests:get_caller(Requests, CorrId),
    From ! {msg, self(), CorrId, Response},
    process_responses(Responses, brod_kafka_requests:del(Requests, CorrId)).

-spec wait_for_response(pid(), term(), integer(), timeout()) -> 'ok' | {'ok', term()} | {'error', any()}.
wait_for_response(_Pid, #kpro_ProduceRequest{requiredAcks = 0}, _CorrId, _Timeout) ->
    ok;

wait_for_response(Pid, _Request, CorrId, Timeout) ->
    MRef = erlang:monitor(process, Pid),
    receive
        {msg, Pid, CorrId, Response} ->
            erlang:demonitor(MRef, [flush]),
            {ok, Response};
        {'DOWN', MRef, _, _, Reason} ->
            {error, {sock_down, Reason}}
    after
        Timeout ->
            erlang:demonitor(MRef, [flush]),
            {error, timeout}
    end.

