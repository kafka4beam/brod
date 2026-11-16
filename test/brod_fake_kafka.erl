%% Test-only Kafka broker used to observe request headers.
%%
%% It answers ApiVersions and FindCoordinator, and closes the connection
%% immediately after a JoinGroup request so the caller can assert the header
%% of the group coordinator connection without implementing the whole group
%% protocol.
-module(brod_fake_kafka).

-behaviour(gen_server).

-export([start_link/1, stop/1, address/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(API_METADATA, 3).
-define(API_OFFSET_COMMIT, 8).
-define(API_OFFSET_FETCH, 9).
-define(API_FIND_COORDINATOR, 10).
-define(API_JOIN_GROUP, 11).
-define(API_HEARTBEAT, 12).
-define(API_LEAVE_GROUP, 13).
-define(API_SYNC_GROUP, 14).
-define(API_API_VERSIONS, 18).

-record(state, {
    owner :: pid(),
    lsock :: gen_tcp:socket(),
    port :: inet:port_number(),
    conns = #{} :: #{reference() => gen_tcp:socket()}
}).

start_link(Owner) ->
    gen_server:start_link(?MODULE, Owner, []).

stop(Pid) ->
    gen_server:stop(Pid).

address(Pid) ->
    gen_server:call(Pid, address).

init(Owner) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary,
        {packet, 4},
        {active, false},
        {reuseaddr, true},
        {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(LSock),
    Server = self(),
    _ = spawn_link(fun() -> accept_loop(Server, LSock) end),
    {ok, #state{owner = Owner, lsock = LSock, port = Port}}.

handle_call(address, _From, State = #state{port = Port}) ->
    {reply, {"127.0.0.1", Port}, State};
handle_call({request, ApiKey, ApiVersion, CorrId, _ClientId, _Body}, _From, State) ->
    case response(ApiKey, ApiVersion, CorrId, State) of
        {reply, Reply} ->
            {reply, {reply, Reply}, State};
        close ->
            {reply, close, State};
        ignore ->
            {reply, ignore, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ignore, State}.

handle_cast({accepted, Sock}, State = #state{owner = Owner, conns = Conns}) ->
    ConnId = make_ref(),
    Server = self(),
    _ = spawn_link(fun() -> conn_loop(Server, Owner, ConnId, Sock) end),
    {noreply, State#state{conns = Conns#{ConnId => Sock}}};
handle_cast({closed, ConnId}, State = #state{conns = Conns}) ->
    {noreply, State#state{conns = maps:remove(ConnId, Conns)}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{lsock = LSock, conns = Conns}) ->
    _ = [gen_tcp:close(Sock) || Sock <- maps:values(Conns)],
    _ = gen_tcp:close(LSock),
    ok.

%%--------------------------------------------------------------------
%% Listener and connection handling
%%--------------------------------------------------------------------

accept_loop(Server, LSock) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            gen_server:cast(Server, {accepted, Sock}),
            accept_loop(Server, LSock);
        {error, _} ->
            ok
    end.

conn_loop(Server, Owner, ConnId, Sock) ->
    case gen_tcp:recv(Sock, 0, 30000) of
        {ok, Packet} ->
            case parse_header(Packet) of
                {ApiKey, ApiVersion, CorrId, ClientId, Body} ->
                    Owner ! {fake_kafka_request, ApiKey, ApiVersion, ClientId, ConnId},
                    handle_request(Server, Owner, ConnId, Sock, ApiKey, ApiVersion,
                                   CorrId, ClientId, Body);
                error ->
                    gen_tcp:close(Sock)
            end;
        {error, _} ->
            gen_tcp:close(Sock),
            gen_server:cast(Server, {closed, ConnId})
    end.

handle_request(Server, Owner, ConnId, Sock, ApiKey, ApiVersion, CorrId, ClientId, Body) ->
    case gen_server:call(
        Server,
        {request, ApiKey, ApiVersion, CorrId, ClientId, Body},
        10000
    ) of
        {reply, Reply} ->
            case gen_tcp:send(Sock, Reply) of
                ok ->
                    conn_loop(Server, Owner, ConnId, Sock);
                {error, _} ->
                    gen_tcp:close(Sock),
                    gen_server:cast(Server, {closed, ConnId})
            end;
        close ->
            gen_tcp:close(Sock),
            gen_server:cast(Server, {closed, ConnId});
        ignore ->
            conn_loop(Server, Owner, ConnId, Sock)
    end.

%%--------------------------------------------------------------------
%% Requests and responses
%%--------------------------------------------------------------------

response(?API_API_VERSIONS, _ApiVersion, CorrId, _State) ->
    {reply, api_versions_response(CorrId)};
response(?API_FIND_COORDINATOR, _ApiVersion, CorrId, #state{port = Port}) ->
    {reply, find_coordinator_response(CorrId, Port)};
response(?API_JOIN_GROUP, _ApiVersion, _CorrId, _State) ->
    %% Closing here makes the coordinator re-discover the coordinator and
    %% send another JoinGroup, which is what the tests observe.
    close;
response(?API_METADATA, _ApiVersion, CorrId, #state{port = Port}) ->
    {reply, metadata_response(CorrId, Port)};
response(_ApiKey, _ApiVersion, _CorrId, _State) ->
    ignore.

api_versions_response(CorrId) ->
    Entries = [
        {?API_METADATA, 0, 0},
        {?API_OFFSET_COMMIT, 2, 2},
        {?API_OFFSET_FETCH, 1, 1},
        {?API_FIND_COORDINATOR, 0, 0},
        {?API_JOIN_GROUP, 0, 0},
        {?API_HEARTBEAT, 0, 0},
        {?API_LEAVE_GROUP, 0, 0},
        {?API_SYNC_GROUP, 0, 0},
        {?API_API_VERSIONS, 0, 0}
    ],
    Encoded = iolist_to_binary([
        <<Key:16/signed, Min:16/signed, Max:16/signed>>
     || {Key, Min, Max} <- Entries
    ]),
    <<CorrId:32/signed, 0:16/signed, (length(Entries)):32/signed, Encoded/binary>>.

find_coordinator_response(CorrId, Port) ->
    Host = <<"127.0.0.1">>,
    <<CorrId:32/signed, 0:16/signed, 1:32/signed, (byte_size(Host)):16/signed,
        Host/binary, Port:32/signed>>.

metadata_response(CorrId, Port) ->
    Host = <<"127.0.0.1">>,
    %% One broker and no topics; the group coordinator does not need more.
    <<CorrId:32/signed, 1:32/signed, 1:32/signed, (byte_size(Host)):16/signed,
        Host/binary, Port:32/signed, 0:32/signed>>.

%%--------------------------------------------------------------------
%% Request header parsing
%%--------------------------------------------------------------------

parse_header(
    <<ApiKey:16/signed, ApiVersion:16/signed, CorrId:32/signed, ClientIdLen:16/signed,
        Rest/binary>>
) ->
    case ClientIdLen of
        -1 ->
            {ApiKey, ApiVersion, CorrId, undefined, Rest};
        Len when Len >= 0, byte_size(Rest) >= Len ->
            <<ClientId:Len/binary, Body/binary>> = Rest,
            {ApiKey, ApiVersion, CorrId, ClientId, Body};
        _ ->
            error
    end;
parse_header(_) ->
    error.
