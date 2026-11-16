%% Tests for the client ID sent on the group coordinator connection.
-module(brod_group_client_id_SUITE).

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    t_default_group_client_id/1,
    t_configured_group_client_id/1,
    t_configured_group_client_id_after_reconnect/1,
    t_max_group_client_id/1,
    t_invalid_group_client_id/1
]).

-export([init/2, handle_message/2, terminate/2, assignments_revoked/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLIENT_ID, brod_group_client_id_SUITE).
-define(GROUP_ID, <<"brod-group-client-id">>).
-define(TOPIC, <<"brod-group-client-id-topic">>).
-define(API_JOIN_GROUP, 11).
-define(GROUP_CONFIG, [{max_rejoin_attempts, 100}, {rejoin_delay_seconds, 0}]).

all() ->
    [
        t_default_group_client_id,
        t_configured_group_client_id,
        t_configured_group_client_id_after_reconnect,
        t_max_group_client_id,
        t_invalid_group_client_id
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(brod),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(brod),
    ok.

init_per_testcase(_TestCase, Config) ->
    process_flag(trap_exit, true),
    {ok, Broker} = brod_fake_kafka:start_link(self()),
    {Host, Port} = brod_fake_kafka:address(Broker),
    ok = brod:start_client([{Host, Port}], ?CLIENT_ID, []),
    [{broker, Broker} | Config].

end_per_testcase(_TestCase, Config) ->
    Broker = ?config(broker, Config),
    _ = catch brod_fake_kafka:stop(Broker),
    _ = catch brod:stop_client(?CLIENT_ID),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_default_group_client_id(_Config) ->
    with_subscriber([], fun(_Subscriber) ->
        [Header | _] = wait_join_group_headers(1),
        NodePrefix = iolist_to_binary(io_lib:format("~p/", [node()])),
        ?assert(byte_size(Header) > byte_size(NodePrefix)),
        ?assertEqual(NodePrefix, binary:part(Header, 0, byte_size(NodePrefix))),
        Suffix = binary:part(
            Header, byte_size(NodePrefix), byte_size(Header) - byte_size(NodePrefix)
        ),
        ?assertMatch({match, _}, re:run(Suffix, <<"^<\\d+\\.\\d+\\.\\d+>$">>))
    end).

t_configured_group_client_id(_Config) ->
    GroupClientId = <<"configured-group-client-id">>,
    with_subscriber([{group_client_id, GroupClientId}], fun(_Subscriber) ->
        [Header | _] = wait_join_group_headers(1),
        ?assertEqual(GroupClientId, Header)
    end).

t_configured_group_client_id_after_reconnect(_Config) ->
    GroupClientId = <<"configured-group-client-id">>,
    with_subscriber([{group_client_id, GroupClientId}], fun(_Subscriber) ->
        [{First, ConnId1}, {Second, ConnId2}] = wait_join_group_requests(2),
        ?assertEqual(GroupClientId, First),
        ?assertEqual(GroupClientId, Second),
        ?assertNotEqual(ConnId1, ConnId2)
    end).

t_max_group_client_id(_Config) ->
    %% Kafka appends "-" and a 36-byte UUID to the client ID when it derives a
    %% dynamic member ID, so 32730 bytes is the longest usable value.
    GroupClientId = binary:copy(<<"a">>, 32730),
    with_subscriber([{group_client_id, GroupClientId}], fun(_Subscriber) ->
        [Header | _] = wait_join_group_headers(1),
        ?assertEqual(GroupClientId, Header)
    end).

t_invalid_group_client_id(_Config) ->
    ?assertMatch(
        {error, {{invalid_group_client_id, 123}, _}},
        brod_group_coordinator:start_link(
            ?CLIENT_ID, ?GROUP_ID, [?TOPIC], [{group_client_id, 123}], ?MODULE, self()
        )
    ),
    TooLong = binary:copy(<<"a">>, 32731),
    ?assertMatch(
        {error, {{invalid_group_client_id, TooLong}, _}},
        brod_group_coordinator:start_link(
            ?CLIENT_ID, ?GROUP_ID, [?TOPIC], [{group_client_id, TooLong}], ?MODULE, self()
        )
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

with_subscriber(GroupConfig, Fun) ->
    {ok, Subscriber} = start_subscriber(GroupConfig),
    try
        Fun(Subscriber)
    after
        stop_subscriber(Subscriber)
    end.

start_subscriber(GroupConfig) ->
    brod:start_link_group_subscriber_v2(#{
        client => ?CLIENT_ID,
        group_id => ?GROUP_ID,
        topics => [?TOPIC],
        cb_module => ?MODULE,
        init_data => #{},
        message_type => message,
        consumer_config => [],
        group_config => ?GROUP_CONFIG ++ GroupConfig
    }).

stop_subscriber(Subscriber) ->
    _ = catch brod_group_subscriber_v2:stop(Subscriber),
    receive
        {'EXIT', Subscriber, _} -> ok
    after 0 ->
        ok
    end.

wait_join_group_headers(Count) ->
    [ClientId || {ClientId, _ConnId} <- wait_join_group_requests(Count)].

wait_join_group_requests(Count) ->
    wait_join_group_requests(Count, []).

wait_join_group_requests(0, Acc) ->
    lists:reverse(Acc);
wait_join_group_requests(Count, Acc) ->
    receive
        {fake_kafka_request, ?API_JOIN_GROUP, _ApiVersion, ClientId, ConnId} ->
            wait_join_group_requests(Count - 1, [{ClientId, ConnId} | Acc])
    after 15000 ->
        ct:fail({join_group_timeout, lists:reverse(Acc)})
    end.

%%--------------------------------------------------------------------
%% brod_group_subscriber_v2 callbacks
%%--------------------------------------------------------------------

init(_GroupInfo, State) ->
    {ok, State}.

handle_message(_Message, State) ->
    {ok, commit, State}.

terminate(_Reason, _State) ->
    ok.

assignments_revoked({Pid, Count}) ->
    Pid ! {assignments_revoked, Count},
    ok;
assignments_revoked(_) ->
    ok.
