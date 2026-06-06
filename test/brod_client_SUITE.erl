%%%
%%%   Copyright (c) 2015-2021, Klarna Bank AB (publ)
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

%% @private
-module(brod_client_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

-export([ auth/6 ]).

%% Test cases
-export([ t_skip_unreachable_endpoint/1
        , t_no_reachable_endpoint/1
        , t_call_bad_client_id/1
        , t_metadata_connection_restart/1
        , t_payload_connection_restart/1
        , t_auto_start_producers/1
        , t_auto_start_producer_for_unknown_topic/1
        , t_restart_previous_producers/1
        , t_restart_previous_consumers/1
        , t_ssl/1
        , t_sasl_plain_ssl/1
        , t_sasl_plain_file_ssl/1
        , t_sasl_callback/1
        , t_magic_version/1
        , t_get_partitions_count_configure_cache_ttl/1
        , t_get_partitions_count_safe/1
        , t_double_stop_consumer/1
        , t_start_producer_does_not_post_init_callback/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOST, "localhost").
-define(HOSTS, [{?HOST, 9192}]).
-define(HOSTS_SSL, [{?HOST, 9193}]).
-define(HOSTS_SASL_SSL, [{?HOST, 9194}]).
-define(TOPIC, <<"brod-client-SUITE-topic">>).

-define(WAIT(PATTERN, RESULT, TIMEOUT),
        fun() ->
          receive
            PATTERN ->
              RESULT
          after TIMEOUT ->
            ct:pal("timeout ~p ~p ~p", [?MODULE, ?LINE, TIMEOUT]),
            ct:fail(timeout)
          end
        end()).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) ->
  application:stop(brod),
  ok.

init_per_testcase(Case, Config) ->
  ct:pal("=== ~p begin ===", [Case]),
  try
    ?MODULE:Case({init, Config})
  catch
    error : function_clause ->
      Config
  end.

end_per_testcase(Case, Config) ->
  try
    ?MODULE:Case({'end', Config})
  catch
    error : function_clause ->
      ok
  end,
  ct:pal("=== ~p end ===", [Case]),
  ok.

all() ->
  KafkaVsn = kafka_version(),
  MinVsn = fun(F) -> try ?MODULE:F(min_kafka_vsn)
                     catch error : function_clause -> ?KAFKA_0_9
                     end end,
  [F || {F, _A} <- module_info(exports),
        case atom_to_list(F) of
          "t_" ++ _ -> true;
          _         -> false
        end andalso KafkaVsn >= MinVsn(F)
  ].


%%%_* Test functions ===========================================================

t_get_partitions_count_safe(Config) when is_list(Config) ->
  Client = ?FUNCTION_NAME,
  ok = start_client(?HOSTS, Client),
  Topic = <<"unknown-topic-001">>,
  Res = brod:get_partitions_count_safe(Client, Topic),
  ?assertMatch({error, unknown_topic_or_partition}, Res),
  %% expect the 'unknown' result to be cached
  Res2 = brod_client:lookup_partitions_count_cache(Client, Topic),
  ?assertMatch({error, unknown_topic_or_partition}, Res2),
  ok = brod:stop_client(Client).


t_get_partitions_count_configure_cache_ttl(Config) when is_list(Config) ->
  Client = ?FUNCTION_NAME,
  ClientConfig = [{unknown_topic_cache_ttl, 100}],
  ok = start_client(?HOSTS, Client, ClientConfig),
  Topic = <<"unknown-topic-001">>,
  Res = brod:get_partitions_count_safe(Client, Topic),
  ?assertMatch({error, unknown_topic_or_partition}, Res),
  Res2 = brod_client:lookup_partitions_count_cache(Client, Topic),
  ?assertMatch({error, unknown_topic_or_partition}, Res2),
  timer:sleep(101),
  Res3 = brod_client:lookup_partitions_count_cache(Client, Topic),
  ?assertMatch(false, Res3),
  ok = brod:stop_client(Client).


t_skip_unreachable_endpoint(Config) when is_list(Config) ->
  Client = t_skip_unreachable_endpoint,
  ok = start_client([{"localhost", 8192} | ?HOSTS], Client),
  _Res = brod_client:get_partitions_count(Client, <<"some-unknown-topic">>),
  ?assertMatch({error, unknown_topic_or_partition}, _Res),
  ClientPid = whereis(Client),
  Ref = erlang:monitor(process, ClientPid),
  ok = brod:stop_client(Client),
  Reason = ?WAIT({'DOWN', Ref, process, ClientPid, Reason_}, Reason_, 5000),
  ?assertEqual(shutdown, Reason).

t_no_reachable_endpoint({'end', _Config}) ->
  brod:stop_client(t_no_reachable_endpoint);
t_no_reachable_endpoint(Config) when is_list(Config) ->
  Client = t_no_reachable_endpoint,
  Endpoint = {"badhost", 9092},
  ok = start_client([Endpoint], Client),
  ClientPid = whereis(Client),
  Mref = erlang:monitor(process, ClientPid),
  Reason = ?WAIT({'DOWN', Mref, process, ClientPid, ReasonX}, ReasonX, 1000),
  ?assertMatch([{Endpoint, {nxdomain, _Stack}}], Reason).

t_call_bad_client_id(Config) when is_list(Config) ->
  %% call a bad client ID
  Res = brod:produce(?undef, <<"topic">>, _Partition = 0, <<"k">>, <<"v">>),
  ?assertEqual({error, client_down}, Res).

t_metadata_connection_restart({init, Config}) ->
  meck:new(kpro_connection, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_metadata_connection_restart({'end', Config}) ->
  brod:stop_client(t_metadata_connection_restart),
  meck:validate(kpro_connection),
  meck:unload(kpro_connection),
  Config;
t_metadata_connection_restart(Config) when is_list(Config) ->
  Ref = mock_connection(hd(?HOSTS)),
  Client = t_metadata_connection_restart,
  ok = start_client(?HOSTS, Client),
  ClientPid = whereis(Client),
  Connection = ?WAIT({connection_pid, Ref, Pid}, Pid, 5000),
  ?assert(is_process_alive(ClientPid)),
  ?assert(is_process_alive(Connection)),
  %% kill the connection pid
  MRef = erlang:monitor(process, Connection),
  exit(Connection, kill),
  ?WAIT({'DOWN', MRef, process, Connection, Reason_}, Reason_, 5000),
  %% trigger a metadata query
  brod_client:get_metadata(Client, all),
  %% expect the connection pid get restarted
  Connection2 = ?WAIT({connection_pid, Ref, Pid}, Pid, 5000),
  ?assert(is_process_alive(ClientPid)),
  ?assert(is_process_alive(Connection2)),
  ok.

t_payload_connection_restart({init, Config}) ->
  meck:new(kpro_connection, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_payload_connection_restart({'end', Config}) ->
  brod:stop_client(t_payload_connection_restart),
  meck:validate(kpro_connection),
  meck:unload(kpro_connection),
  Config;
t_payload_connection_restart(Config) when is_list(Config) ->
  Ref = mock_connection(hd(?HOSTS)),
  CooldownSecs = 2,
  ProducerRestartDelay = 1,
  ClientConfig = [{reconnect_cool_down_seconds, CooldownSecs}],
  Client = t_payload_connection_restart,
  ok = start_client(?HOSTS, Client, ClientConfig),
  ?WAIT({connection_pid, Ref, _MetadataConnection}, ok, 5000),
  ProducerConfig = [{partition_restart_delay_seconds, ProducerRestartDelay},
                    {max_retries, 0}],
  ok = brod:start_producer(Client, ?TOPIC, ProducerConfig),
  Partition = 0,
  ProduceFun =
    fun() -> brod:produce_sync(Client, ?TOPIC, Partition, <<"k">>, <<"v">>)
    end,
  %% producing data should trigger a payload connection to be established
  ok = ProduceFun(),
  %% the connection pid should have already delivered to self() mail box
  PayloadSock = ?WAIT({connection_pid, Ref, Pid}, Pid, 0),

  %% spawn a writer which keeps retrying to produce data to partition 0
  %% and report the produce_sync return value changes
  Parent = self(),
  WriterPid = erlang:spawn_link(
                fun() ->
                  retry_writer_loop(Parent, ProduceFun, undefined)
                end),
  %% wait until the writer succeeded producing data
  ?WAIT({WriterPid, {produce_result, ok}}, ok, 1000),
  %% kill the payload socket
  exit(PayloadSock, kill),
  %% now the writer should have {error, _} returned from produce API
  ?WAIT({WriterPid, {produce_result, {error, _}}}, ok, 1000),
  ?WAIT({connection_pid, Ref, Pid_}, Pid_, 4000),
  %% then wait for the producer to get restarted by supervisor
  %% and the writer process should continue working normally again.
  %% socket should be restarted after cooldown timeout
  %% and the restart is triggered by producer restart
  %% add 2 seconds more in wait timeout to avoid race
  Timeout = timer:seconds(CooldownSecs + ProducerRestartDelay + 2),
  ?WAIT({WriterPid, {produce_result, ok}}, ok, Timeout),
  %% stop the temp writer
  Mref = erlang:monitor(process, WriterPid),
  WriterPid ! stop,
  ?WAIT({'DOWN', Mref, process, WriterPid, normal}, ok, 5000),
  ok.

t_auto_start_producers({init, Config}) ->
  Config;
t_auto_start_producers({'end', Config}) ->
  brod:stop_client(t_auto_start_producers),
  Config;
t_auto_start_producers(Config) when is_list(Config) ->
  K = <<"k">>,
  V = <<"v">>,
  Client = t_auto_start_producers,
  ok = start_client(?HOSTS, Client),
  ?assertEqual({error, {producer_not_found, ?TOPIC}},
               brod:produce_sync(Client, ?TOPIC, 0, K, V)),
  ClientConfig = [{auto_start_producers, true}],
  ok = brod:stop_client(Client),
  ok = start_client(?HOSTS, Client, ClientConfig),
  ?assertEqual(ok, brod:produce_sync(Client, ?TOPIC, 0, <<"k">>, <<"v">>)),
  ok.

t_restart_previous_producers({init, Config}) ->
  Config;
t_restart_previous_producers({'end', Config}) ->
  brod:stop_client(t_restart_previous_producers),
  Config;
t_restart_previous_producers(Config) when is_list(Config) ->
  Client = t_restart_previous_producers,
  Topic = ?TOPIC,
  ok = start_client(?HOSTS, Client),
  ok = brod:start_producer(Client, Topic, []),
  ok = brod_client:stop_producer(Client, Topic),
  ?assertEqual(ok, brod:start_producer(Client, Topic, [])),
  {ok, Pid} = brod_client:get_producer(Client, Topic, 0),
  ?assertEqual(true, erlang:is_process_alive(Pid)),
  ok.

t_restart_previous_consumers({init, Config}) ->
  Config;
t_restart_previous_consumers({'end', Config}) ->
  brod:stop_client(t_restart_previous_consumers),
  Config;
t_restart_previous_consumers(Config) when is_list(Config) ->
  Client = t_restart_previous_consumers,
  Topic =?TOPIC,
  ok = start_client(?HOSTS, Client),
  ok = brod:start_consumer(Client, Topic, []),
  ok = brod_client:stop_consumer(Client, Topic),
  ?assertEqual(ok, brod:start_consumer(Client, Topic, [])),
  {ok, Pid} = brod_client:get_consumer(Client, Topic, 0),
  ?assertEqual(true, erlang:is_process_alive(Pid)),
  ok.

t_auto_start_producer_for_unknown_topic({'end', Config}) ->
  brod:stop_client(t_auto_start_producer_for_unknown_topic),
  Config;
t_auto_start_producer_for_unknown_topic(Config) when is_list(Config) ->
  Client = t_auto_start_producer_for_unknown_topic,
  ClientConfig = [{auto_start_producers, true}],
  ok = start_client(?HOSTS, Client, ClientConfig),
  Topic0 = ?TOPIC,
  Partition = 1000, %% non-existing partition
  ?assertEqual({error, {producer_not_found, Topic0, Partition}},
               brod:produce_sync(Client, Topic0, Partition, <<>>, <<"v">>)),
  Topic1 = <<"unknown-topic">>,
  ?assertEqual({error, 'unknown_topic_or_partition'},
               brod:produce_sync(Client, Topic1, 0, <<>>, <<"v">>)),
  %% this error should hit the cache
  ?assertEqual({error, 'unknown_topic_or_partition'},
               brod:produce_sync(Client, Topic1, 0, <<>>, <<"v">>)),
  ok.

t_ssl({init, Config}) ->
  Config;
t_ssl({'end', Config}) ->
  brod:stop_client(t_ssl),
  Config;
t_ssl(Config) when is_list(Config) ->
  ClientConfig = [ {ssl, ssl_options()}
                 , {get_metadata_timeout_seconds, 10}],
  produce_and_consume_message(?HOSTS_SSL, t_ssl, ClientConfig).

t_sasl_plain_ssl(min_kafka_vsn) -> ?KAFKA_0_10;
t_sasl_plain_ssl({init, Config}) ->
  Config;
t_sasl_plain_ssl({'end', Config}) ->
  brod:stop_client(t_sasl_plain_ssl),
  Config;
t_sasl_plain_ssl(Config) when is_list(Config) ->
  ClientConfig = [ {ssl, ssl_options()}
                 , {get_metadata_timeout_seconds, 10}
                 , {sasl, {plain, "alice", "ecila"}}
                 ],
  produce_and_consume_message(?HOSTS_SASL_SSL, t_sasl_plain_ssl, ClientConfig).

t_sasl_plain_file_ssl(min_kafka_vsn) -> ?KAFKA_0_10;
t_sasl_plain_file_ssl({init, Config}) ->
  ok = file:write_file("sasl-plain-user-pass-file", "alice\necila\n"),
  Config;
t_sasl_plain_file_ssl({'end', Config}) ->
  brod:stop_client(t_sasl_plain_file_ssl),
  Config;
t_sasl_plain_file_ssl(Config) when is_list(Config) ->
  ClientConfig = [ {ssl, ssl_options()}
                 , {get_metadata_timeout_seconds, 10}
                 , {sasl, {plain, "sasl-plain-user-pass-file"}}
                 ],
  produce_and_consume_message(?HOSTS_SASL_SSL, t_sasl_plain_ssl, ClientConfig).

t_sasl_callback(min_kafka_vsn) -> ?KAFKA_0_10;
t_sasl_callback({init, Config}) -> Config;
t_sasl_callback({'end', Config}) ->
  brod:stop_client(t_sasl_callback),
  Config;
t_sasl_callback(Config) when is_list(Config) ->
  ClientConfig = [ {get_metadata_timeout_seconds, 10}
                 , {sasl, {callback, ?MODULE, []}}
                 ],
  produce_and_consume_message(?HOSTS, t_sasl_callback, ClientConfig).

t_magic_version({init, Config}) -> Config;
t_magic_version({'end', Config}) ->
  brod:stop_client(t_magic_version),
  Config;
t_magic_version(Config) when is_list(Config) ->
  Client = t_magic_version,
  ClientConfig = [{get_metadata_timeout_seconds, 10}],
  K = term_to_binary(make_ref()),
  ok = start_client(?HOSTS, Client, ClientConfig),
  ok = brod:start_producer(Client, ?TOPIC, []),
  {ok, Conn} = brod_client:get_leader_connection(Client, ?TOPIC, 0),
  Headers = [{<<"foo">>, <<"bar">>}],
  Msg = #{value => <<"v">>, headers => Headers},
  {ok, Offset} = brod:produce_sync_offset(Client, ?TOPIC, 0, K, Msg),
  {ok, {_, [M]}} = brod:fetch(Conn, ?TOPIC, 0, Offset, #{max_wait_time => 100}),
  #kafka_message{key = K, headers = Hdrs, ts = Ts} = M,
  case kafka_version() of
    ?KAFKA_0_9 ->
      ?assertEqual([], Hdrs),
      ?assertEqual(?undef, Ts);
    ?KAFKA_0_10 ->
      ?assertEqual([], Hdrs),
      ?assert(is_integer(Ts));
    _ ->
      ?assertEqual(Headers, Hdrs),
      ?assert(is_integer(Ts))
  end.

t_double_stop_consumer({init, Config}) -> Config;
t_double_stop_consumer({'end', Config}) ->
  brod:stop_client(?FUNCTION_NAME),
  Config;
t_double_stop_consumer(Config) when is_list(Config) ->
  Client = ?FUNCTION_NAME,
  ClientConfig = [{get_metadata_timeout_seconds, 10}],
  ok = start_client(?HOSTS, Client, ClientConfig),
  ok = brod:start_consumer(Client, ?TOPIC, []),
  ?assertMatch({ok, _}, brod_client:get_consumer(Client, ?TOPIC, 0)),
  ?assertMatch(ok, brod_client:stop_consumer(Client, ?TOPIC)),
  ?assertMatch({error, {consumer_not_found, _}}, brod_client:get_consumer(Client, ?TOPIC, 0)),
  ?assertMatch({error, not_found}, brod_client:stop_consumer(Client, ?TOPIC)),
  ok.

%%%_* Help functions ===========================================================

%% mocked callback
auth(_Host, _Sock, _Mod, _ClientId, _Timeout, _Opts) -> ok.

ssl_options() ->
  LibDir = code:lib_dir(brod),
  Fname = fun(Name) -> filename:join([LibDir, test, data, ssl, Name]) end,
  [ {cacertfile, Fname("ca.pem")}
  , {keyfile,    Fname("client-key.pem")}
  , {certfile,   Fname("client-crt.pem")}
  , {versions,   ['tlsv1.2']}
  , {verify,     verify_none}
  ].

produce_and_consume_message(Host, Client, ClientConfig) ->
  K = term_to_binary(make_ref()),
  ok = start_client(Host, Client, ClientConfig),
  ok = brod:start_consumer(Client, ?TOPIC, []),
  {ok, ConsumerPid} =
    brod:subscribe(Client, self(), ?TOPIC, 0, [{begin_offset, latest}]),
  ok = brod:start_producer(Client, ?TOPIC, []),
  ?assertEqual(ok, brod:produce_sync(Client, ?TOPIC, 0, K, <<"v">>)),
  ?WAIT({ConsumerPid,
         #kafka_message_set{ topic = ?TOPIC
                           , partition = 0
                           , messages = [#kafka_message{key = K}]
                           }}, ok, 5000),
  ok.

retry_writer_loop(Parent, ProduceFun, LastResult) ->
  Result = ProduceFun(),
  %% assert result patterns
  case Result of
    ok -> ok;
    {error, client_down} -> ok;
    {error, {producer_down, _}} -> ok;
    {error, Reason} -> error({unexpected_error, Reason})
  end,
  %% tell parent about produce return value changes
  case Result =/= LastResult of
    true  -> Parent ! {self(), {produce_result, Result}};
    false -> ok
  end,
  %% continue if not told to stop
  receive
    stop ->
      exit(normal)
  after 100 ->
    retry_writer_loop(Parent, ProduceFun, Result)
  end.

%% tap the call to kpro_connection:start/3
%% intercept the returned socket pid
%% and send it to the test process: self()
mock_connection(EP) ->
  Ref = make_ref(),
  Tester = self(),
  StartFun =
    fun(Host, Port, Config) ->
      {ok, Pid} = meck:passthrough([Host, Port, Config]),
      Tester ! {connection_pid, Ref, Pid},
      {ok, Pid}
    end,
  ok = meck:expect(kpro_connection, start, StartFun),
  ok = meck:expect(kpro_connection, get_endpoint, 1, {ok, EP}),
  Ref.

kafka_version() -> kafka_test_helper:kafka_version().

%% Regression for issue #662: brod_client and brod_producers_sup could
%% deadlock when brod_client's `do_get_metadata' walked the producer
%% supervisor tree via `count_started_children/1' while a per-topic
%% supervisor was sitting in its `post_init/1' callback waiting for
%% brod_client to answer `get_partitions_count/2'.
%%
%% The fix is for brod_client to pre-supply the partition count to
%% `brod_producers_sup:start_topic_producer/5' so the new per-topic
%% supervisor's `post_init/1' never calls back into brod_client. This
%% test asserts that property white-box: after `brod:start_producer/3'
%% (which goes through the same `do_start_producer/3' path as the
%% `auto_start_producers' flow that exposed the deadlock), no
%% `brod_client:get_partitions_count/2' call should originate from the
%% per-topic supervisor's `post_init/1' callback. Before the fix this
%% count would be at least 1 (one call per per-topic supervisor created).
t_start_producer_does_not_post_init_callback({init, Config}) ->
  meck:new(brod_client, [passthrough]),
  Config;
t_start_producer_does_not_post_init_callback({'end', Config}) ->
  meck:unload(brod_client),
  brod:stop_client(t_start_producer_does_not_post_init_callback),
  Config;
t_start_producer_does_not_post_init_callback(Config) when is_list(Config) ->
  Client = t_start_producer_does_not_post_init_callback,
  Topic = ?TOPIC,
  ok = start_client(?HOSTS, Client),
  %% Warm the partition count cache via a direct call so the counter below
  %% only attributes new calls to the per-topic supervisor's `post_init/1'.
  {ok, _} = brod_client:get_partitions_count(Client, Topic),
  %% From here on, every `brod_client:get_partitions_count/2' call must come
  %% from `brod_producers_sup:post_init/1' — there is no other caller in the
  %% `start_producer' code path.
  Counter = counters:new(1, []),
  meck:expect(brod_client, get_partitions_count,
    fun(C, T) ->
        counters:add(Counter, 1, 1),
        meck:passthrough([C, T])
    end),
  ok = brod:start_producer(Client, Topic, []),
  %% Allow the per-topic supervisor's `handle_continue' / `post_init/1' to run.
  ok = timer:sleep(200),
  ?assertEqual(
    0, counters:get(Counter, 1),
    "brod_producers_sup:post_init/1 must not call back into brod_client "
    "when brod_client has pre-supplied the partition count"),
  ok.

start_client(Hosts, ClientId) -> start_client(Hosts, ClientId, []).

start_client(Hosts, ClientId, Config0) ->
  Config =
    case kafka_version() of
      ?KAFKA_0_9 -> [{query_api_versions, false} | Config0];
      _ -> Config0
    end,
  brod:start_client(Hosts, ClientId, Config).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
