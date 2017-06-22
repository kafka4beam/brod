%%%
%%%   Copyright (c) 2015-2017, Klarna AB
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
%%% ============================================================================

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
        , t_metadata_socket_restart/1
        , t_payload_socket_restart/1
        , t_auto_start_producers/1
        , t_auto_start_producer_for_unknown_topic/1
        , t_ssl/1
        , t_sasl_plain_ssl/1
        , t_sasl_plain_file_ssl/1
        , t_sasl_callback/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOST, "localhost").
-define(HOSTS, [{?HOST, 9092}]).
-define(HOSTS_SSL, [{?HOST, 9192}]).
-define(HOSTS_SASL_SSL, [{?HOST, 9292}]).
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

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

t_skip_unreachable_endpoint(Config) when is_list(Config) ->
  Client = t_skip_unreachable_endpoint,
  ok = brod:start_client([{"localhost", 8192} | ?HOSTS], Client),
  _Res = brod_client:get_partitions_count(Client, <<"some-unknown-topic">>),
  ?assertMatch({error, 'UnknownTopicOrPartition'}, _Res),
  ClientPid = whereis(Client),
  Ref = erlang:monitor(process, ClientPid),
  ok = brod:stop_client(Client),
  Reason = ?WAIT({'DOWN', Ref, process, ClientPid, Reason_}, Reason_, 5000),
  ?assertEqual(shutdown, Reason).

t_no_reachable_endpoint({'end', _Config}) ->
  brod:stop_client(t_no_reachable_endpoint);
t_no_reachable_endpoint(Config) when is_list(Config) ->
  Client = t_no_reachable_endpoint,
  ok = brod:start_client([{"badhost", 9092}], Client),
  ClientPid = whereis(Client),
  Mref = erlang:monitor(process, ClientPid),
  Reason = ?WAIT({'DOWN', Mref, process, ClientPid, Reason_}, Reason_, 1000),
  ?assertMatch({{{connection_failure, nxdomain}, _Hosts}, _Stacktrace}, Reason).

t_call_bad_client_id(Config) when is_list(Config) ->
  %% call a bad client ID
  Res = brod:produce(?undef, <<"topic">>, _Partition = 0, <<"k">>, <<"v">>),
  ?assertEqual({error, client_down}, Res).

t_metadata_socket_restart({init, Config}) ->
  meck:new(brod_sock, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_metadata_socket_restart({'end', Config}) ->
  brod:stop_client(t_metadata_socket_restart),
  meck:validate(brod_sock),
  meck:unload(brod_sock),
  Config;
t_metadata_socket_restart(Config) when is_list(Config) ->
  Ref = mock_brod_sock(),
  Client = t_metadata_socket_restart,
  ok = brod:start_client(?HOSTS, Client),
  ClientPid = whereis(Client),
  SocketPid = ?WAIT({socket_started, Ref, Pid}, Pid, 5000),
  ?assert(is_process_alive(ClientPid)),
  ?assert(is_process_alive(SocketPid)),
  %% kill the brod_sock pid
  MRef = erlang:monitor(process, SocketPid),
  exit(SocketPid, kill),
  ?WAIT({'DOWN', MRef, process, SocketPid, Reason_}, Reason_, 5000),
  %% query metadata to trigger reconnect
  {ok, _} = brod_client:get_metadata(Client, ?TOPIC),
  %% expect the socket pid get restarted
  SocketPid2 = ?WAIT({socket_started, Ref, Pid}, Pid, 5000),
  ?assert(is_process_alive(ClientPid)),
  ?assert(is_process_alive(SocketPid2)),
  ok.

t_payload_socket_restart({init, Config}) ->
  meck:new(brod_sock, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_payload_socket_restart({'end', Config}) ->
  brod:stop_client(t_payload_socket_restart),
  meck:validate(brod_sock),
  meck:unload(brod_sock),
  Config;
t_payload_socket_restart(Config) when is_list(Config) ->
  Ref = mock_brod_sock(),
  CooldownSecs = 2,
  ProducerRestartDelay = 1,
  ClientConfig = [{reconnect_cool_down_seconds, CooldownSecs}],
  Client = t_payload_socket_restart,
  ok = brod:start_client(?HOSTS, Client, ClientConfig),
  ?WAIT({socket_started, Ref, _MetadataSocket}, ok, 5000),
  ProducerConfig = [{partition_restart_delay_seconds, ProducerRestartDelay},
                    {max_retries, 0}],
  ok = brod:start_producer(Client, ?TOPIC, ProducerConfig),
  Partition = 0,
  ProduceFun =
    fun() -> brod:produce_sync(Client, ?TOPIC, Partition, <<"k">>, <<"v">>)
    end,
  %% producing data should trigger a payload connection to be established
  ok = ProduceFun(),
  %% the socket pid should have already delivered to self() mail box
  PayloadSock = ?WAIT({socket_started, Ref, Pid}, Pid, 0),

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
  ?WAIT({socket_started, Ref, Pid_}, Pid_, 4000),
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
  ok = brod:start_client(?HOSTS, Client),
  ?assertEqual({error, {producer_not_found, ?TOPIC}},
               brod:produce_sync(Client, ?TOPIC, 0, K, V)),
  ClientConfig = [{auto_start_producers, true}],
  ok = brod:stop_client(Client),
  ok = brod:start_client(?HOSTS, Client, ClientConfig),
  ?assertEqual(ok, brod:produce_sync(Client, ?TOPIC, 0, <<"k">>, <<"v">>)),
  ok.

t_auto_start_producer_for_unknown_topic({'end', Config}) ->
  brod:stop_client(t_auto_start_producer_for_unknown_topic),
  Config;
t_auto_start_producer_for_unknown_topic(Config) when is_list(Config) ->
  Client = t_auto_start_producer_for_unknown_topic,
  ClientConfig = [{auto_start_producers, true}],
  ok = brod:start_client(?HOSTS, Client, ClientConfig),
  Topic0 = ?TOPIC,
  Partition = 1000, %% non-existing partition
  ?assertEqual({error, {producer_not_found, Topic0, Partition}},
               brod:produce_sync(Client, Topic0, Partition, <<>>, <<"v">>)),
  Topic1 = <<"unknown-topic">>,
  ?assertEqual({error, 'UnknownTopicOrPartition'},
               brod:produce_sync(Client, Topic1, 0, <<>>, <<"v">>)),
  %% this error should hit the cache
  ?assertEqual({error, 'UnknownTopicOrPartition'},
               brod:produce_sync(Client, Topic1, 0, <<>>, <<"v">>)),
  ok.

t_ssl({init, Config}) ->
  Config;
t_ssl({'end', Config}) ->
  brod:stop_client(t_ssl),
  Config;
t_ssl(Config) when is_list(Config) ->
  ClientConfig = [ {ssl, ssl_options()}
                 , {get_metadata_timout_seconds, 10}],
  produce_and_consume_message(?HOSTS_SSL, t_ssl, ClientConfig).

t_sasl_plain_ssl({init, Config}) ->
  Config;
t_sasl_plain_ssl({'end', Config}) ->
  brod:stop_client(t_sasl_plain_ssl),
  Config;
t_sasl_plain_ssl(Config) when is_list(Config) ->
  ClientConfig = [ {ssl, ssl_options()}
                 , {get_metadata_timout_seconds, 10}
                 , {sasl, {plain, "alice", "alice-secret"}}
                 ],
  produce_and_consume_message(?HOSTS_SASL_SSL, t_sasl_plain_ssl, ClientConfig).

t_sasl_plain_file_ssl({init, Config}) ->
  ok = file:write_file("sasl-plain-user-pass-file", "alice\nalice-secret\n"),
  Config;
t_sasl_plain_file_ssl({'end', Config}) ->
  brod:stop_client(t_sasl_plain_file_ssl),
  Config;
t_sasl_plain_file_ssl(Config) when is_list(Config) ->
  ClientConfig = [ {ssl, ssl_options()}
                 , {get_metadata_timout_seconds, 10}
                 , {sasl, {plain, "sasl-plain-user-pass-file"}}
                 ],
  produce_and_consume_message(?HOSTS_SASL_SSL, t_sasl_plain_ssl, ClientConfig).

t_sasl_callback({init, Config}) ->
  Config;
t_sasl_callback({'end', Config}) ->
  brod:stop_client(t_sasl_callback),
  Config;
t_sasl_callback(Config) when is_list(Config) ->
  ClientConfig = [ {get_metadata_timout_seconds, 10}
                 , {sasl, {callback, ?MODULE, []}}
                 ],
  produce_and_consume_message(?HOSTS, t_sasl_callback, ClientConfig).

%%%_* Help functions ===========================================================

%% mocked callback
auth(_Host, _Sock, _Mod, _ClientId, _Timeout, _Opts) -> ok.

ssl_options() ->
  PrivDir = code:priv_dir(brod),
  Fname = fun(Name) -> filename:join([PrivDir, ssl, Name]) end,
  [ {cacertfile, Fname("ca.crt")}
  , {keyfile,    Fname("client.key")}
  , {certfile,   Fname("client.crt")}
  ].

produce_and_consume_message(Host, Client, ClientConfig) ->
  K = term_to_binary(make_ref()),
  ok = brod:start_client(Host, Client, ClientConfig),
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
    ok              -> ok;
    {error, Reason} -> ?assertMatch({producer_down, _}, Reason)
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

%% tap the call to brod_sock:start_link/5,
%% intercept the returned socket pid
%% and send it to the test process: self()
mock_brod_sock() ->
  Ref = make_ref(),
  Tester = self(),
  SocketStartLinkFun =
    fun(Parent, Host, Port, ClientId, Dbg) ->
      {ok, Pid} = meck:passthrough([Parent, Host, Port, ClientId, Dbg]),
      %% assert the caller
      ?assertEqual(Parent, whereis(ClientId)),
      ct:pal("client ~p: socket to ~s:~p intercepted. pid=~p",
             [ClientId, Host, Port, Pid]),
      Tester ! {socket_started, Ref, Pid},
      {ok, Pid}
    end,
  ok = meck:expect(brod_sock, start_link, SocketStartLinkFun),
  Ref.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
