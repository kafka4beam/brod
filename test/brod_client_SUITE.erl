%%%
%%%   Copyright (c) 2015, Klarna AB
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
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

-define(HOST, "localhost").
-define(PORT, 9092).
-define(HOSTS, [{?HOST, ?PORT}]).
-define(TOPIC, <<"brod-client-SUITE-topic">>).

-define(WAIT(PATTERN, RESULT, TIMEOUT),
        fun() ->
          receive
            PATTERN ->
              RESULT
          after TIMEOUT ->
            ct:pal("~p ~p ~p", [?MODULE, ?LINE, TIMEOUT]),
            ct:fail(timeout)
          end
        end()).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

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
  {ok, Pid} = brod:start_link_client(Client, [{"localhost", 8092} | ?HOSTS],
                                     _Producers = [], _Consumers = [],
                                     _Config = []),
  ?assert(is_pid(Pid)),
  _Res = brod_client:get_partitions(Pid, <<"some-unknown-topic">>),
  % auto.create.topics.enabled is 'true' in default spotify/kafka container
  % ?assertEqual({error, 'UnknownTopicOrPartitionException'}, _Res),
  Ref = erlang:monitor(process, Pid),
  ok = brod:stop_client(Pid),
  Reason = ?WAIT({'DOWN', Ref, process, Pid, Reason_}, Reason_, 5000),
  ?assertEqual(normal, Reason).

t_no_reachable_endpoint(Config) when is_list(Config) ->
  process_flag(trap_exit, true),
  {ok, Pid} =brod:start_link_client([{"badhost", 9092}],
                                    _Producers = [], _Consumers = []),
  Reason = ?WAIT({'EXIT', Pid, Reason_}, Reason_, 1000),
  ?assertMatch({nxdomain, _Stacktrace}, Reason).

t_not_a_brod_client(Config) when is_list(Config) ->
  %% call a bad client ID
  Res = brod:produce(?undef, <<"topic">>, _Partition = 0, <<"k">>, <<"v">>),
  ?assertEqual({error, client_down}, Res).

t_metadata_socket_restart({init, Config}) ->
  meck:new(brod_sock, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_metadata_socket_restart({'end', Config}) ->
  case whereis(t_metadata_socket_restart) of
    ?undef -> ok;
    Pid    -> brod:stop_client(Pid)
  end,
  meck:validate(brod_sock),
  meck:unload(brod_sock),
  Config;
t_metadata_socket_restart(Config) when is_list(Config) ->
  Ref = mock_brod_sock(),
  {ok, ClientPid} =
    brod:start_link_client(t_metadata_socket_restart, ?HOSTS,
                           _Producers = [], _Consumers = [], _Config = []),
  SocketPid = ?WAIT({socket_started, Ref, Pid}, Pid, 5000),
  ?assert(is_process_alive(ClientPid)),
  ?assert(is_process_alive(SocketPid)),
  %% kill the brod_sock pid
  exit(SocketPid, kill),
  %% expect the socket pid get restarted rightaway
  SocketPid2 = ?WAIT({socket_started, Ref, Pid}, Pid, 5000),
  ?assert(is_process_alive(ClientPid)),
  ?assert(is_process_alive(SocketPid2)),
  brod_client:get_metadata(ClientPid, ?TOPIC),
  ok.

t_payload_socket_restart({init, Config}) ->
  meck:new(brod_sock, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_payload_socket_restart({'end', Config}) ->
  case whereis(t_payload_socket_restart) of
    ?undef -> ok;
    Pid    -> brod:stop_client(Pid)
  end,
  meck:validate(brod_sock),
  meck:unload(brod_sock),
  Config;
t_payload_socket_restart(Config) when is_list(Config) ->
  Ref = mock_brod_sock(),
  CooldownSecs = 2,
  ProducerRestartDelay = 1,
  ClientConfig = [{reconnect_cool_down_seconds, CooldownSecs}],
  Producer = {?TOPIC, [{partition_restart_delay_seconds, ProducerRestartDelay}]},
  Partition = 0,
  {ok, Client} =
    brod:start_link_client(t_payload_socket_restart, ?HOSTS,
                           [Producer], [], ClientConfig),
  ?WAIT({socket_started, Ref, _MetadataSocket}, ok, 5000),
  ProduceFun =
    fun() -> brod:produce_sync(Client, ?TOPIC, Partition, <<"k">>, <<"v">>)
    end,
  %% producing data should trigger a payload connection to be established
  ok = ProduceFun(),
  %% the socket pid should have already delivered to self() mail box
  PayloadSock = ?WAIT({socket_started, Ref, Pid}, Pid, 0),
  %% spawn a data writer to keep retrying in case of error
  WriterLoopFun =
    fun(LoopFun) ->
      case ProduceFun() of
        ok              -> ok;
        {error, Reason} -> ?assertMatch({producer_down, _}, Reason)
      end,
      receive stop -> exit(normal)
      after 500    -> LoopFun(LoopFun)
      end
    end,
  Parent = self(),
  WriterPid = erlang:spawn_link(
                fun() ->
                  Parent ! {self(), <<"i'm ready">>},
                  WriterLoopFun(WriterLoopFun)
                end),
  ?WAIT({WriterPid, <<"i'm ready">>}, ok, 1000),
  %% kill the payload socket
  exit(PayloadSock, kill),
  %% socket should be restarted after cooldown timeout
  %% and the restart is triggered by producer restart
  %% add 2 seconds more in wait timeout to avoid race
  Timeout = timer:seconds(CooldownSecs + ProducerRestartDelay + 3),
  SockPid = ?WAIT({socket_started, Ref, Pid_}, Pid_, Timeout),
  Mref = erlang:monitor(process, WriterPid),
  WriterPid ! stop,
  ?WAIT({'DOWN', Mref, process, WriterPid, normal}, ok, 5000),
  {ok, SockPid_} = brod_client:get_connection(Client, ?HOST, ?PORT),
  ?assertEqual(SockPid, SockPid_),
  {ok, ProducerPid} = brod_client:get_producer(Client, ?TOPIC, Partition),
  ?assert(is_process_alive(ProducerPid)),
  ok = ProduceFun().

%%%_* Help functions ===========================================================

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
