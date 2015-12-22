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

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, <<"brod-client-SUITE-topic">>).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
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
      Config
  end.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].


%%%_* Test functions ===========================================================

t_skip_unreachable_endpoint(Config) when is_list(Config) ->
  Client = t_skip_unreachable_endpoint,
  {ok, Pid} = brod:start_link_client(Client, [{"localhost", 8092} | ?HOSTS],
                                     _Config = [], _Producers = []),
  ?assert(is_pid(Pid)),
  _Res = brod_client:get_partitions(Pid, <<"some-unknown-topic">>),
  % auto.create.topics.enabled is 'true' in default spotify/kafka container
  % ?assertEqual({error, 'UnknownTopicOrPartitionException'}, _Res),
  Ref = erlang:monitor(process, Pid),
  ok = brod:stop_client(Pid),
  receive
    {'DOWN', Ref, process, Pid, Reason} ->
      ?assertEqual(normal, Reason)
  after 5000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end.

t_no_reachable_endpoint(Config) when is_list(Config) ->
  process_flag(trap_exit, true),
  {ok, Pid} = brod:start_link_client([{"badhost", 9092}], _Producers = []),
  receive
    {'EXIT', Pid, Reason} ->
      ?assertMatch({nxdomain, _Stacktrace}, Reason)
  after 1000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end.

t_not_a_brod_client(Config) when is_list(Config) ->
  %% make some random pid
  Pid = erlang:spawn(fun() -> ok end),
  Res1 = brod:produce(Pid, <<"topic">>, _Partition = 0, <<"k">>, <<"v">>),
  ?assertEqual({error, client_down}, Res1),
  %% call a bad client ID
  Res2 = brod:produce(?undef, <<"topic">>, _Partition = 0, <<"k">>, <<"v">>),
  ?assertEqual({error, client_down}, Res2).

t_metadata_socket_restart({init, Config}) ->
  meck:new(brod_sock, [passthrough]),
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
  Tester = self(),
  %% tap the call to brod_sock:start_link/5,
  %% intercept the returned socket pid
  %% and send it to the test process: self()
  SocketStartLinkFun =
    fun(Parent, Host, Port, ClientId, Dbg) ->
      {ok, Pid} = meck:passthrough([Parent, Host, Port, ClientId, Dbg]),
      %% assert the caller
      ?assertEqual(Parent, whereis(t_metadata_socket_restart)),
      Tester ! {socket_started, Pid},
      {ok, Pid}
    end,
  ok = meck:expect(brod_sock, start_link, SocketStartLinkFun),
  {ok, ClientPid} =
    brod:start_link_client(t_metadata_socket_restart, ?HOSTS,
                           _Config = [], _Producers = []),
  receive
    {socket_started, SocketPid} ->
      ?assert(is_process_alive(ClientPid)),
      ?assert(is_process_alive(SocketPid)),
      %% kill the brod_sock pid
      exit(SocketPid, kill)
  after 5000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end,
  %% expect the socket pid get restarted rightaway
  receive
    {socket_started, SocketPid2} ->
      ?assert(is_process_alive(ClientPid)),
      ?assert(is_process_alive(SocketPid2))
  after 5000 ->
    ct:fail({?MODULE, ?LINE, timeout})
  end,
  brod_client:get_metadata(ClientPid, ?TOPIC),
  ok.

%%%_* Help functions ===========================================================

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
