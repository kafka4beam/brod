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

%% @private
-module(brod_sock_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_request_timeout/1
        ]).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_Case, Config) when is_list(Config) ->
  ok.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

t_request_timeout({init, Config}) ->
  meck:new(ssl, [passthrough, no_passthrough_cover, no_history]),
  Config;
t_request_timeout({'end', Config}) ->
  meck:validate(ssl),
  meck:unload(ssl),
  Config;
t_request_timeout(Config) when is_list(Config) ->
  TesterPid = self(),
  meck:expect(ssl, connect, fun(_, _, _) -> {ok, TesterPid} end),
  meck:expect(ssl, setopts, fun(_, _) -> ok end),
  meck:expect(ssl, send, fun(Pid, _Bin) ->
                                 ?assertEqual(Pid, TesterPid),
                                 ok
                             end),
  %% spawn an isolated middleman
  %% so we dont get killed when brod_sock exits
  {Pid, Ref} =
    spawn_monitor(
      fun() ->
          {ok, SockPid} =
            brod_sock:start_link(self(), "localhost", 9092, client_id,
                                [{ssl, true}, {request_timeout, 1000}]),
          TesterPid ! {sock, SockPid},
          receive  Msg -> exit({<<"unexpected message">>, Msg})
          after 10000  -> exit(<<"test timeout">>)
          end
      end),
  Sock = receive {sock, P} -> P
         after 5000 -> erlang:exit(timeout)
         end,
  ProduceRequest = kpro:produce_request(0, <<"t">>, 0, [{<<"K">>, <<"V">>}],
                                        1, 1000, no_compression),
  _ = brod_sock:request_async(Sock, ProduceRequest),
  receive
    {_DOWN, Ref, process, Pid, Reason} ->
      ?assertEqual(Reason, request_timeout);
    Msg ->
      erlang:exit({<<"unexpected">>, Msg})
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
