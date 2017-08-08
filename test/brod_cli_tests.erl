%%%
%%%   Copyright (c) 2017, Klarna AB
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

-module(brod_cli_tests).

-ifdef(BROD_CLI).

-include_lib("eunit/include/eunit.hrl").

%% no crash on 'help', 'version' etc commands
informative_test() ->
  run(["--help"]),
  run(["--version"]),
  run(["meta", "--help", "--debug"]).

meta_test() ->
  run(["meta", "-b", "localhost", "-L"]),
  run(["meta", "-b", "localhost", "-t", "test-topic"]).

ssl_test() ->
  run(["meta", "-b", "localhost:9192", "-L",
       "--cacertfile", "priv/ssl/ca.crt",
       "--keyfile", "priv/ssl/client.key",
       "--certfile", "priv/ssl/client.crt"]).

offset_test() ->
  Args = ["offset", "-b", "localhost", "-t", "test-topic", "-p", "0"],
  run(Args),
  run(Args ++ ["-T", "latest"]),
  run(Args ++ ["-T", "earliest"]),
  run(Args ++ ["-T", "-1"]),
  run(Args ++ ["-T", "-2"]),
  run(Args ++ ["-T", "0"]).

send_fetch_test() ->
  K = make_ts_str(),
  V = make_ts_str(),
  Output =
    cmd("send --brokers localhost:9092,localhost:9093 -t test-topic "
        "-p 0 -k " ++ K ++ " -v " ++ V),
  ?assertEqual(Output, ""),
  FetchOutput =
    cmd("fetch --brokers localhost:9092 -t test-topic -p 0 "
        "-c 1 --fmt kv"),
  ?assertEqual(FetchOutput, K ++ ":" ++ V ++ "\n"),
  ok.

sasl_test() ->
  ok = file:write_file("sasl.testdata", "alice\nalice-secret\n"),
  K = make_ts_str(),
  V = make_ts_str(),
  Output =
    cmd("send --brokers localhost:9292,localhost:9392 -t test-topic "
        "--cacertfile priv/ssl/ca.crt "
        "--keyfile priv/ssl/client.key "
        "--certfile priv/ssl/client.crt "
        "--sasl-plain sasl.testdata "
        "-p 0 -k " ++ K ++ " -v " ++ V),
  ?assertEqual(Output, ""),
  FetchOutput =
    cmd("fetch --brokers localhost:9092 -t test-topic -p 0 "
        "-c 1 --fmt kv"),
  ?assertEqual(FetchOutput, K ++ ":" ++ V ++ "\n"),
  ok.

fetch_format_fun_test() ->
  T = os:timestamp(),
  Value = term_to_binary(T),
  file:write_file("fetch.testdata", Value),
  cmd("send -b localhost -t test-topic -p 0 -v @fetch.testdata"),
  FmtFun = "fun(_O, _K, V) -> io_lib:format(\"~p\", [binary_to_term(V)]) end",
  Output =
    cmd("fetch -b localhost -t test-topic -p 0 -c 1 --fmt '" ++ FmtFun ++ "'"),
  Expected = lists:flatten(io_lib:format("~p", [T])),
  ?assertEqual(Expected, Output).

pipe_test() ->
  %% get last offset
  OffsetStr = cmd("offset -b localhost -t test-topic -p 0 -T latest"),
  %% send file
  PipeCmdOutput =
    cmd("pipe -b localhost -t test-topic -p 0 -s README.md "
        "--kv-deli none --msg-deli '\\n'"),
  ?assertEqual("", PipeCmdOutput),
  FetchOutput =
    cmd("fetch -b localhost -t test-topic -p 0 -w 100 -c -1 -o " ++ OffsetStr),
  FetchedLines = iolist_to_binary(FetchOutput),
  {ok, Expected0} = file:read_file("README.md"),
  Expected1 = binary:split(Expected0, <<"\n">>, [global]),
  Expected2 = lists:filtermap(fun(<<>>) -> false;
                                 (Line) -> {true, [Line, "\n"]}
                              end, Expected1),
  Expected = iolist_to_binary(Expected2),
  ?assertEqual(Expected, FetchedLines).

groups_test() ->
  assert_no_error(cmd("groups")),
  assert_no_error(cmd("groups --describe")),
  assert_no_error(cmd("groups --describe --ids all")).

assert_no_error(Result) ->
  case binary:match(iolist_to_binary(Result), <<"***">>) of
    nomatch -> ok;
    _ -> erlang:throw(Result)
  end.

run(Args) -> brod_cli:main(Args, exit).

cmd(ArgsStr) ->
  os:cmd("scripts/brod " ++ ArgsStr).

make_ts_str() ->
  Ts = os:timestamp(),
  {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
