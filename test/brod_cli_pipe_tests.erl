%%%
%%%   Copyright (c) 2017-2018, Klarna Bank AB (publ)
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

-module(brod_cli_pipe_tests).

-ifdef(build_brod_cli).

-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(WAIT(Pattern, Handle, Timeout),
        fun Wait() ->
          receive
            {'EXIT', _Pid, normal} ->
              %% discard normal exits of linked pid
              Wait();
            Pattern ->
              Handle;
            Msg ->
              erlang:error({unexpected,
                            [{line, ?LINE},
                             {pattern, ??Pattern},
                             {received, Msg}]})
          after
            Timeout ->
              erlang:error({timeout,
                            [{line, ?LINE},
                             {pattern, ??Pattern}]})
          end
        end()).

-define(TEST_FILE, "pipe.testdata").

line_mode_test() ->
  Lines = ["key1\n", "val1\n", "key2\n", "val2\n", "key3\n"],
  ok = file:write_file(?TEST_FILE, Lines),
  Args =
    [ {source, {file, ?TEST_FILE}}
    , {kv_deli, <<"\n">>}
    , {msg_deli, <<"\n">>}
    , {prompt, false}
    , {tail, false}
    , {no_exit, false}
    , {blk_size, ignore}
    , {retry_delay, ignore}
    ],
  {ok, Pid} = brod_cli_pipe:start_link(Args),
  erlang:monitor(process, Pid),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key1">>, <<"val1">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key2">>, <<"val2">>)]}, ok, 1000),
  ?WAIT({'DOWN', _Ref, process, Pid, normal}, ok, 1000),
  ok.

line_mode_no_key_test() ->
  Lines = ["val1\n", "val2\n", "val3\n"],
  ok = file:write_file(?TEST_FILE, Lines),
  Args =
    [ {source, {file, ?TEST_FILE}}
    , {kv_deli, none}
    , {msg_deli, <<"\n">>}
    , {prompt, true}
    , {tail, false}
    , {no_exit, false}
    , {blk_size, ignore}
    , {retry_delay, ignore}
    ],
  {ok, Pid} = brod_cli_pipe:start_link(Args),
  erlang:monitor(process, Pid),
  ?WAIT({pipe, Pid, [?TKV(_, <<>>, <<"val1">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<>>, <<"val2">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<>>, <<"val3">>)]}, ok, 1000),
  ?WAIT({'DOWN', _Ref, process, Pid, normal}, ok, 1000),
  ok.

line_mode_split_test() ->
  Lines = ["key1:val1\n", "key2:val2\n"],
  ok = file:write_file(?TEST_FILE, Lines),
  Args =
    [ {source, {file, ?TEST_FILE}}
    , {kv_deli, <<":">>}
    , {msg_deli, <<"\n">>}
    , {prompt, false}
    , {tail, false}
    , {no_exit, false}
    , {blk_size, 1}
    , {retry_delay, ignore}
    ],
  {ok, Pid} = brod_cli_pipe:start_link(Args),
  erlang:monitor(process, Pid),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key1">>, <<"val1">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key2">>, <<"val2">>)]}, ok, 1000),
  ?WAIT({'DOWN', _Ref, process, Pid, normal}, ok, 1000),
  ok.

stream_one_byte_delimiter_test() ->
  Data = "key1:val1#key2:val2#key3:val3",
  ok = file:write_file(?TEST_FILE, Data),
  Args =
    [ {source, {file, ?TEST_FILE}}
    , {kv_deli, <<":">>}
    , {msg_deli, <<"#">>}
    , {prompt, false}
    , {tail, false}
    , {no_exit, false}
    , {blk_size, 2}
    , {retry_delay, ignore}
    ],
  {ok, Pid} = brod_cli_pipe:start_link(Args),
  erlang:monitor(process, Pid),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key1">>, <<"val1">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key2">>, <<"val2">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key3">>, <<"val3">>)]}, ok, 1000),
  ?WAIT({'DOWN', _Ref, process, Pid, normal}, ok, 1000),
  ok.

stream_test() ->
  Data = "key1::val1###key2::val2###key3::val3##",
  ok = file:write_file(?TEST_FILE, Data),
  Args =
    [ {source, {file, ?TEST_FILE}}
    , {kv_deli, <<"::">>}
    , {msg_deli, <<"###">>}
    , {prompt, false}
    , {tail, false}
    , {no_exit, true}
    , {blk_size, 3}
    , {retry_delay, 10}
    ],
  {ok, Pid} = brod_cli_pipe:start_link(Args),
  erlang:monitor(process, Pid),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key1">>, <<"val1">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key2">>, <<"val2">>)]}, ok, 1000),
  ?assertError({timeout, _},
               ?WAIT({pipe, Pid, [{<<"key3">>, <<"val3">>}]}, ok, 500)),
  MoreData = "#key-4::val-4###",
  ok = file:write_file(?TEST_FILE, MoreData, [append]),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key3">>, <<"val3">>)]}, ok, 1000),
  ?WAIT({pipe, Pid, [?TKV(_, <<"key-4">>, <<"val-4">>)]}, ok, 1000),
  ok = brod_cli_pipe:stop(Pid),
  ?WAIT({'DOWN', _Ref, process, Pid, normal}, ok, 1000),
  ok.

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
