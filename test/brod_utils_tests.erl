%%%
%%%   Copyright (c) 2018-2021, Klarna Bank AB (publ)
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

-module(brod_utils_tests).

-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

make_batch_input_test_() ->
  [fun() -> ?assertMatch([#{key := <<>>, value := <<>>}],
                         mk(?undef, ?undef)) end,
   fun() -> ?assertMatch([#{key := <<>>, value := <<>>}], mk([], [])) end,
   fun() -> ?assertMatch([#{key := <<"foo">>}], mk("foo", [])) end,
   fun() -> ?assertMatch([#{value:= <<"foo">>}], mk("bar", "foo")) end,
   fun() -> ?assertMatch([#{ts := 1, key := <<>>, value:= <<"foo">>}],
                         mk("", {1, "foo"})) end,
   fun() -> ?assertMatch([#{key := <<"foo">>, value := <<"bar">>}],
                         mk("ignore", [{"foo", "bar"}])) end,
   fun() -> ?assertMatch([#{ts := 1, key := <<"foo">>, value := <<"bar">>}],
                         mk("ignore", [{1, "foo", "bar"}])) end,
   fun() -> ?assertMatch([#{ts := 1, key := <<"k1">>, value := <<"v1">>},
                          #{ts := 2, key := <<"k2">>, value := <<"v2">>},
                          #{ts := 3, key := <<"k3">>, value := <<"v3">>}],
                         mk("ignore", [{1, "k1", "v1"},
                                       {<<>>, [{2, "k2", "v2"},
                                               {4, <<>>, [{3, "k3", "v3"}]}
                                              ]}
                                      ])) end,
   fun() -> ?assertMatch([#{ts := _, key := <<"foo">>, value := <<"bar">>}],
                         mk("foo", #{value => "bar"})) end,
   fun() -> ?assertMatch([#{ts := _, key := <<"key">>, value := <<"bar">>}],
                         mk("foo", #{key => "key", value => "bar"})) end,
   fun() -> ?assertMatch([#{ts := 1, key := <<"key">>, value := <<"v">>}],
                         mk("foo", #{ts => 1, key => "key", value => "v"})) end,
   fun() -> ?assertEqual([#{ts => 1, key => <<"k">>, value => <<"v">>,
                            headers => [{<<"hk">>, <<"hv">>}]}],
                         mk("foo", #{ts => 1, key => "k", value => "v",
                                     headers => [{"hk", "hv"}]})) end
  ].

mk(K, V) -> brod_utils:make_batch_input(K, V).

pmap_test_() ->
  [fun() ->
    %% Basic functionality - simple map operation
    Result = brod_utils:pmap(fun(X) -> X * 2 end, [1, 2, 3, 4, 5], infinity),
    ?assertEqual([2, 4, 6, 8, 10], Result)
   end,
   fun() ->
    %% Empty list
    Result = brod_utils:pmap(fun(X) -> X end, [], infinity),
    ?assertEqual([], Result)
   end,
   fun() ->
    %% Order preservation - results should be in same order as input
    Result = brod_utils:pmap(fun(X) -> X end, [3, 1, 4, 1, 5], infinity),
    ?assertEqual([3, 1, 4, 1, 5], Result)
   end,
   fun() ->
    %% Numeric transformation
    Result = brod_utils:pmap(fun(X) -> X + 1 end, [10, 20, 30], infinity),
    ?assertEqual([11, 21, 31], Result)
   end,
   fun() ->
    %% Large list
    List = lists:seq(1, 100),
    Result = brod_utils:pmap(fun(X) -> X * 2 end, List, infinity),
    Expected = [X * 2 || X <- List],
    ?assertEqual(Expected, Result)
   end,
   fun() ->
     %% Timeout behavior - function that takes longer than timeout
     StartTime = erlang:monotonic_time(millisecond),
     try
       brod_utils:pmap(fun(X) -> timer:sleep(200), X * 2 end, [1, 2, 3], 50),
       %% Should not reach here
       ?assert(false)
       catch
         throw:timeout ->
           EndTime = erlang:monotonic_time(millisecond),
           %% Should timeout quickly (within timeout + small overhead)
           ?assert(EndTime - StartTime < 100)
     end
   end,
   fun() ->
     %% Infinity timeout - should complete successfully
     Result = brod_utils:pmap(fun(X) -> timer:sleep(10), X * 2 end, [1, 2, 3], infinity),
     ?assertEqual([2, 4, 6], Result)
   end,
   fun() ->
     %% Single element list
     Result = brod_utils:pmap(fun(X) -> X + 100 end, [42], infinity),
     ?assertEqual([142], Result)
   end,
   fun() ->
     %% Function that returns different types
     Result = brod_utils:pmap(fun(X) -> {result, X} end, [1, 2, 3], infinity),
     ?assertEqual([{result, 1}, {result, 2}, {result, 3}], Result)
   end].

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
