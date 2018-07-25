%%%
%%%   Copyright (c) 2018, Klarna Bank AB (publ)
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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
