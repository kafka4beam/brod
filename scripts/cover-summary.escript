#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -sname coversummay

%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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
%%%=============================================================================

-mode(compile).

main([CoverDataFileDir]) ->
  {ok, CoverDataFile} = find_latest_coverdata(CoverDataFileDir),
  io:format("using coverdata file: ~s\n", [CoverDataFile]),
  Parent = self(),
  Ref = make_ref(),
  erlang:spawn_link(
    fun() ->
      %% shutup the chatty prints from cover:xxx calls
      {ok, F} = file:open("/dev/null", [write]),
      group_leader(F, self()),
      ok = cover:import(CoverDataFile),
      Modules = cover:imported_modules(),
      Result = [{Mod, analyse_module(Mod)} || Mod <- Modules],
      Parent ! {Ref, Result}
    end),
  receive
    {Ref, Result} ->
      print_summary(Result)
  end.

find_latest_coverdata(Dir) ->
  Files = filelib:fold_files(Dir, ".*all.coverdata", true,
                             fun(N, Acc) -> [N | Acc] end, []),
  {ok, lists:last(lists:sort(Files))}.

-spec analyse_module([{module(), Line::integer(), {Covered, NotCovered}}]) ->
        {Covered, NotCovered} when Covered :: integer(),
                                   NotCovered :: integer().
analyse_module(Module) ->
  {ok, Lines} = cover:analyse(Module, coverage, line),
  lists:foldl(
    fun({{_Mod, 0}, _}, Acc)                   -> Acc;
       ({{_Mod, _}, {C, Nc}}, {C_Acc, Nc_Acc}) -> {C + C_Acc, Nc + Nc_Acc}
    end, {0, 0}, Lines).

-spec print_summary([{module(), {Covered :: integer(),
                                 NotCovered :: integer()}}]) -> ok.
print_summary(Coverage) ->
  Width = lists:max([length(atom_to_list(M)) || {M, _} <- Coverage]),
  fmt_line(Width, hd, hd, hd, hd),
  fmt_line(Width, hl, hl, hl, hl),
  print_coverage(Width, Coverage).

print_coverage(_Width, []) -> ok;
print_coverage(Width, [{Module, {Covered, NotCovered}} | Rest]) ->
  Percent =
    case Covered + NotCovered of
      0 -> 0;
      N -> erlang:round(100 * Covered / N)
    end,
  fmt_line(Width, Module, Covered, NotCovered, Percent),
  print_coverage(Width, Rest).

fmt_line(Width, Mod, Covered, NotCovered, Coverage) ->
  io:format("~s ~s ~s ~s\n",
            [ col_module(Mod, Width)
            , col_covered(Covered)
            , col_not_covered(NotCovered)
            , col_coverage(Coverage)
            ]).

module_str(Width, Module) ->
  FmtStr = "~" ++ integer_to_list(Width) ++ "s",
  lists:flatten(io_lib:format(FmtStr, [atom_to_list(Module)])).

col_module(hd, Width)  -> module_str(Width, 'module');
col_module(hl, Width)  -> lists:duplicate(Width, $-);
col_module(Mod, Width) -> module_str(Width, Mod).

col_covered(hd)  -> "covered";
col_covered(hl)  -> "-------";
col_covered(Val) -> str("~7B", [Val]).

col_not_covered(hd)  -> "not-covered";
col_not_covered(hl)  -> "-----------";
col_not_covered(Val) -> str("~11B", [Val]).

col_coverage(hd)  -> "coverage";
col_coverage(hl)  -> "--------";
col_coverage(Val) -> str("~7B%", [Val]).

str(Fmt, Args) -> lists:flatten(io_lib:format(Fmt, Args)).

