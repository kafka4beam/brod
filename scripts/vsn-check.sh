#!/bin/bash -e

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"
MAKEFILE="$THIS_DIR/../Makefile"
APP_SRC="$THIS_DIR/../src/brod.app.src"
REBAR_CONFIG="$THIS_DIR/../rebar.config"

PROJECT_VERSION=$1

ESCRIPT=$(cat <<EOF
{ok, [{_,_,L}]} = file:consult('$APP_SRC'),
{vsn, Vsn} = lists:keyfind(vsn, 1, L),
io:put_chars(Vsn),
halt(0)
EOF
)

APP_VSN=$(erl -noshell -eval "$ESCRIPT")

if [ "$PROJECT_VERSION" != "$APP_VSN" ]; then
  echo "version discrepancy, PROJECT_VERSION is '$PROJECT_VERSION', vsn in app.src is '$APP_VSN'"
  exit 1
fi

ESCRIPT=$(cat <<-EOF
{ok, Config} = file:consult('$REBAR_CONFIG'),
{deps, Deps0} = lists:keyfind(deps, 1, Config),
LoopFun =
	fun(_F, eof, []) -> 0;
	   (_F, eof, Deps) -> io:format("deps not found in Makefile but defined in rebar.config\n~p\n", [Deps]), 1;
	   (F, Line, Deps) ->
	      [NameStr, Vsn] = string:tokens(Line, "= \n"),
	      Name = list_to_atom(NameStr),
	      case lists:keyfind(Name, 1, Deps) of
	        {_, V} ->
	          case V =:= Vsn of
	            true ->
	              NewDeps = lists:keydelete(Name, 1, Deps),
	              F(F, io:get_line([]), NewDeps);
	            false ->
	              io:format("dependency ~s version discrepancy, in Makefile: ~s, in rebar.config: ~s\n", [Name, Vsn, V]),
	              2
	          end;
	      false ->
	        io:format("dependency ~s defined in Makefile not found in rebar.config\n", [Name]),
	        3
	    end
end,
halt(LoopFun(LoopFun, io:get_line([]), Deps0)).
EOF
)

grep -E "dep_.*_commit\s=" $MAKEFILE | sed 's/dep_//' | sed 's/_commit//' | erl -noshell -eval "$ESCRIPT"

