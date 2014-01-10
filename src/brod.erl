%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).

%% API
-export([ connect/2
        , close_connection/1
        , get_metadata/1
        ]).

%%%_* Includes -----------------------------------------------------------------

%%%_* Types --------------------------------------------------------------------
-type connection() :: port().

%%%_* API ----------------------------------------------------------------------
-spec connect(string(), integer()) -> {ok, connection()} | {error, any()}.
connect(Host, Port) ->
  SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}],
  gen_tcp:connect(Host, Port, SockOpts).

-spec close_connection(connection()) -> ok.
close_connection(Conn) ->
  gen_tcp:close(Conn).

-spec get_metadata(connection()) -> {ok, any()} | {error, any()}.
get_metadata(Conn) ->
  CorrId = 0,
  Request = kafka:metadata_request(CorrId),
  ok = gen_tcp:send(Conn, Request),
  {ok, <<Length:32/integer>>} = gen_tcp:recv(Conn, 4),
  {ok, <<CorrId:32/integer, Bin/binary>>} = gen_tcp:recv(Conn, Length),
  kafka:metadata_response(Bin).

%%%_* Internal functions -------------------------------------------------------

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
