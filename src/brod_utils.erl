%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_utils).

%% Exports
-export([ fetch_metadata/1
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* Code ---------------------------------------------------------------------
%% try to connect to any of bootstrapped nodes and fetch metadata
fetch_metadata(Hosts) ->
  SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}],
  {ok, Sock} = try_connect(Hosts, SockOpts),
  CorrId = 0,
  RequestBin = kafka:encode(CorrId, #metadata_request{}),
  ok = gen_tcp:send(Sock, RequestBin),
  {ok, Bin} = gen_tcp:recv(Sock, 0),
  gen_tcp:close(Sock),
  {[{CorrId, ResponseBin}], <<>>} = kafka:pre_parse_stream(Bin),
  {ok, kafka:decode_metadata(ResponseBin)}.

try_connect(Hosts, SockOpts) ->
  try_connect(Hosts, SockOpts, []).

try_connect([], _, LastError) ->
  LastError;
try_connect([{Host, Port} | Hosts], SockOpts, _) ->
  case gen_tcp:connect(Host, Port, SockOpts) of
    {ok, Sock} -> {ok, Sock};
    Error      -> try_connect(Hosts, SockOpts, Error)
  end.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
