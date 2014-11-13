%%% ============================================================================
%%% @doc Mock brod_sock for unit tests
%%% @copyright 2014 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_sock).

-export([ send/2
        ]).

%%%_* API ======================================================================
%% connect(Host, Port, SockOpts) ->
%%   proc_lib:start_link(?MODULE, init, [self(), Host, Port, SockOpts]).

send(_Leader, _Produce) ->
  {ok, 1}.

%% close(Sock) ->
%%   Sock ! close,
%%   ok.

%%%_* Internal functions =======================================================
%% init(Parent, _Host, _Port, _SockOpts) ->
%%   proc_lib:init_ack(Parent, {ok, self()}),
%%   loop().

%% loop() ->
%%   receive
%%     {send, _Bin} ->
%%       ?MODULE:loop();
%%     close ->
%%       ok
%%   end.
