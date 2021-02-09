%% Custom signal event handler.
%% Used as a replacement for default `erl_signal_handler` for the purpose of
%% intercepting SIGTERM signal and notify group coordinators via ETS table
%% that shutdown process is initiated and there should no be further attempts
%% to re-join the group.

-module(brod_signal_handler).
-behaviour(gen_event).

-export([ start_link/0
        , init/1
        , handle_event/2
        , handle_info/2
        , handle_call/2
        , terminate/2
        ]).

-include("brod_int.hrl").

-define(STOP_AFTER_SIGTERM_DELAY, 1000).


%% @doc
%% Start (link) a custom signal handler instead of default `erl_signal_handler`.
%% @end
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
  case ets:info(?ERL_SIGNAL_TABLE_NAME) of
    undefined ->
      ets:new(?ERL_SIGNAL_TABLE_NAME, [named_table, public, {read_concurrency, true}]);
    _info ->
      ok
  end,
  ok = gen_event:swap_sup_handler(
    erl_signal_server,
    {erl_signal_handler, []},
    {brod_signal_handler, []}),
  ignore.

init({[], _}) ->
  {ok, {}}.

handle_event(sigterm, State) ->
  io:format("SIGTERM received, raising shutting_down flag and stopping VM in ~p ms~n", [?STOP_AFTER_SIGTERM_DELAY]),
  ets:insert(?ERL_SIGNAL_TABLE_NAME, {shutting_down, true}),
  erlang:send_after(?STOP_AFTER_SIGTERM_DELAY, self(), stop),
  {ok, State}
;
handle_event(ErrorMsg, State) ->
  % proxy other events to default signal handler
  erl_signal_handler:handle_event(ErrorMsg, State),
  {ok, State}.

handle_info(stop, State) ->
  io:format("Stopping due to earlier SIGTERM~n", []),
  ok = init:stop(),
  {ok, State}
;
handle_info(_, State) ->
  {ok, State}.

handle_call(_Request, State) ->
  {ok, ok, State}.

terminate(_Args, _State) ->
  io:format("brod_signal_handler is terminating~n", []),
  ok.
