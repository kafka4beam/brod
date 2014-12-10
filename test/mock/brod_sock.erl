%%% ============================================================================
%%% @doc Mock brod_sock for unit tests
%%% @copyright 2014 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_sock).

-export([ send/2
        , send_sync/3
        , start/4
        , start_link/4
        , stop/1
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("../../src/brod_int.hrl").

%%%_* API ======================================================================
start_link(_Parent, "h1", _Port, _Debug) ->
  {ok, p1};
start_link(_Parent, "h2", _Port, _Debug) ->
  {ok, p2};
start_link(_Parent, "h3", _Port, _Debug) ->
  {ok, p3}.

start(_Parent, "h1", _Port, _Debug) ->
  {ok, p1};
start(_Parent, "h2", _Port, _Debug) ->
  {ok, p2};
start(_Parent, "h3", _Port, _Debug) ->
  {ok, p3}.

stop(_Pid) ->
  ok.

send(_Pid, _Request) ->
  {ok, 1}.

send_sync(p1, #metadata_request{} = R, _Timeout) ->
  {ok, #metadata_response{brokers = [], topics = []}};
send_sync(p2, #metadata_request{} = R, _Timeout) ->
  Topics = [#topic_metadata{name = <<"t">>, partitions = []}],
  {ok, #metadata_response{brokers = [], topics = Topics}};
send_sync(p3, #metadata_request{} = R, _Timeout) ->
  Brokers = [#broker_metadata{node_id = 1, host = "h3", port = 2181}],
  Partitions = [#partition_metadata{id = 0, leader_id = 1}],
  Topics = [#topic_metadata{name = <<"t">>, partitions = Partitions}],
  {ok, #metadata_response{brokers = Brokers, topics = Topics}}.

