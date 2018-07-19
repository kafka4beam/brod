%%%
%%%   Copyright (c) 2016-2018, Klarna Bank AB (publ)
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

%% @private
-module(brod_compression_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_produce_gzip/1
        , t_produce_snappy/1
        %, t_produce_lz4/1
        , t_produce_compressed_batch_consume_from_middle_gzip/1
        , t_produce_compressed_batch_consume_from_middle_snappy/1
        %, t_produce_compressed_batch_consume_from_middle_lz4/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  Client = Case,
  brod:stop_client(Client),
  ok = start_client(?HOSTS, Client),
  NewConfig = [{client, Client} | Config],
  try
    ?MODULE:Case({init, NewConfig})
  catch
    error : function_clause ->
      NewConfig
  end.

end_per_testcase(Case, Config) ->
  try
    ?MODULE:Case({'end', Config})
  catch
    error : function_clause ->
      ok
  end,
  brod:stop_client(?config(client)),
  Config.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%%%_* Test functions ===========================================================

t_produce_gzip(Config) ->
  run(fun produce/1, gzip, Config).

t_produce_snappy(Config) ->
  run(fun produce/1, snappy, Config).

%t_produce_lz4(Config) ->
%  run(fun produce/1, lz4, Config).

t_produce_compressed_batch_consume_from_middle_gzip(Config) ->
  run(fun produce_compressed_batch_consume_from_middle/1, gzip, Config).

t_produce_compressed_batch_consume_from_middle_snappy(Config) ->
  run(fun produce_compressed_batch_consume_from_middle/1, snappy, Config).

%t_produce_compressed_batch_consume_from_middle_lz4(Config) ->
%  run(fun produce_compressed_batch_consume_from_middle/1, lz4, Config).

%%%_* Help functions ===========================================================

run(Case, Compression, Config) ->
  NewConfig = add_config({compression, Compression}, Config),
  Case(NewConfig).

add_config(Config, {Op, Configs}) -> {Op, add_config(Config, Configs)};
add_config(Config, Configs) when is_list(Configs) -> [Config | Configs].

produce(Config) when is_list(Config) ->
  Topic = ?TOPIC,
  Client = ?config(client),
  ProducerConfig = [ {min_compression_batch_size, 0}
                   , {compression, ?config(compression)}
                   ],
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  {K, V} = make_unique_kv(),
  {ok, Offset} = brod:produce_sync_offset(Client, Topic, 0, K, V),
  ok = brod:start_consumer(Client, Topic, [{begin_offset, Offset}]),
  {ok, ConsumerPid} = brod:subscribe(Client, self(), ?TOPIC, 0, []),
  receive
    {ConsumerPid, MsgSet} ->
      #kafka_message_set{ messages = [Message]
                        , partition = 0
                        } = MsgSet,
      ?assertEqual(K, Message#kafka_message.key),
      ?assertEqual(V, Message#kafka_message.value);
    Other ->
      erlang:error({unexpected, Other})
  after 10000 ->
    ct:fail(timeout)
  end,
  ok.

produce_compressed_batch_consume_from_middle(Config) when is_list(Config) ->
  Topic = ?TOPIC,
  Client = ?config(client),
  BatchCount = 100,
  %% get the latest offset before producing the batch
  {ok, Offset0} = brod:resolve_offset(?HOSTS, Topic, 0, latest,
                                      client_config()),
  ct:pal("offset before batch: ~p", [Offset0]),
  ProducerConfig = [ {min_compression_batch_size, 0}
                   , {compression, ?config(compression)}
                   ],
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  KvList = [make_unique_kv() || _ <- lists:seq(1, BatchCount)],
  ok = brod:produce_sync(Client, Topic, 0, <<>>, KvList),
  %% Get the latest offset after the batch is produced
  {ok, Offset1} = brod:resolve_offset(?HOSTS, Topic, 0, latest,
                                      client_config()),
  ct:pal("offset after batch: ~p", [Offset1]),
  ?assertEqual(Offset1, Offset0 + BatchCount),
  HalfBatch = BatchCount div 2,
  BatchMiddle = Offset0 + HalfBatch,
  %% kafka should decompress the compressed message set,
  %% and assign offsets to each and every messages in the batch,
  %% compress it back, then write to disk. fetching from an offset in
  %% the middle of a compressed batch will result in a full
  %% delivery of the compressed batch, but brod_consumer should
  %% filter out the ones before the requested offset.
  ok = brod:start_consumer(Client, Topic, [{begin_offset, BatchMiddle}]),
  {ok, _ConsumerPid} = brod:subscribe(Client, self(), ?TOPIC, 0, []),
  Messages  = receive_messages(BatchCount - HalfBatch, []),
  Expected0 = lists:zip(lists:seq(Offset0, Offset1-1), KvList),
  Expected  = lists:sublist(Expected0, HalfBatch+1, BatchCount-HalfBatch),
  lists:foreach(
    fun({{Offset, {K, V}}, Message}) ->
      ?assertMatch(#kafka_message{ offset = Offset
                                 , key    = K
                                 , value  = V
                                 }, Message)
    end, lists:zip(Expected, Messages)).

receive_messages(0, Acc) -> Acc;
receive_messages(Count, Acc) when Count > 0 ->
  receive
    {_ConsumerPid, MsgSet} ->
      #kafka_message_set{messages = Messages} = MsgSet,
      receive_messages(Count - length(Messages), Acc ++ Messages);
    Other ->
      erlang:error({unexpected, Other})
  after 10000 ->
    erlang:error(timeout)
  end.

%% os:timestamp should be unique enough for testing
make_unique_kv() ->
  { iolist_to_binary(["key-", make_ts_str()])
  , iolist_to_binary(["val-", make_ts_str()])
  }.

make_ts_str() -> brod_utils:os_time_utc_str().

start_client(Hosts, ClientId) ->
  Config = client_config(),
  brod:start_client(Hosts, ClientId, Config).

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
