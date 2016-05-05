%%%
%%%   Copyright (c) 2016, Klarna AB
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
%%% ============================================================================

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
        , t_produce_compressed_batch_consume_from_middle_gzip/1
        , t_produce_compressed_batch_consume_from_middle_snappy/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("brod/src/brod_int.hrl").

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
  ok = brod:start_client(?HOSTS, Client),
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

t_produce_compressed_batch_consume_from_middle_gzip(Config) ->
  run(fun produce_compressed_batch_consume_from_middle/1, gzip, Config).

t_produce_compressed_batch_consume_from_middle_snappy(Config) ->
  run(fun produce_compressed_batch_consume_from_middle/1, snappy, Config).

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
  {ok, [Offset0]} = brod:get_offsets(?HOSTS, Topic, 0),
  ok = brod:produce_sync(Client, Topic, 0, K, V),
  {ok, [Offset1]} = brod:get_offsets(?HOSTS, Topic, 0),
  ?assert(Offset1 =:= Offset0 + 1),
  ok = brod:start_consumer(Client, Topic, [{begin_offset, Offset0}]),
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

produce_compressed_batch_consume_from_middle({init, Config}) ->
  BatchCount = 10, %% any value >= 2
  %% mock brod_producer_buffer:maybe_send to simply return the buffer
  %% for the first 9 calls, then passthrough from the 10th call
  %% hence the 10th call will produce a batch
  SendFun = fun(Buf, Pid) ->
              NthCall = case get(nth_call) of
                          undefined -> 1;
                          N         -> N+1
                        end,
              put(nth_call, NthCall),
              case NthCall >= BatchCount of
                true  -> meck:passthrough([Buf, Pid]);
                false -> {ok, Buf}
              end
            end,
  %% hijack kpro:produce_request/6 to verify if the args are as expected
  MakeRequestFun =
    fun(T, P, KVL, RequiredAcks, AckTimeout, Compression) ->
      ?assertEqual(?config(compression), Compression),
      ?assertEqual(BatchCount, length(KVL)),
      meck:passthrough([T, P, KVL, RequiredAcks, AckTimeout, Compression])
    end,
  meck:new(brod_producer_buffer,
           [passthrough, no_passthrough_cover, no_history]),
  meck:new(kpro, [passthrough, no_passthrough_cover, no_history]),
  meck:expect(brod_producer_buffer, maybe_send, 2, SendFun),
  meck:expect(kpro, produce_request, 6, MakeRequestFun),
  [{batch_count, BatchCount} | Config];
produce_compressed_batch_consume_from_middle({'end', Config}) ->
  meck:validate(brod_producer_buffer),
  meck:unload(brod_producer_buffer),
  meck:validate(kpro),
  meck:unload(kpro),
  Config;
produce_compressed_batch_consume_from_middle(Config) when is_list(Config) ->
  Topic = ?TOPIC,
  Client = ?config(client),
  BatchCount = ?config(batch_count),
  %% get the latest offset before producing the batch
  {ok, [Offset0]} = brod:get_offsets(?HOSTS, Topic, 0),
  ct:pal("offset before batch: ~p", [Offset0]),
  ProducerConfig = [ {min_compression_batch_size, 0}
                   , {compression, ?config(compression)}
                   ],
  ok = brod:start_producer(Client, Topic, ProducerConfig),
  KvList = [make_unique_kv() || _ <- lists:seq(1, BatchCount)],
  RefList =
    lists:map(fun({K, V}) ->
                {ok, Ref} = brod:produce(Client, Topic, 0, K, V),
                Ref
              end, KvList),
  ok = lists:foreach(fun brod:sync_produce_request/1, RefList),
  %% Get the latest offset after the batch is produced
  {ok, [Offset1]} = brod:get_offsets(?HOSTS, Topic, 0),
  ct:pal("offset after batch: ~p", [Offset1]),
  ?assertEqual(Offset1, Offset0 + BatchCount),
  BatchMiddle = BatchCount div 2,
  %% kafka should decompress the compressed message set,
  %% and assign offsets to each and every messages in the batch
  %% compress, then write to disk. fetching from an offset in
  %% the middle of a compressed batch should result in a full
  %% delivery of the compressed batch.
  ok = brod:start_consumer(Client, Topic,
                           [{begin_offset, Offset0 + BatchMiddle}]),
  {ok, _ConsumerPid} = brod:subscribe(Client, self(), ?TOPIC, 0, []),
  Messages = receive_messages(BatchCount, []),
  Expected = lists:zip(lists:seq(Offset0, Offset1-1), KvList),
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

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
