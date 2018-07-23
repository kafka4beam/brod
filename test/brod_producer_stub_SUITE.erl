%%%
%%%   Copyright (c) 2017-2018 Klarna Bank AB (publ)
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
-module(brod_producer_stub_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_normal_flow/1
        , t_no_required_acks/1
        , t_retry_on_same_connection/1
        , t_connection_down_retry/1
        , t_leader_migration/1
        ]).


-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(WAIT(Pattern, Handle, Timeout),
        fun() ->
          receive
            Pattern ->
              Handle;
            Msg ->
              erlang:error({unexpected,
                            [{line, ?LINE},
                             {pattern, ??Pattern},
                             {received, Msg}]})
          after
            Timeout ->
              erlang:error({timeout,
                            [{line, ?LINE},
                             {pattern, ??Pattern}]})
          end
        end()).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  meck_module(brod_client),
  meck_module(kpro),
  meck_module(brod_kafka_apis),
  meck:expect(brod_kafka_apis, pick_version,
              fun(_, API) ->
                  brod_kafka_apis:default_version(API)
              end),
  try
    ?MODULE:Case({'init', Config})
  catch
    error : function_clause ->
      Config
  end.

end_per_testcase(Case, Config) ->
  meck:unload(brod_client),
  meck:unload(kpro),
  meck:unload(brod_kafka_apis),
  try
    ?MODULE:Case({'end', Config})
  catch
    error : function_clause ->
      Config
  end.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%%%_* Tests ====================================================================

t_normal_flow(Config) when is_list(Config) ->
  Tester = self(),
  meck:expect(brod_client, get_leader_connection,
              fun(_, <<"topic">>, 0) -> {ok, Tester} end),
  meck:expect(kpro, request_async,
              fun(Connection, KafkaReq) ->
                  Connection ! {request_async, KafkaReq},
                  ok
              end),
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0, []),
  {ok, CallRef} = brod_producer:produce(Producer, <<"key">>, <<"val">>),
  ?WAIT({request_async, #kpro_req{ref = Ref}},
        Producer ! {msg, Tester, fake_rsp(Ref, <<"topic">>, 0)}, 2000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef,
                            result = brod_produce_req_acked},
        ok, 2000),
  ok = brod_producer:stop(Producer).

%% When required acks set to 0
%% no produce response will be received from kafka
%% but still caller should receive 'acked' reply
t_no_required_acks({init, Config}) ->
  meck_module(brod_kafka_request),
  Config;
t_no_required_acks({'end', Config}) ->
  meck:unload(brod_kafka_request),
  Config;
t_no_required_acks(Config) when is_list(Config) ->
  MaxLingerCount = 3,
  Tester = self(),
  meck:expect(brod_client, get_leader_connection,
              fun(_, <<"topic">>, 0) -> {ok, Tester} end),
  meck:expect(brod_kafka_request, produce,
              fun(_, <<"topic">>, 0, Batch, 0, _, no_compression) ->
                  case length(Batch) =:= MaxLingerCount of
                    true ->
                      ?assertMatch([#{key := <<"1">>, value := <<"1">>},
                                    #{key := <<"2">>, value := <<"2">>},
                                    #{key := <<"3">>, value := <<"3">>}
                                   ], Batch);
                    false ->
                      ?assertMatch([#{key := <<"4">>, value := <<"4">>}], Batch)
                  end,
                  #kpro_req{api = produce, no_ack = true}
              end),
  meck:expect(kpro, request_async, fun(_Conn, _Req) -> ok end),
  ProducerConfig = [{required_acks, 0},
                    {max_linger_ms, 1000},
                    {max_linger_count, MaxLingerCount}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  %% The first 3 message are collected into a batch
  {ok, CallRef1} = brod_producer:produce(Producer, <<"1">>, <<"1">>),
  {ok, CallRef2} = brod_producer:produce(Producer, <<"2">>, <<"2">>),
  {ok, CallRef3} = brod_producer:produce(Producer, <<"3">>, <<"3">>),
  %% The 4-th message is not included in the batch
  {ok, CallRef4} = brod_producer:produce(Producer, <<"4">>, <<"4">>),
  ?WAIT(#brod_produce_reply{call_ref = CallRef1,
                            result = brod_produce_req_acked}, ok, 2000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef2,
                            result = brod_produce_req_acked}, ok, 2000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef3,
                            result = brod_produce_req_acked}, ok, 2000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef4,
                            result = brod_produce_req_acked}, ok, 2000),
  ok = brod_producer:stop(Producer).

t_retry_on_same_connection(Config) when is_list(Config) ->
  Tester = self(),
  %% A mocked connection process which expects 2 requests to be sent
  %% reply a retriable error code for the first one
  %% and succeed the second one
  ConnLoop =
    fun Loop(N) ->
        receive
          {produce, ProducerPid, #kpro_req{ref = Ref}} ->
            Rsp = case N of
                    1 -> fake_rsp(Ref, <<"topic">>, 0, ?request_timed_out);
                    2 -> fake_rsp(Ref, <<"topic">>, 0)
                  end,
            Tester ! {'try', N},
            ProducerPid ! {msg, self(), Rsp},
            Loop(N + 1)
        end
    end,
  ConnPid = erlang:spawn_link(fun() -> ConnLoop(1) end),
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) ->
                  {ok, ConnPid}
              end),
  ProducerConfig = [{required_acks, 1},
                    {max_linger_ms, 0},
                    {retry_backoff_ms, 100}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  meck:expect(kpro, request_async,
              fun(Connection, Req) ->
                  Connection ! {produce, Producer, Req},
                  ok
              end),
  {ok, CallRef} = brod_producer:produce(Producer, <<"k">>, <<"v">>),
  ?WAIT({'try', 1}, ok, 1000),
  ?WAIT({'try', 2}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef,
                            result = brod_produce_req_acked},
        ok, 2000),
  ok = brod_producer:stop(Producer).

%% This is a typical connection restart scenario:
%% 0. Start producer allowing two requests on wire
%% 1. Send first request on wire, but no ack yet
%% 2. Reply {error, {connection_down, Reason}} for the second produce call
%% 3. Kill the mocked connection pid
%% Expect brod_producer to retry.
%% The retried produce request should have both messages in one message set
t_connection_down_retry({init, Config}) ->
  meck_module(brod_kafka_request),
  meck:expect(brod_kafka_request, produce,
              fun(_Vsn, <<"topic">>, 0, Batch, _RequiredAcks,
                  _AckTimeout, no_compression) ->
                  #kpro_req{msg = Batch}
              end),
  Config;
t_connection_down_retry({'end', Config}) ->
  meck:unload(brod_kafka_request),
  Config;
t_connection_down_retry(Config) when is_list(Config) ->
  Tester = self(),
  %% A mocked connection process which expects 2 requests to be sent
  %% black-hole the first request and succeed the second one
  ConnFun =
    fun L() ->
        receive
          {produce, ProducerPid, #kpro_req{ref = Ref, msg = Batch}} ->
            Tester ! {sent, Batch},
            case length(Batch) of
              1 ->
                ok;
              _ ->
                Rsp = fake_rsp(Ref, <<"topic">>, 0),
                ProducerPid ! {msg, self(), Rsp}
            end,
            L()
        end
    end,
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) ->
                  ConnPid = erlang:spawn(ConnFun),
                  Tester ! {connected, ConnPid},
                  {ok, ConnPid}
              end),
  ProducerConfig = [{required_acks, 1},
                    {max_linger_ms, 0},
                    {partition_onwire_limit, 2},
                    {retry_backoff_ms, 1000}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  WhichCall = fun() ->
                CallCount = case get(call_count) of
                              undefined -> 0;
                              N         -> N + 1
                            end,
                put(call_count, CallCount),
                CallCount
              end,
  meck:expect(kpro, request_async,
              fun(Connection, KafkaReq) ->
                  case WhichCall() of
                    0 ->
                      Connection ! {produce, Producer, KafkaReq},
                      ok;
                    1 ->
                      Tester ! {called, 1},
                      {error, {connection_down, test}};
                    2 ->
                      Connection ! {produce, Producer, KafkaReq},
                      ok
                  end
              end),
  {ok, CallRef1} = brod_producer:produce(Producer, <<"k1">>, <<"v1">>),
  ConnPid1 = ?WAIT({connected, Pid}, Pid, 1000),
  ?WAIT({sent, [#{key := <<"k1">>, value := <<"v1">>}]}, ok, 1000),
  {ok, CallRef2} = brod_producer:produce(Producer, <<"k2">>, <<"v2">>),
  ?WAIT({called, 1}, ok, 1000),
  erlang:exit(ConnPid1, kill),
  ?WAIT({connected, _}, ok, 3000),
  ?WAIT({sent, [#{key := <<"k1">>}, #{key := <<"k2">>}]}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef1,
                            result = brod_produce_req_acked}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef2,
                            result = brod_produce_req_acked}, ok, 1000),
  ok = brod_producer:stop(Producer).

%% leader migration for brod_producer means new connection pid
%% expect brod_producer to retry on the new connection
t_leader_migration({init, Config}) ->
  meck_module(brod_kafka_request),
  meck:expect(brod_kafka_request, produce,
              fun(_Vsn = 0, <<"topic">>, 0, Batch, _RequiredAcks,
                  _AckTimeout, no_compression) ->
                  #kpro_req{msg = Batch}
              end),
  Config;
t_leader_migration({'end', Config}) ->
  meck:unload(brod_kafka_request),
  Config;
t_leader_migration(Config) when is_list(Config) ->
  Tester = self(),
  ConnFun =
    fun L() ->
      receive
        {produce, ProducerPid, #kpro_req{ref = Ref, msg = Batch}} ->
          Tester ! {sent, Batch},
          Rsp =
            case length(Batch) of
              1 -> fake_rsp(Ref, <<"topic">>, 0, ?not_leader_for_partition);
              _ -> fake_rsp(Ref, <<"topic">>, 0)
            end,
          ProducerPid ! {msg, self(), Rsp},
          %% keep looping. i.e. do not exit as if the old leader is still alive
          L()
      end
    end,
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) ->
                  ConnPid =
                    case get(<<"which-leader">>) of
                      undefined ->
                        Pid = erlang:spawn_link(ConnFun),
                        put(<<"which-leader">>, Pid),
                        Pid;
                      _Pid ->
                        erlang:spawn_link(ConnFun)
                    end,
                  Tester ! {connected, ConnPid},
                  {ok, ConnPid}
              end),
  ProducerConfig = [{required_acks, 1},
                    {max_linger_ms, 0},
                    {partition_onwire_limit, 1},
                    {retry_backoff_ms, 1000}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  meck:expect(kpro, request_async,
              fun(Connection, KafkaReq) ->
                  Connection ! {produce, Producer, KafkaReq},
                  ok
              end),
  {ok, CallRef1} = brod_producer:produce(Producer, <<"k1">>, <<"v1">>),
  {ok, CallRef2} = brod_producer:produce(Producer, <<"k2">>, <<"v2">>),
  {ok, CallRef3} = brod_producer:produce(Producer, <<"k3">>, <<"v3">>),
  ConnPid1 = ?WAIT({connected, P}, P, 1000),
  ?WAIT({sent, [#{key := <<"k1">>}]}, ok, 1000),
  ConnPid2 = ?WAIT({connected, P}, P, 2000),
  ?assert(ConnPid1 =/= ConnPid2),
  ?WAIT({sent, [#{key := <<"k1">>},
                #{key := <<"k2">>},
                #{key := <<"k3">>}]}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef1,
                            result = brod_produce_req_acked}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef2,
                            result = brod_produce_req_acked}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef3,
                            result = brod_produce_req_acked}, ok, 1000),
  ok = brod_producer:stop(Producer).

%%%_* Help functions ===========================================================

meck_module(Module) ->
  meck:new(Module, [passthrough, no_passthrough_cover, no_history]).

fake_rsp(Ref, Topic, Partition) ->
  fake_rsp(Ref, Topic, Partition, ?no_error).

fake_rsp(Ref, Topic, Partition, ErrorCode) ->
  #kpro_rsp{ api = produce
           , vsn = 0
           , ref = Ref
           , msg = [{responses,
                     [[{topic, Topic}
                      ,{partition_responses,
                        [[{partition, Partition},
                          {error_code, ErrorCode},
                          {base_offset, -1}
                         ]
                        ]}
                      ]
                     ]}]
           }.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
