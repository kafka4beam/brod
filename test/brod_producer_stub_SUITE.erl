%%%=============================================================================
%%% @doc
%%% @copyright 2017 Klarna AB
%%% @end
%%% ============================================================================

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
        , t_retry_on_same_socket/1
        , t_sock_down_retry/1
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
  meck_module(brod_sock),
  try
    ?MODULE:Case({'init', Config})
  catch
    error : function_clause ->
      Config
  end.

end_per_testcase(Case, Config) ->
  meck:unload(brod_client),
  meck:unload(brod_sock),
  _ = erase(corr_id),
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
              fun(client, <<"topic">>, 0) -> {ok, Tester} end),
  meck:expect(brod_sock, request_async,
              fun(Pid, KafkaReq) ->
                  CorrId = corr_id(),
                  Pid ! {request_async, CorrId, KafkaReq},
                  {ok, CorrId}
              end),
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0, []),
  {ok, CallRef} = brod_producer:produce(Producer, <<"key">>, <<"val">>),
  ?WAIT({request_async, CorrId, _KafkaReq},
        Producer ! {msg, Tester, fake_rsp(CorrId, <<"topic">>, 0)}, 2000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef,
                            result = brod_produce_req_acked},
        ok, 2000),
  ok = brod_producer:stop(Producer).

%% When required acks set to 0
%% no produce response will be received from kafka
%% but still caller should receive 'acked' reply
t_no_required_acks({init, Config}) ->
  meck_module(kpro),
  Config;
t_no_required_acks({'end', Config}) ->
  meck:unload(kpro),
  Config;
t_no_required_acks(Config) when is_list(Config) ->
  MaxLingerCount = 3,
  Tester = self(),
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) -> {ok, Tester} end),
  meck:expect(kpro, produce_request,
              fun(_Vsn = 0, <<"topic">>, 0, KvList, 0, _, no_compression) ->
                  case length(KvList) =:= MaxLingerCount of
                    true ->
                      ?assertEqual([{<<"1">>, <<"1">>},
                                    {<<"2">>, <<"2">>},
                                    {<<"3">>, <<"3">>}
                                   ], KvList);
                    false ->
                      ?assertEqual([{<<"4">>, <<"4">>}], KvList)
                  end,
                  <<"fake-produce-request">>
              end),
  meck:expect(brod_sock, request_async,
              fun(_Pid, <<"fake-produce-request">>) ->
                  %% mocking brod_sock's behaviour when
                  %% required-acks is 0
                  ok
              end),
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

t_retry_on_same_socket(Config) when is_list(Config) ->
  Tester = self(),
  %% A mocked socket process which expects 2 requests to be sent
  %% reply a retriable error code for the first one
  %% and succeed the second one
  SockLoop =
    fun Loop(N) ->
        receive
          {produce, CorrId, ProducerPid, <<"the req">>} ->
            Rsp = case N of
                    1 -> fake_rsp(CorrId, <<"topic">>, 0,
                                  ?EC_REQUEST_TIMED_OUT);
                    2 -> fake_rsp(CorrId, <<"topic">>, 0)
                  end,
            Tester ! {'try', N},
            ProducerPid ! {msg, self(), Rsp},
            Loop(N + 1)
        end
    end,
  SockPid = erlang:spawn_link(fun() -> SockLoop(1) end),
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) ->
                  {ok, SockPid}
              end),
  ProducerConfig = [{required_acks, 1},
                    {max_linger_ms, 0},
                    {retry_backoff_ms, 100}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  meck:expect(brod_sock, request_async,
              fun(SockPid_, _KafkaReq) ->
                  CorrId = corr_id(),
                  SockPid_ ! {produce, CorrId, Producer, <<"the req">>},
                  {ok, CorrId}
              end),
  {ok, CallRef} = brod_producer:produce(Producer, <<"k">>, <<"v">>),
  ?WAIT({'try', 1}, ok, 1000),
  ?WAIT({'try', 2}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef,
                            result = brod_produce_req_acked},
        ok, 2000),
  ok = brod_producer:stop(Producer).

%% This is a tipical socket restart scenario:
%% 0. Allow two requests on wire
%% 1. Send first request on wire, but no ack yet
%% 2. Reply {error, {sock_down, reason}} for the second produce call
%% 3. Kill the mocked socket pid
%% Expect brod_producer to retry.
%% The retried produce request should have both messages in one message set
t_sock_down_retry({init, Config}) ->
  meck_module(kpro),
  meck:expect(kpro, produce_request,
              fun(_Vsn = 0, <<"topic">>, 0, KvList, _RequiredAcks,
                  _AckTimeout, no_compression) ->
                  {<<"fake-req">>, KvList}
              end),
  Config;
t_sock_down_retry({'end', Config}) ->
  meck:unload(kpro),
  Config;
t_sock_down_retry(Config) when is_list(Config) ->
  Tester = self(),
  %% A mocked socket process which expects 2 requests to be sent
  %% black-hole the first request and succeed the second one
  SockFun =
    fun L() ->
        receive
          {produce, CorrId, ProducerPid, {<<"fake-req">>, KvList}} ->
            Tester ! {sent, CorrId, KvList},
            case CorrId of
              0 ->
                ok;
              2 ->
                Rsp = fake_rsp(CorrId, <<"topic">>, 0),
                ProducerPid ! {msg, self(), Rsp};
              X ->
                %% this should pollute Tester's message queue
                %% and cause test to fail
                Tester ! {<<"unexpected corr_id">>, X}
            end,
            L()
        end
    end,
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) ->
                  SockPid = erlang:spawn(SockFun),
                  Tester ! {connected, SockPid},
                  {ok, SockPid}
              end),
  ProducerConfig = [{required_acks, 1},
                    {max_linger_ms, 0},
                    {partition_onwire_limit, 2},
                    {retry_backoff_ms, 1000}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  meck:expect(brod_sock, request_async,
              fun(SockPid_, KafkaReq) ->
                  case corr_id() of
                    0 ->
                      SockPid_ ! {produce, 0, Producer, KafkaReq},
                      {ok, 0};
                    1 ->
                      Tester ! {called, 1},
                      {error, {sock_down, test}};
                    2 ->
                      SockPid_ ! {produce, 2, Producer, KafkaReq},
                      {ok, 2}
                  end
              end),
  {ok, CallRef1} = brod_producer:produce(Producer, <<"k1">>, <<"v1">>),
  SockPid1 = ?WAIT({connected, Pid}, Pid, 1000),
  ?WAIT({sent, 0, [{<<"k1">>, <<"v1">>}]}, ok, 1000),
  {ok, CallRef2} = brod_producer:produce(Producer, <<"k2">>, <<"v2">>),
  ?WAIT({called, 1}, ok, 1000),
  erlang:exit(SockPid1, kill),
  ?WAIT({connected, _}, ok, 3000),
  ?WAIT({sent, 2, [{<<"k1">>, _}, {<<"k2">>, _}]}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef1,
                            result = brod_produce_req_acked}, ok, 1000),
  ?WAIT(#brod_produce_reply{call_ref = CallRef2,
                            result = brod_produce_req_acked}, ok, 1000),
  ok = brod_producer:stop(Producer).

%% leader migration for brod_producer means new socket pid
%% expect brod_producer to retry on the new socket
t_leader_migration({init, Config}) ->
  meck_module(kpro),
  meck:expect(kpro, produce_request,
              fun(_Vsn = 0, <<"topic">>, 0, KvList, _RequiredAcks,
                  _AckTimeout, no_compression) ->
                  {<<"fake-req">>, KvList}
              end),
  Config;
t_leader_migration({'end', Config}) ->
  meck:unload(kpro),
  Config;
t_leader_migration(Config) when is_list(Config) ->
  Tester = self(),
  SockFunFun =
    fun(ExpectedCorrId) ->
      fun L() ->
        receive
          {produce, CorrId, ProducerPid, {<<"fake-req">>, KvList}} ->
            ?assertEqual(ExpectedCorrId, CorrId),
            Tester ! {sent, CorrId, KvList},
            Rsp = case CorrId of
                    0 -> fake_rsp(CorrId, <<"topic">>, 0,
                                  ?EC_NOT_LEADER_FOR_PARTITION);
                    1 -> fake_rsp(CorrId, <<"topic">>, 0)
                  end,
            ProducerPid ! {msg, self(), Rsp},
            %% keep looping.
            %% i.e. do not exit as if the old leader is still alive
            L()
        end
      end
    end,
  meck:expect(brod_client, get_leader_connection,
              fun(client, <<"topic">>, 0) ->
                  SockPid =
                    case get(<<"which-leader">>) of
                      undefined ->
                        Pid = erlang:spawn_link(SockFunFun(0)),
                        put(<<"which-leader">>, Pid),
                        Pid;
                      _Pid ->
                        erlang:spawn_link(SockFunFun(1))
                    end,
                  Tester ! {connected, SockPid},
                  {ok, SockPid}
              end),
  ProducerConfig = [{required_acks, 1},
                    {max_linger_ms, 0},
                    {partition_onwire_limit, 1},
                    {retry_backoff_ms, 1000}],
  {ok, Producer} = brod_producer:start_link(client, <<"topic">>, 0,
                                            ProducerConfig),
  meck:expect(brod_sock, request_async,
              fun(SockPid_, KafkaReq) ->
                  CorrId = corr_id(),
                  SockPid_ ! {produce, CorrId, Producer, KafkaReq},
                  {ok, CorrId}
              end),
  {ok, CallRef1} = brod_producer:produce(Producer, <<"k1">>, <<"v1">>),
  {ok, CallRef2} = brod_producer:produce(Producer, <<"k2">>, <<"v2">>),
  {ok, CallRef3} = brod_producer:produce(Producer, <<"k3">>, <<"v3">>),
  SockPid1 = ?WAIT({connected, P}, P, 1000),
  ?WAIT({sent, 0, [{<<"k1">>, _}]}, ok, 1000),
  SockPid2 = ?WAIT({connected, P}, P, 2000),
  ?assert(SockPid1 =/= SockPid2),
  ?WAIT({sent, 1, [{<<"k1">>, _}, {<<"k2">>, _}, {<<"k3">>, _}]}, ok, 1000),
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

corr_id() ->
  CorrId = case get(corr_id) of
             undefined -> 0;
             N         -> N + 1
           end,
  put(corr_id, CorrId),
  CorrId.

fake_rsp(CorrId, Topic, Partition) ->
  fake_rsp(CorrId, Topic, Partition, ?EC_NONE).

fake_rsp(CorrId, Topic, Partition, ErrorCode) ->
  #kpro_rsp{ tag = produce_response
           , vsn = 0
           , corr_id = CorrId
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
