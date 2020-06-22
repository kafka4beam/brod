%%%
%%%   Copyright (c) 2015-2018, Klarna Bank AB (publ)
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
-module(brod_topic_subscriber_SUITE).


%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , common_init_per_testcase/2
        , common_end_per_testcase/2
        , suite/0
        ]).

%% brod subscriber callbacks
-export([ init/2
        , terminate/2
        , handle_message/3
        ]).

%% Test cases
-export([ t_async_acks/1
        , t_demo/1
        , t_demo_message_set/1
        , t_consumer_crash/1
        , t_callback_crash/1
        , t_begin_offset/1
        , t_cb_fun/1
        ]).

-include("brod_test_setup.hrl").
-include_lib("snabbkaffe/include/ct_boilerplate.hrl").
-include("brod.hrl").

-define(CLIENT_ID, ?MODULE).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
  kafka_test_helper:init_per_suite(Config).

end_per_suite(_Config) -> ok.

common_init_per_testcase(Case, Config0) ->
  ok = brod_demo_topic_subscriber:delete_commit_history(?topic(Case, 1)),
  Config = kafka_test_helper:common_init_per_testcase(?MODULE, Case, Config0),
  ok = brod:start_client(bootstrap_hosts(), ?CLIENT_ID, client_config()),
  Config.

common_end_per_testcase(Case, Config) ->
  brod:stop_client(?CLIENT_ID),
  kafka_test_helper:common_end_per_testcase(Case, Config).

%%%_* Topic subscriber callbacks ===============================================

-record(state,
        { is_async_ack :: boolean()
        , counter      :: integer()
        , worker_id    :: reference()
        }).

init(_Topic, {IsAsyncAck, CommittedOffsets}) ->
  Ref = make_ref(),
  ?tp(topic_subscriber_init,
      #{ worker_id => Ref
       , state     => 0
       }),
  State = #state{ is_async_ack = IsAsyncAck
                , counter      = 1
                , worker_id    = Ref
                },
  {ok, CommittedOffsets, State}.

handle_message(Partition, Message, #state{ is_async_ack = IsAsyncAck
                                         , counter      = Counter
                                         , worker_id    = Ref
                                         } = State0) ->
  #kafka_message{ offset = Offset
                , value  = Value
                } = Message,
  ?tp(topic_subscriber_seen_message,
     #{ partition => Partition
      , offset    => Offset
      , value     => Value
      , state     => Counter
      , worker_id => Ref
      }),
  State = State0#state{counter = Counter + 1},
  case IsAsyncAck of
    true  -> {ok, State};
    false -> {ok, ack, State}
  end.

terminate(Reason, #state{worker_id = Ref, counter = Counter}) ->
  ?tp(topic_subscriber_terminate,
      #{ worker_id => Ref
       , state     => Counter
       , reason    => Reason
       }).

%%%_* Test functions ===========================================================

t_demo(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_topic_subscriber:bootstrap(1, message),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_demo_message_set(Config) when is_list(Config) ->
  {Pid, Mref} =
    erlang:spawn_monitor(
      fun() ->
        brod_demo_topic_subscriber:bootstrap(1, message_set),
        receive
          _ ->
            ok
        end
      end),
  receive
    {'DOWN', Mref, process, Pid, Reason} ->
      erlang:error({demo_crashed, Reason})
  after 10000 ->
    exit(Pid, shutdown),
    ok
  end.

t_async_acks(Config) when is_list(Config) ->
  %% use consumer managed offset commit behaviour
  %% so we can control where to start fetching messages from
  MaxSeqNo       = 100,
  ConsumerConfig = [ {prefetch_count, MaxSeqNo}
                   , {prefetch_bytes, 0} %% as discard
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   ],
  ?check_trace(
     %% Run stage:
     begin
       O0 = produce({?topic, 0}, <<0>>),
       InitArgs = {true, [{0, O0}]},
       {ok, SubscriberPid} =
         brod:start_link_topic_subscriber(?CLIENT_ID, ?topic, ConsumerConfig,
                                          ?MODULE, InitArgs),
       %% Send messages:
       Messages = [<<I>> || I <- lists:seq(1, MaxSeqNo)],
       [produce({?topic, 0}, I) || I <- Messages],
       %% Ack messages:
       wait_and_ack(SubscriberPid, Messages),
       ok = brod_topic_subscriber:stop(SubscriberPid),
       Messages
     end,
     %% Check stage:
     fun(Expected, Trace) ->
         check_received_messages(Expected, Trace),
         check_state_continuity(Trace),
         check_init_terminate(Trace)
     end).

t_begin_offset(Config) when is_list(Config) ->
  ConsumerConfig = [ {prefetch_count, 100}
                   , {prefetch_bytes, 0} %% as discard
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   ],
  SendFun =
    fun(I) ->
      produce({?topic, 0}, <<I>>)
    end,
  ?check_trace(
     %% Run stage:
     begin
       _Offset0 = SendFun(1),
       Offset1 = SendFun(2),
       Offset2 = SendFun(3),
       %% Start as if committed Offset1, expect it to start fetching from
       %% Offset2
       InitArgs = {_IsAsyncAck = true,
                   _ConsumerOffsets = [{0, Offset1}]},
       {ok, SubscriberPid} =
         brod:start_link_topic_subscriber(?CLIENT_ID, ?topic, ConsumerConfig,
                                          ?MODULE, InitArgs),
       Expected = [<<3>>],
       wait_and_ack(SubscriberPid, Expected),
       ok = brod_topic_subscriber:stop(SubscriberPid),
       Expected
     end,
     %% Check stage:
     fun(Expected, Trace) ->
         check_received_messages(Expected, Trace),
         check_state_continuity(Trace),
         check_init_terminate(Trace)
     end).

t_consumer_crash(Config) when is_list(Config) ->
  ConsumerConfig = [ {prefetch_count, 10}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   , {partition_restart_delay_seconds, 1}
                   ],
  Partition = 0,
  SendFun =
    fun(I) ->
        produce({?topic, Partition}, <<I>>)
    end,
  ?check_trace(
     %% Run stage:
     begin
       O0 = SendFun(0),
       InitArgs = {true, [{0, O0}]},
       {ok, SubscriberPid} =
         brod:start_link_topic_subscriber(?CLIENT_ID, ?topic, ConsumerConfig,
                                          ?MODULE, InitArgs),
       %% send some messages, ack some of them:
       [SendFun(I) || I <- lists:seq(1, 5)],
       {ok, #{offset := O3}} = wait_message(<<3>>),
       {ok, #{offset := O5}} = wait_message(<<5>>),
       ok = brod_topic_subscriber:ack(SubscriberPid, Partition, O3),
       %% do a sync request to the subscriber, so that we know it has
       %% processed the ack, then kill the brod_consumer process
       sys:get_state(SubscriberPid),
       {ok, ConsumerPid} = brod:get_consumer(?CLIENT_ID, ?topic, Partition),
       kafka_test_helper:kill_process(ConsumerPid),
       %% ack all previously received messages
       %% so topic subscriber can re-subscribe to the restarted consumer
       ok = brod_topic_subscriber:ack(SubscriberPid, Partition, O5),
       %% send more messages and wait until they are processed:
       [SendFun(I) || I <- lists:seq(6, 8)],
       {ok, _} = wait_message(<<8>>),
       %% stop the subscriber:
       ok = brod_topic_subscriber:stop(SubscriberPid)
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         Expected = [<<I>> || I <- lists:seq(1, 8)],
         check_received_messages(Expected, Trace),
         check_state_continuity(Trace),
         check_init_terminate(Trace)
     end).

t_callback_crash(Config) when is_list(Config) ->
  %% Test that terminate callback is called after handle_message callback
  %% throws an exception:
  ?check_trace(
     %% Run stage:
     begin
       O0 = produce({?topic, 0}, <<0>>),
       {ok, SubscriberPid} =
         brod:start_link_topic_subscriber(
           #{ client        => ?CLIENT_ID
            , topic         => ?topic
            , message_type  => message
            , init_data     => {false, [{0, O0}]}
            , cb_module     => ?MODULE
            }),
       MRef = monitor(process, SubscriberPid),
       unlink(SubscriberPid),
       ?inject_crash( #{value := <<2>>, ?snk_kind := topic_subscriber_seen_message}
                    , snabbkaffe_nemesis:always_crash()
                    ),
       %% Send messages:
       Messages = [<<I>> || I <- lists:seq(1, 3)],
       [produce({?topic, 0}, I) || I <- Messages],
       receive
         {'DOWN', MRef, process, SubscriberPid, _} -> ok
       after
         10000 -> error(would_not_die)
       end
     end,
     %% Check stage:
     fun(_Ret, Trace) ->
         check_init_terminate(Trace)
     end).

%% Test the old way of consuming messages via callback fun:
t_cb_fun(Config) when is_list(Config) ->
  N = 10,
  Ref = make_ref(),
  ConsumerConfig = [ {prefetch_count, 10}
                   , {sleep_timeout, 0}
                   , {max_wait_time, 1000}
                   , {partition_restart_delay_seconds, 1}
                   ],
  CbFun = fun(Partition, Msg, State) ->
              #kafka_message{ offset = Offset
                            , value  = Value
                            } = Msg,
              ?tp(topic_subscriber_seen_message,
                  #{ partition => Partition
                   , offset    => Offset
                   , value     => Value
                   , state     => State
                   , worker_id => Ref
                   }),
              {ok, ack, State + 1}
          end,
  InitialState = 0,
  ?check_trace(
     %% Run stage:
     begin
       %% Produce messages:
       Messages = [<<I>> || I <- lists:seq(0, N)],
       [_, _, O3|_] = [produce({?topic, 0}, I) || I <- Messages],
       %% Start subscriber from offset O+1 and wait until it's done:
       {ok, SubscriberPid} =
         brod_topic_subscriber:start_link(?CLIENT_ID, ?topic, [0],
                                          ConsumerConfig, [{0, O3}], message,
                                          CbFun, InitialState),
       ?assertMatch({ok, _}, wait_message(<<N>>)),
       brod_topic_subscriber:stop(SubscriberPid),
       Messages
     end,
     %% Check stage:
     fun(Messages, Trace) ->
         %% Drop messages with offsets =< O3:
         [_, _, _|Expected] = Messages,
         check_received_messages(Expected, Trace),
         check_state_continuity(Trace)
     end).

%%%_* Internal functions  ======================================================

wait_message(Content) ->
  wait_message(Content, 30000).

wait_message(Content, Timeout) ->
  ?block_until(#{ ?snk_kind := topic_subscriber_seen_message
                , value := Content
                }, Timeout).

%% Wait for multiple messages and ack them as they are received:
wait_and_ack(SubscriberPid, Messages) ->
  [begin
     {ok, #{offset := Offset}} = wait_message(I),
     ok = brod_topic_subscriber:ack(SubscriberPid, 0, Offset)
   end || I <- Messages].

%% Check that messages processed by the tested process are exactly the
%% same as expected:
check_received_messages(Expected, Trace) ->
  Messages = ?of_kind(topic_subscriber_seen_message, Trace),
  %% Check that all messages have been seen once:
  ?assertEqual( Expected
              , ?projection(value, Messages)
              ).

%% Check that state of callback module is correctly passed around
%% between the calls:
check_state_continuity(WorkerId, Trace) ->
  snabbkaffe:strictly_increasing([S || #{ worker_id := WorkerId
                                        , state := S
                                        } <- Trace]).

check_state_continuity(Trace) ->
  %% Find IDs of all worker processes:
  Workers = lists:usort([ID || #{ worker_id := ID
                                , state := _
                                } <- Trace]),
  [check_state_continuity(I, Trace) || I <- Workers],
  true.

%% Check that for any `init' there is a `terminate':
check_init_terminate(Trace) ->
  ?strict_causality( #{worker_id := _ID, ?snk_kind := topic_subscriber_init}
                   , #{worker_id := _ID, ?snk_kind := topic_subscriber_terminate}
                   , Trace
                   ).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
