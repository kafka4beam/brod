%%%
%%%   Copyright (c) 2015-2017, Klarna AB
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
-module(brod_producer_buffer).

-export([ new/7
        , add/4
        , ack/2
        , nack/3
        , nack_all/2
        , maybe_send/2
        ]).

-export([ is_empty/1
        ]).

-export_type([buf/0]).

-include("brod_int.hrl").

%% keep data in fun() to avoid huge log dumps in case of crash etc.
-type data() :: fun(() -> {brod:key(), brod:value()}).
-type milli_ts() :: pos_integer().
-type milli_sec() :: non_neg_integer().
-type count() :: non_neg_integer().

-record(req,
        { call_ref :: brod:call_ref()
        , data     :: data()
        , bytes    :: non_neg_integer()
        , ctime    :: milli_ts() %% time when request was created
        , failures :: non_neg_integer() %% the number of failed attempts
        }).
-type req() :: #req{}.

-type send_fun() :: fun((pid(), [{brod:key(), brod:value()}]) ->
                        ok |
                        {ok, brod:corr_id()} |
                        {error, any()}).
-define(ERR_FUN, fun() -> erlang:error(bad_init) end).

-define(NEW_QUEUE, queue:new()).

-record(buf,
        { buffer_limit      = 1          :: pos_integer()
        , onwire_limit      = 1          :: pos_integer()
        , max_batch_size    = 1          :: pos_integer()
        , max_retries       = 0          :: integer()
        , max_linger_ms     = 0          :: milli_sec()
        , max_linger_count  = 0          :: count()
        , send_fun          = ?ERR_FUN   :: send_fun()
        , buffer_count      = 0          :: non_neg_integer()
        , onwire_count      = 0          :: non_neg_integer()
        , pending           = ?NEW_QUEUE :: queue:queue(#req{})
        , buffer            = ?NEW_QUEUE :: queue:queue(#req{})
        , onwire            = []         :: [{brod:corr_id(), [#req{}]}]
        }).

-opaque buf() :: #buf{}.

-type corr_id() :: brod:corr_id().

%%%_* APIs =====================================================================

%% @doc Create a new buffer
%% For more details: @see brod_producer:start_link/4
%% @end
-spec new(pos_integer(), pos_integer(), pos_integer(),
          integer(), milli_sec(), count(), send_fun()) -> buf().
new(BufferLimit, OnWireLimit, MaxBatchSize, MaxRetry,
    MaxLingerMs, MaxLingerCount0, SendFun) ->
  true = (BufferLimit > 0), %% assert
  true = (OnWireLimit > 0), %% assert
  true = (MaxBatchSize > 0), %% assert
  true = (MaxLingerCount0 >= 0), %% assert
  %% makes no sense to allow lingering messages more than buffer limit
  MaxLingerCount = erlang:min(BufferLimit, MaxLingerCount0),
  #buf{ buffer_limit     = BufferLimit
      , onwire_limit     = OnWireLimit
      , max_batch_size   = MaxBatchSize
      , max_retries      = MaxRetry
      , max_linger_ms    = MaxLingerMs
      , max_linger_count = MaxLingerCount
      , send_fun         = SendFun
      }.

%% @doc Buffer a produce request.
%% Respond to caller immediately if the buffer limit is not yet reached.
%% @end
-spec add(buf(), brod:call_ref(), brod:key(), brod:value()) -> {ok, buf()}.
add(#buf{pending = Pending} = Buf, CallRef, Key, Value) ->
  Req = #req{ call_ref = CallRef
            , data     = fun() -> {Key, Value} end
            , bytes    = data_size(Key) + data_size(Value)
            , ctime    = now_ms()
            , failures = 0
            },
  maybe_buffer(Buf#buf{pending = queue:in(Req, Pending)}).

%% @doc Maybe (if there is any produce requests buffered) send the produce
%% request to kafka. In case a request has been tried for more maximum allowed
%% times an 'exit' exception is raised.
%% Return `{Res, NewBuffer}', where `Res' can be:
%% ok:
%%   There is no more message left to send, or allowed to send due to
%%   onwire limit, caller should wait for the next event to trigger a new
%%   `maybe_send' call.  Such event can either be a new produce request or
%%   a produce response from kafka.
%% {delay, Time}:
%%   Caller should retry after the returned milli-seconds.
%% retry:
%%   Failed to send a batch, caller should schedule a delayed retry.
%% @end
-spec maybe_send(buf(), pid()) -> {Action, buf()}
        when Action :: ok | retry | {delay, milli_sec()}.
maybe_send(#buf{} = Buf, SockPid) ->
  case take_reqs(Buf) of
    false          -> {ok, Buf};
    {delay, T}     -> {{delay, T}, Buf};
    {Reqs, NewBuf} -> do_send(Reqs, NewBuf, SockPid)
  end.

%% @doc Reply 'acked' to callers.
-spec ack(buf(), corr_id()) -> {ok, buf()} | {error, none | corr_id()}.
ack(#buf{ onwire_count = OnWireCount
        , onwire       = [{CorrId, Reqs} | Rest]
        } = Buf, CorrId) ->
  ok = lists:foreach(fun reply_acked/1, Reqs),
  {ok, Buf#buf{ onwire_count = OnWireCount - 1
              , onwire       = Rest
              }};
ack(#buf{onwire = OnWire}, CorrIdReceived) ->
  %% unkonwn corr-id, ignore
  CorrIdExpected = assert_corr_id(OnWire, CorrIdReceived),
  {error, CorrIdExpected}.

%% @doc 'Negative' ack, put all sent requests back to the head of buffer.
%% An 'exit' exception is raised if any of the negative-acked requests
%% reached maximum retry limit.
%% Unknown correlation IDs are discarded.
%% @end
-spec nack(buf(), corr_id(), any()) -> {ok, buf()} | {error, none | corr_id()}.
nack(#buf{onwire = [{CorrId, _Reqs} | _]} = Buf, CorrId, Reason) ->
  nack_all(Buf, Reason);
nack(#buf{onwire = OnWire}, CorrIdReceived, _Reason) ->
  CorrIdExpected = assert_corr_id(OnWire, CorrIdReceived),
  {error, CorrIdExpected}.

%% @doc 'Negative' ack, put all sent requests back to the head of buffer.
%% An 'exit' exception is raised if any of the negative-acked requests
%% reached maximum retry limit.
%% @end
-spec nack_all(buf(), any()) -> {ok, buf()}.
nack_all(#buf{onwire = OnWire} = Buf, Reason) ->
  AllOnWireReqs = lists:map(fun({_CorrId, Reqs}) -> Reqs end, OnWire),
  NewBuf = Buf#buf{ onwire_count = 0
                  , onwire       = []
                  },
  {ok, rebuffer_or_crash(lists:append(AllOnWireReqs), NewBuf, Reason)}.

%% @doc Return true if there is no message pending,
%% buffered or waiting for ack.
%% @end
-spec is_empty(buf()) -> boolean().
is_empty(#buf{ pending = Pending
             , buffer  = Buffer
             , onwire  = Onwire
             }) ->
  queue:is_empty(Pending) andalso
  queue:is_empty(Buffer) andalso
  Onwire =:= [].

%%%_* Internal functions =======================================================

%% @private This is a validation on the received correlation IDs for produce
%% responses, the assumption made in brod implementation is that kafka broker
%% guarantees the produce responses are replied in the order the corresponding
%% produce requests were received from clients.
%% Return expected correlation ID, or otherwise raise an 'exit' exception.
%% @end
-spec assert_corr_id([{corr_id(), [#req{}]}], corr_id()) -> none | corr_id().
assert_corr_id(_OnWireRequests = [], _CorrIdReceived) ->
  none;
assert_corr_id([{CorrId, _Req} | _], CorrIdReceived) ->
  case is_later_corr_id(CorrId, CorrIdReceived) of
    true  -> exit({bad_order, CorrId, CorrIdReceived});
    false -> CorrId
  end.

%% @private Compare two corr-ids, return true if ID-2 is considered a 'later'
%% one comparing to ID1.
%% Assuming that no clients would send up to 2^26 messages asynchronously.
%% @end
-spec is_later_corr_id(corr_id(), corr_id()) -> boolean().
is_later_corr_id(Id1, Id2) ->
  Diff = abs(Id1 - Id2),
  case Diff < (kpro:max_corr_id() div 2) of
    true  -> Id1 < Id2;
    false -> Id1 > Id2
  end.

%% @private
-spec take_reqs(buf()) -> false | {delay, milli_sec()} | {[req()], buf()}.
take_reqs(#buf{ onwire_count = OnWireCount
              , onwire_limit = OnWireLimit
              }) when OnWireCount >= OnWireLimit ->
  %% too many sent on wire
  false;
take_reqs(#buf{ buffer = Buffer, pending = Pending} = Buf) ->
  case queue:is_empty(Buffer) andalso queue:is_empty(Pending) of
    true ->
      %% no buffer AND no pending
      false;
    false ->
      %% ensure buffer is not empty before calling do_take_reqs/1
      {ok, NewBuf} = maybe_buffer(Buf),
      do_take_reqs(NewBuf)
  end.

%% @private
-spec do_take_reqs(buf()) -> {delay, milli_sec()} | {[req()], buf()}.
do_take_reqs(#buf{ max_linger_count = MaxLingerCount
                 , buffer_count     = BufferCount
                 } = Buf) when BufferCount >= MaxLingerCount ->
  %% there is alredy enough messages lingering around
  take_reqs_loop(Buf, _Acc = [], _AccBytes = 0);
do_take_reqs(#buf{ max_linger_ms = MaxLingerMs
                 , buffer        = Buffer
                 } = Buf) ->
  %% buffer should not be empty
  %% ensured by take_reqs/1
  {value, Req} = queue:peek(Buffer),
  Age = now_ms() - Req#req.ctime,
  case Age < MaxLingerMs of
    true ->
      %% the buffer is still too young
      {delay, MaxLingerMs - Age};
    false ->
      take_reqs_loop(Buf, _Acc = [], _AccBytes = 0)
  end.

%% @private
-spec take_reqs_loop(buf(), [req()], integer()) -> {[req()], buf()}.
take_reqs_loop(#buf{ buffer_count = 0
                   , pending = Pending
                   } = Buf, Acc, AccBytes) ->
  %% no more requests in buffer
  case queue:is_empty(Pending) of
    true ->
      %% no more requests in pending either
      [_ | _] = Acc, %% assert not empty
      {lists:reverse(Acc), Buf};
    false ->
      %% Take requests from pending to buffer
      {ok, NewBuf} = maybe_buffer(Buf),
      %% and continue to accumulate the batch
      take_reqs_loop_2(NewBuf, Acc, AccBytes)
  end;
take_reqs_loop(Buf, Acc, AccBytes) ->
  take_reqs_loop_2(Buf, Acc, AccBytes).

%% @private
-spec take_reqs_loop_2(buf(), [req()], non_neg_integer()) -> {[req()], buf()}.
take_reqs_loop_2(#buf{ buffer_count   = BufferCount
                     , buffer         = Buffer
                     , max_batch_size = MaxBatchSize
                     } = Buf, Acc, AccBytes) ->
  {value, Req} = queue:peek(Buffer),
  BatchSize = AccBytes + Req#req.bytes,
  %% Always take at least one message to send regardless of its size
  %% Otherwise try not to exceed the max batch size limit.
  case Acc =/= [] andalso BatchSize > MaxBatchSize of
    true ->
      %% finished accumulating the batch
      %% take more pending ones into buffer
      {ok, NewBuf} = maybe_buffer(Buf),
      {lists:reverse(Acc), NewBuf};
    false ->
      {_, Rest} = queue:out(Buffer),
      NewBuf = Buf#buf{ buffer_count = BufferCount - 1
                      , buffer       = Rest
                      },
      take_reqs_loop(NewBuf, [Req | Acc], BatchSize)
  end.

%% @private Send produce request to kafka.
-spec do_send([req()], buf(), pid()) -> {Action, buf()}
        when Action :: ok | retry | {delay, milli_sec()}.
do_send(Reqs, #buf{ onwire_count = OnWireCount
                  , onwire       = OnWire
                  , send_fun     = SendFun
                  } = Buf, SockPid) ->
  MessageSet = lists:map(fun(#req{data = F}) -> F() end, Reqs),
  case SendFun(SockPid, MessageSet) of
    ok ->
      %% fire and forget, do not add onwire counter
      ok = lists:foreach(fun reply_acked/1, Reqs),
      %% continue to try next batch
      maybe_send(Buf, SockPid);
    {ok, CorrId} ->
      %% Keep onwire message reference to match acks later on
      NewBuf = Buf#buf{ onwire_count = OnWireCount + 1
                      , onwire       = OnWire ++ [{CorrId, Reqs}]
                      },
      %% continue try next batch
      maybe_send(NewBuf, SockPid);
    {error, Reason} ->
      %% The requests sent on-wire are not re-buffered here
      %% because there are still chances to receive acks for them.
      %% brod_producer should call nack_all to put all sent requests
      %% back to buffer for retry in any of the cases below:
      %% 1. Socket pid monitoring 'DOWN' message is received
      %% 2. Discovered a new leader (a new socket pid)
      NewBuf = rebuffer_or_crash(Reqs, Buf, Reason),
      {retry, NewBuf}
  end.

%% @private Put the produce requests back to buffer.
%% raise an 'exit' exception if the first request to send has reached
%% retry limit
%% @end
-spec rebuffer_or_crash([#req{}], buf(), any()) -> buf() | no_return().
rebuffer_or_crash([#req{failures = Failures} | _],
                  #buf{max_retries = MaxRetries}, Reason)
  when MaxRetries >= 0 andalso Failures >= MaxRetries ->
  exit({reached_max_retries, Reason});
rebuffer_or_crash(Reqs0, #buf{ buffer       = Buffer
                             , buffer_count = BufferCount
                             } = Buf, _Reason) ->
  Reqs = lists:map(fun(#req{failures = Failures} = Req) ->
                        Req#req{failures = Failures + 1}
                   end, Reqs0),
  NewBuffer = lists:foldr(fun (Req, AccBuffer) ->
                                queue:in_r(Req, AccBuffer)
                          end, Buffer, Reqs),
  Buf#buf{ buffer       = NewBuffer
         , buffer_count = length(Reqs) + BufferCount
         }.

%% @private Take pending requests into buffer and reply 'buffered' to caller.
-spec maybe_buffer(buf()) -> {ok, buf()}.
maybe_buffer(#buf{ buffer_limit = BufferLimit
                 , buffer_count = BufferCount
                 , pending      = Pending
                 , buffer       = Buffer
                 } = Buf) when BufferCount < BufferLimit ->
  case queue:out(Pending) of
    {{value, Req}, NewPending} ->
      ok = reply_buffered(Req),
      NewBuf = Buf#buf{ buffer_count = BufferCount + 1
                      , pending      = NewPending
                      , buffer       = queue:in(Req, Buffer)
                      },
      maybe_buffer(NewBuf);
    {empty, _} ->
      {ok, Buf}
  end;
maybe_buffer(#buf{} = Buf) ->
  {ok, Buf}.

-spec reply_buffered(#req{}) -> ok.
reply_buffered(#req{call_ref = CallRef}) ->
  Reply = #brod_produce_reply{ call_ref = CallRef
                             , result   = brod_produce_req_buffered
                             },
  cast(CallRef#brod_call_ref.caller, Reply).

-spec reply_acked(#req{}) -> ok.
reply_acked(#req{call_ref = CallRef}) ->
  Reply = #brod_produce_reply{ call_ref = CallRef
                             , result   = brod_produce_req_acked
                             },
  cast(CallRef#brod_call_ref.caller, Reply).

cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

-spec data_size(brod:key() | brod:value()) -> non_neg_integer().
data_size(Data) -> brod_utils:bytes(Data).

%% @private
-spec now_ms() -> milli_ts().
now_ms() ->
  {M, S, Micro} = os:timestamp(),
  ((M * 1000000) + S) * 1000 + Micro div 1000.

%%%_* Tests ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

cast_test() ->
  Ref = make_ref(),
  ok = cast(self(), Ref),
  receive Ref -> ok
  end,
  ok = cast(?undef, Ref).

assert_corr_id_test() ->
  Max = kpro:max_corr_id(),
  {error, none} = ack(#buf{}, 0),
  {error, none} = nack(#buf{}, 0, ignored),
  {error, 1} = ack(#buf{onwire = [{1, req}]}, 0),
  {error, 1} = nack(#buf{onwire = [{1, req}]}, 0, ignored),
  {error, 1} = ack(#buf{onwire = [{1, req}]}, Max),
  ?assertException(exit, {bad_order, 0, 1},
                   ack(#buf{onwire = [{0, req}]}, 1)),
  ?assertException(exit, {bad_order, Max, 0},
                   ack(#buf{onwire = [{Max, req}]}, 0)),
  ok.

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
