%%%
%%%   Copyright (c) 2015-2021, Klarna Bank AB (publ)
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
        , add/3
        , ack/2
        , ack/3
        , nack/3
        , nack_all/2
        , maybe_send/3
        , empty_buffers/1
        ]).

-export([ is_empty/1
        ]).

-export_type([buf/0]).

-include("brod_int.hrl").

-type batch() :: [{brod:key(), brod:value()}].
-type milli_ts() :: pos_integer().
-type milli_sec() :: non_neg_integer().
-type count() :: non_neg_integer().
-type buf_cb_arg() :: ?buffered | {?acked, offset()}.
-type buf_cb() :: fun((buf_cb_arg()) -> ok) |
                  {fun((any(), buf_cb_arg()) -> ok), any()}.

-record(req,
        { buf_cb   :: buf_cb()
        , data     :: batch()
        , bytes    :: non_neg_integer()
        , msg_cnt  :: non_neg_integer() %% messages in the set
        , ctime    :: milli_ts() %% time when request was created
        , failures :: non_neg_integer() %% the number of failed attempts
        }).
-type req() :: #req{}.

-type vsn() :: brod_kafka_apis:vsn().
-type send_fun_res() :: ok |
                        {ok, reference()} |
                        {error, any()}.
-type send_fun() :: fun((pid(), batch(), vsn()) -> send_fun_res()) |
                    {fun((any(), pid(), batch(), vsn()) -> send_fun_res()), any()}.
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
        , pending           = ?NEW_QUEUE :: queue:queue(req())
        , buffer            = ?NEW_QUEUE :: queue:queue(req())
        , onwire            = []         :: [{reference(), [req()]}]
        }).

-opaque buf() :: #buf{}.

-type offset()  :: brod:offset().

%%%_* APIs =====================================================================

%% @doc Create a new buffer
%% For more details: @see brod_producer:start_link/4
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
-spec add(buf(), buf_cb(), brod:batch_input()) -> buf().
add(#buf{pending = Pending} = Buf, BufCb, Batch) ->
  Req = #req{ buf_cb   = BufCb
            , data     = Batch
            , bytes    = data_size(Batch)
            , msg_cnt  = length(Batch)
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
-spec maybe_send(buf(), pid(), vsn()) -> {Action, buf()}
        when Action :: ok | retry | {delay, milli_sec()}.
maybe_send(#buf{} = Buf, Conn, Vsn) ->
  case take_reqs(Buf) of
    false          -> {ok, Buf};
    {delay, T}     -> {{delay, T}, Buf};
    {Reqs, NewBuf} -> do_send(Reqs, NewBuf, Conn, Vsn)
  end.

%% @doc Reply 'acked' to callers.
-spec ack(buf(), reference()) -> buf().
ack(Buf, Ref) ->
  ack(Buf, Ref, ?BROD_PRODUCE_UNKNOWN_OFFSET).

%% @doc Reply 'acked' with base offset to callers.
-spec ack(buf(), reference(), offset()) -> buf().
ack(#buf{ onwire_count = OnWireCount
        , onwire       = [{Ref, Reqs} | Rest]
        } = Buf, Ref, BaseOffset) ->
  _ = lists:foldl(fun eval_acked/2, BaseOffset, Reqs),
  Buf#buf{ onwire_count = OnWireCount - 1
         , onwire       = Rest
         }.

%% @doc 'Negative' ack, put all sent requests back to the head of buffer.
%% An 'exit' exception is raised if any of the negative-acked requests
%% reached maximum retry limit.
-spec nack(buf(), reference(), any()) -> buf().
nack(#buf{onwire = [{Ref, _Reqs} | _]} = Buf, Ref, Reason) ->
  nack_all(Buf, Reason).

%% @doc 'Negative' ack, put all sent requests back to the head of buffer.
%% An 'exit' exception is raised if any of the negative-acked requests
%% reached maximum retry limit.
-spec nack_all(buf(), any()) -> buf().
nack_all(#buf{onwire = OnWire} = Buf, Reason) ->
  AllOnWireReqs = lists:flatmap(fun({_Ref, Reqs}) -> Reqs end, OnWire),
  NewBuf = Buf#buf{ onwire_count = 0
                  , onwire       = []
                  },
  rebuffer_or_crash(AllOnWireReqs, NewBuf, Reason).

%% @doc Return true if there is no message pending,
%% buffered or waiting for ack.
-spec is_empty(buf()) -> boolean().
is_empty(#buf{ pending = Pending
             , buffer  = Buffer
             , onwire  = Onwire
             }) ->
  queue:is_empty(Pending) andalso
  queue:is_empty(Buffer) andalso
  Onwire =:= [].

%%%_* Internal functions =======================================================

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
      NewBuf = maybe_buffer(Buf),
      do_take_reqs(NewBuf)
  end.

-spec do_take_reqs(buf()) -> {delay, milli_sec()} | {[req()], buf()}.
do_take_reqs(#buf{ max_linger_count = MaxLingerCount
                 , buffer_count     = BufferCount
                 } = Buf) when BufferCount >= MaxLingerCount ->
  %% there is already enough messages lingering around
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
      NewBuf = maybe_buffer(Buf),
      %% and continue to accumulate the batch
      take_reqs_loop_2(NewBuf, Acc, AccBytes)
  end;
take_reqs_loop(Buf, Acc, AccBytes) ->
  take_reqs_loop_2(Buf, Acc, AccBytes).

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
      NewBuf = maybe_buffer(Buf),
      {lists:reverse(Acc), NewBuf};
    false ->
      {_, Rest} = queue:out(Buffer),
      NewBuf = Buf#buf{ buffer_count = BufferCount - 1
                      , buffer       = Rest
                      },
      take_reqs_loop(NewBuf, [Req | Acc], BatchSize)
  end.

%% Send produce request to kafka.
-spec do_send([req()], buf(), pid(), brod_kafka_apis:vsn()) -> {Action, buf()}
        when Action :: ok | retry | {delay, milli_sec()}.
do_send(Reqs, #buf{ onwire_count = OnWireCount
                  , onwire       = OnWire
                  , send_fun     = SendFun
                  } = Buf, Conn, Vsn) ->
  Batch = lists:flatmap(fun(#req{data = Data}) -> Data end, Reqs),
  case apply_sendfun(SendFun, Conn, Batch, Vsn) of
    ok ->
      %% fire and forget, do not add onwire counter
      ok = lists:foreach(fun eval_acked/1, Reqs),
      %% continue to try next batch
      maybe_send(Buf, Conn, Vsn);
    {ok, Ref} ->
      %% Keep onwire message reference to match acks later on
      NewBuf = Buf#buf{ onwire_count = OnWireCount + 1
                      , onwire       = OnWire ++ [{Ref, Reqs}]
                      },
      %% continue try next batch
      maybe_send(NewBuf, Conn, Vsn);
    {error, Reason} ->
      %% The requests sent on-wire are not re-buffered here
      %% because there are still chances to receive acks for them.
      %% brod_producer should call nack_all to put all sent requests
      %% back to buffer for retry in any of the cases below:
      %% 1. Connection pid monitoring 'DOWN' message is received
      %% 2. Discovered a new leader (a new connection pid)
      NewBuf = rebuffer_or_crash(Reqs, Buf, Reason),
      {retry, NewBuf}
  end.

apply_sendfun({SendFun, ExtraArg}, Conn, Batch, Vsn) ->
  SendFun(ExtraArg, Conn, Batch, Vsn);
apply_sendfun(SendFun, Conn, Batch, Vsn) ->
  SendFun(Conn, Batch, Vsn).

%% Put the produce requests back to buffer.
%% raise an 'exit' exception if the first request to send has reached
%% retry limit
-spec rebuffer_or_crash([req()], buf(), any()) -> buf() | no_return().
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

%% Take pending requests into buffer and reply 'buffered' to caller.
-spec maybe_buffer(buf()) -> buf().
maybe_buffer(#buf{ buffer_limit = BufferLimit
                 , buffer_count = BufferCount
                 , pending      = Pending
                 , buffer       = Buffer
                 } = Buf) when BufferCount < BufferLimit ->
  case queue:out(Pending) of
    {{value, Req}, NewPending} ->
      ok = eval_buffered(Req),
      NewBuf = Buf#buf{ buffer_count = BufferCount + 1
                      , pending      = NewPending
                      , buffer       = queue:in(Req, Buffer)
                      },
      maybe_buffer(NewBuf);
    {empty, _} ->
      Buf
  end;
maybe_buffer(#buf{} = Buf) ->
  Buf.

-spec eval_buffered(req()) -> ok.
eval_buffered(#req{buf_cb = BufCb}) ->
  _ = apply_bufcb(BufCb, ?buffered),
  ok.

-spec eval_acked(req()) -> ok.
eval_acked(Req) ->
  _ = eval_acked(Req, ?BROD_PRODUCE_UNKNOWN_OFFSET),
  ok.

-spec eval_acked(req(), offset()) -> offset().
eval_acked(#req{buf_cb = BufCb, msg_cnt = Count}, Offset) ->
  _ = apply_bufcb(BufCb, {?acked, Offset}),
  next_base_offset(Offset, Count).

apply_bufcb({BufCb, ExtraArg}, Arg) ->
  BufCb(ExtraArg, Arg);
apply_bufcb(BufCb, Arg) ->
  BufCb(Arg).

next_base_offset(?BROD_PRODUCE_UNKNOWN_OFFSET, _) ->
  ?BROD_PRODUCE_UNKNOWN_OFFSET;
next_base_offset(Offset, Count) ->
  Offset + Count.

-spec data_size(brod:batch_input()) -> non_neg_integer().
data_size(Data) -> brod_utils:bytes(Data).

-spec now_ms() -> milli_ts().
now_ms() ->
  {M, S, Micro} = os:timestamp(),
  ((M * 1000000) + S) * 1000 + Micro div 1000.

-spec empty_buffers(buf()) -> buf().
empty_buffers(Buffer = #buf{}) ->
  %% return a buffer without the data in the queues but maintaining the counters
  Buffer#buf{pending=?NEW_QUEUE, buffer=?NEW_QUEUE, onwire=[]}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
