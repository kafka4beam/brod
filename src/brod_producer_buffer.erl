%%%
%%%   Copyright (c) 2015-2106, Klarna AB
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
%%% @copyright 2015-2016 Klarna AB
%%% @end
%%% ============================================================================

%% @private
-module(brod_producer_buffer).

-export([ new/5
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
-type data() :: fun(() -> {key(), value()}).

-record(req,
        { call_ref :: brod_call_ref()
        , data     :: data()
        , bytes    :: non_neg_integer()
        , failures :: non_neg_integer() %% the number of failed attempts
        }).

-type send_fun() :: fun((pid(), [{key(), value()}]) ->
                        ok |
                        {ok, corr_id()} |
                        {error, any()}).
-define(ERR_FUN, fun() -> erlang:error(bad_init) end).

-define(EMPTY_QUEUE, {[],[]}).

-record(buf,
        { buffer_limit   = 1            :: pos_integer()
        , onwire_limit   = 1            :: pos_integer()
        , max_batch_size = 1            :: pos_integer()
        , max_retries    = 0            :: integer()
        , send_fun       = ?ERR_FUN     :: send_fun()
        , buffer_count   = 0            :: non_neg_integer()
        , onwire_count   = 0            :: non_neg_integer()
        , pending        = ?EMPTY_QUEUE :: queue:queue(#req{})
        , buffer         = ?EMPTY_QUEUE :: queue:queue(#req{})
        , onwire         = []           :: [{corr_id(), [#req{}]}]
        }).

-opaque buf() :: #buf{}.

%%%_* APIs =====================================================================

%% @doc Create a new buffer
%% For more details: @see brod_producer:start_link/4
%% @end
-spec new(pos_integer(), pos_integer(),
          pos_integer(), integer(), send_fun()) -> buf().
new(BufferLimit, OnWireLimit, MaxBatchSize, MaxRetry, SendFun) ->
  true = (BufferLimit > 0), %% assert
  true = (OnWireLimit > 0), %% assert
  true = (MaxBatchSize > 0), %% assert
  ?EMPTY_QUEUE = queue:new(), %% assert
  #buf{ buffer_limit   = BufferLimit
      , onwire_limit   = OnWireLimit
      , max_batch_size = MaxBatchSize
      , max_retries    = MaxRetry
      , send_fun       = SendFun
      }.

%% @doc Buffer a produce request.
%% Respond to caller immediately if the buffer limit is not yet reached.
%% @end
-spec add(buf(), brod_call_ref(), key(), value()) -> {ok, buf()}.
add(#buf{pending = Pending} = Buf, CallRef, Key, Value) ->
  Req = #req{ call_ref = CallRef
            , data     = fun() -> {Key, Value} end
            , bytes    = data_size(Key) + data_size(Value)
            , failures = 0
            },
  maybe_buffer(Buf#buf{pending = queue:in(Req, Pending)}).

%% @doc Maybe (if there is any produce requests buffered) send the produce
%% request to kafka. In case a request has been tried for more than limited
%% times, and 'exit' exception is raised.
%% @end
-spec maybe_send(buf(), pid()) -> {ok, buf()} | {retry, buf()}.
maybe_send(#buf{} = Buf, SockPid) ->
  case take_reqs_to_send(Buf) of
    {[], NewBuf}   -> {ok, NewBuf};
    {Reqs, NewBuf} -> do_send(Reqs, NewBuf, SockPid)
  end.

%% @doc Reply 'acked' to callers.
-spec ack(buf(), corr_id()) -> {ok, buf()} | {error, ignored}.
ack(#buf{ onwire_count = OnWireCount
        , onwire       = [{CorrId, Reqs} | Rest]
        } = Buf, CorrId) ->
  ok = lists:foreach(fun reply_acked/1, Reqs),
  {ok, Buf#buf{ onwire_count = OnWireCount - 1
              , onwire       = Rest
              }};
ack(#buf{onwire = OnWire}, CorrIdReceived) ->
  %% unkonwn corr-id, ignore
  true = assert_corr_id(OnWire, CorrIdReceived),
  {error, ignored}.

%% @doc 'Negative' ack, put all sent requests back to the head of buffer.
%% An 'exit' exception is raised if any of the negative-acked requests
%% reached maximum retry limit.
%% Unknown corr-id:s are ignored.
%% @end
-spec nack(buf(), corr_id(), any()) -> {ok, buf()} | {error, ignored}.
nack(#buf{onwire = [{CorrId, _Reqs} | _]} = Buf, CorrId, Reason) ->
  nack_all(Buf, Reason);
nack(#buf{onwire = OnWire}, CorrIdReceived, _Reason) ->
  true = assert_corr_id(OnWire, CorrIdReceived),
  %% unknown corr-id, ignore.
  {error, ignored}.

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
is_empty(#buf{ pending = ?EMPTY_QUEUE
             , buffer  = ?EMPTY_QUEUE
             , onwire  = []
             }) -> true;
is_empty(#buf{}) -> false.

%%%_* Internal functions =======================================================

%% @private This is a validation on the received correlation IDs for produce
%% responses, the assumption made in brod implementation is that kafka broker
%% guarantees the produce responses are replied in the order the corresponding
%% produce requests were received from clients.
%% @end
-spec assert_corr_id([{corr_id(), [#req{}]}], corr_id()) -> true.
assert_corr_id(_OnWireRequests = [], _CorrIdReceived) ->
  true;
assert_corr_id([{CorrId, _Req} | _], CorrIdReceived) ->
  case is_later_corr_id(CorrId, CorrIdReceived) of
    true  -> exit({bad_order, CorrId, CorrIdReceived});
    false -> true
  end.

%% @private Compare two corr-ids, return true if ID-2 is considered a 'later'
%% one comparing to ID1.
%% Assuming that no clients would send up to 2^26 messages asynchronously.
%% @end
-spec is_later_corr_id(corr_id(), corr_id()) -> boolean().
is_later_corr_id(Id1, Id2) ->
  Diff = abs(Id1 - Id2),
  case Diff < (?MAX_CORR_ID div 2) of
    true  -> Id1 < Id2;
    false -> Id1 > Id2
  end.

%% @private
-spec take_reqs_to_send(buf()) -> {[#req{}], buf()}.
take_reqs_to_send(#buf{ onwire_count = OnWireCount
                      , onwire_limit = OnWireLimit
                      } = Buf) when OnWireCount >= OnWireLimit ->
  {[], Buf};
take_reqs_to_send(#buf{} = Buf) ->
  take_reqs_to_send(Buf, _Acc = [], _AccBytes = 0).

%% @private
-spec take_reqs_to_send(buf(), [#req{}], integer()) -> {[#req{}], buf()}.
take_reqs_to_send(#buf{ pending = ?EMPTY_QUEUE
                      , buffer  = ?EMPTY_QUEUE
                      } = Buf, Acc, _AccBytes) ->
  %% no more requests in buffer & pending
  {lists:reverse(Acc), Buf};
take_reqs_to_send(#buf{buffer = ?EMPTY_QUEUE} = Buf, Acc, AccBytes) ->
  %% no more requests in buffer, take more from pending
  {ok, NewBuf} = maybe_buffer(Buf),
  take_reqs_to_send(NewBuf, Acc, AccBytes);
take_reqs_to_send(#buf{max_batch_size = MaxBatchSize} = Buf, Acc, AccBytes)
  when AccBytes >= MaxBatchSize ->
  %% reached max bytes in one message set
  {ok, NewBuf} = maybe_buffer(Buf),
  {lists:reverse(Acc), NewBuf};
take_reqs_to_send(#buf{ buffer_count = BufferCount
                      , buffer       = Buffer
                      } = Buf, _Acc = [], _AccBytes = 0) ->
  %% always send at least one message one time regardless of size
  {{value, Req}, Rest} = queue:out(Buffer),
  NewBuf = Buf#buf{ buffer_count = BufferCount - 1
                  , buffer       = Rest
                  },
  take_reqs_to_send(NewBuf, [Req], Req#req.bytes);
take_reqs_to_send(#buf{ buffer_count = BufferCount
                      , buffer       = Buffer
                      } = Buf, Acc, AccBytes) ->
  {{value, Req}, Rest} = queue:out(Buffer),
  NewBuf = Buf#buf{ buffer_count = BufferCount - 1
                  , buffer       = Rest
                  },
  take_reqs_to_send(NewBuf, [Req | Acc], AccBytes + Req#req.bytes).

%% @private Send produce request to kafka.
-spec do_send([#req{}], buf(), pid()) -> {ok, buf()} | {retry, buf()}.
do_send(Reqs, #buf{ onwire_count = OnWireCount
                  , onwire       = OnWire
                  , send_fun     = SendFun
                  } = Buf, SockPid) ->
  MessageSet = lists:map(fun(#req{data = F}) -> F() end, Reqs),
  case SendFun(SockPid, MessageSet) of
    ok ->
      %% fire and forget
      ok = lists:foreach(fun reply_acked/1, Reqs),
      {ok, Buf};
    {ok, CorrId} ->
      {ok, Buf#buf{ onwire_count = OnWireCount + 1
                  , onwire       = OnWire ++ [{CorrId, Reqs}]
                  }};
    {error, Reason} ->
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
maybe_buffer(#buf{ pending      = ?EMPTY_QUEUE } = Buf) ->
  {ok, Buf};
maybe_buffer(#buf{ buffer_limit = BufferLimit
                 , buffer_count = BufferCount
                 , pending      = Pending
                 , buffer       = Buffer
                 } = Buf) when BufferCount < BufferLimit ->
  {{value, Req}, NewPending} = queue:out(Pending),
  ok = reply_buffered(Req),
  NewBuf = Buf#buf{ buffer_count = BufferCount + 1
                  , pending      = NewPending
                  , buffer       = queue:in(Req, Buffer)
                  },
  maybe_buffer(NewBuf);
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

-spec data_size(key() | value()) -> non_neg_integer().
data_size(Data) -> brod_utils:bytes(Data).

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
  {error, ignored} = ack(#buf{}, 0),
  {error, ignored} = nack(#buf{}, 0, ignored),
  {error, ignored} = ack(#buf{onwire = [{1, req}]}, 0),
  {error, ignored} = nack(#buf{onwire = [{1, req}]}, 0, ignored),
  {error, ignored} = ack(#buf{onwire = [{1, req}]}, ?MAX_CORR_ID),
  ?assertException(exit, {bad_order, 0, 1},
                   ack(#buf{onwire = [{0, req}]}, 1)),
  ?assertException(exit, {bad_order, ?MAX_CORR_ID, 0},
                   ack(#buf{onwire = [{?MAX_CORR_ID, req}]}, 0)),
  ok.

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
