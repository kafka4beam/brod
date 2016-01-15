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
        , nack/2
        , maybe_send/4
        ]).

-export_type([buf/0]).

-include("brod_int.hrl").

%% keep data in fun() to avoid huge log dumps in case of crash etc.
-type data() :: fun(() -> kafka_kv()).

-record(req,
        { call_ref :: brod_call_ref()
        , data     :: data()
        , bytes    :: non_neg_integer()
        }).

-record(buf,
        { buffer_limit   = 1        :: pos_integer()
        , onwire_limit   = 1        :: pos_integer()
        , max_batch_size = 1        :: pos_integer()
        , required_acks  = -1       :: required_acks()
        , ack_timeout    = 1000     :: pos_integer()
        , buffer_count   = 0        :: non_neg_integer()
        , onwire_count   = 0        :: non_neg_integer()
        , pending        = []       :: [#req{}]
        , buffer         = []       :: [#req{}]
        , onwire         = []       :: [{corr_id(), [brod_call_ref()]}]
        }).

-opaque buf() :: #buf{}.

%%%_* APIs =====================================================================

%% @doc Create a new buffer
%% For more details: @see brod_producer:start_link/4
%% @end
-spec new(pos_integer(), pos_integer(), pos_integer(),
          required_acks(), pos_integer()) -> buf().
new(BufferLimit, OnWireLimit, MaxBatchSize, RequiredAcks, AckTimeout) ->
  true = (BufferLimit > 0), %% assert
  true = (OnWireLimit > 0), %% assert
  true = (MaxBatchSize > 0), %% assert
  true = (RequiredAcks =:= -1 orelse
         RequiredAcks =:= 0 orelse
         RequiredAcks =:= 1), %% assert
  true = (AckTimeout > 0), %% assert
  #buf{ buffer_limit   = BufferLimit
      , onwire_limit   = OnWireLimit
      , max_batch_size = MaxBatchSize
      , required_acks  = RequiredAcks
      , ack_timeout    = AckTimeout
      }.

%% @doc Buffer a produce request.
%% Respond to caller immediately if the buffer limit is not yet reached.
%% @end
-spec add(buf(), brod_call_ref(), binary(), binary()) -> {ok, buf()}.
add(#buf{pending = Pending} = Buf, CallRef, Key, Value) ->
  Req = #req{ call_ref = CallRef
            , data     = fun() -> {Key, Value} end
            , bytes    = size(Key) + size(Value)
            },
  maybe_buffer(Buf#buf{pending = Pending ++ [Req]}).

-spec maybe_send(buf(), pid(), topic(), partition()) ->
                    {ok, buf()} | {error, any()}.
maybe_send(#buf{ onwire_limit = OnWireLimit
               , onwire_count = OnWireCount
               , onwire       = OnWire
               } = Buf, SockPid, Topic, Partition)
  when OnWireCount < OnWireLimit ->
  case take_reqs_to_send(Buf) of
    {[], NewBuf} ->
      {ok, NewBuf};
    {Reqs, NewBuf} ->
      MessageSet = lists:map(fun(#req{data = F}) -> F() end, Reqs),
      Data = [{Topic, [{Partition, MessageSet}]}],
      KafkaReq = #produce_request{ acks    = Buf#buf.required_acks
                                 , timeout = Buf#buf.ack_timeout
                                 , data    = Data
                                 },
      case brod_sock:send(SockPid, KafkaReq) of
        ok ->
          %% fire and forget
          ok = lists:foreach(fun reply_acked/1, Reqs),
          NewBuf;
        {ok, CorrId} ->
          {ok, NewBuf#buf{ onwire_count = OnWireCount + length(Reqs)
                         , onwire       = OnWire ++ [{CorrId, Reqs}]
                         }};
        {error, Reason} ->
          {error, Reason}
      end
  end;
maybe_send(Buf, _SockPid, _Topic, _Partition) ->
  {ok, Buf}.

%% @doc Reply 'acked' to callers.
ack(#buf{ onwire_count = OnWireCount
        , onwire       = [{CorrId, Reqs} | Rest]
        } = Buf, CorrIdReceived) ->
  CorrId = CorrIdReceived, %% assert
  ok = lists:foreach(fun reply_acked/1, Reqs),
  {ok, Buf#buf{ onwire_count = OnWireCount - length(Reqs)
              , onwire       = Rest
              }}.

%% @doc 'Negative' ack, put 'onwire' requests back to the head of buffer
nack(#buf{ onwire_count = OnWireCount
         , onwire       = [{CorrId, Reqs} | Rest]
         , buffer       = Buffer
         } = Buf, CorrIdReceived) ->
  CorrId = CorrIdReceived, %% assert
  {ok, Buf#buf{ onwire_count = OnWireCount - length(Reqs)
              , onwire       = Rest
              , buffer       = Reqs ++ Buffer
              }}.

%%%_* Internal functions =======================================================
take_reqs_to_send(Buf) ->
  take_reqs_to_send(Buf, _Acc = [], _AccLength = 0, _AccBytes = 0).

%% @private
take_reqs_to_send(#buf{ pending = []
                      , buffer  = []
                      } = Buf, Acc, _AccLength, _AccBytes) ->
  %% no more requests in buffer&pending
  {lists:reverse(Acc), Buf};
take_reqs_to_send(#buf{buffer = []} = Buf, Acc, AccLength, AccBytes) ->
  %% no more requests in buffer, take one from pending
  {ok, NewBuf} = maybe_buffer(Buf),
  take_reqs_to_send(NewBuf, Acc, AccLength, AccBytes);
take_reqs_to_send(#buf{ max_batch_size = MaxBatchSize
                      , onwire_limit = OnWireLimit
                      } = Buf, Acc, AccLength, AccBytes)
 when AccLength >= OnWireLimit orelse AccBytes >= MaxBatchSize ->
  %% reached max number of requests on wire
  %% or reached max bytes in one message set
  {lists:reverse(Acc), Buf};
take_reqs_to_send(#buf{ max_batch_size = MaxBatchSize
                      , buffer_count = BufferCount
                      , buffer       = [Req | Rest]
                      } = Buf, Acc, AccLength, AccBytes) ->
  ReqBytes = Req#req.bytes,
  case AccBytes =:= 0 orelse AccBytes + ReqBytes =< MaxBatchSize of
    true ->
      NewBuf = Buf#buf{ buffer_count = BufferCount - 1
                      , buffer       = Rest
                      },
      take_reqs_to_send(NewBuf, [Req | Acc], AccLength+1, AccBytes+ReqBytes);
    false ->
      {lists:reverse(Acc), Buf}
  end.

%% @private Take pending requests into buffer and reply 'buffered' to caller.
-spec maybe_buffer(buf()) -> {ok, buf()}.
maybe_buffer(#buf{ buffer_limit = BufferLimit
                 , buffer_count = BufferCount
                 , pending      = [Req | Rest]
                 , buffer       = Buffer
                 } = Buf) when BufferCount < BufferLimit ->
  ok = reply_buffered(Req),
  NewBuf = Buf#buf{ buffer_count = BufferCount + 1
                  , pending      = Rest
                  , buffer       = Buffer ++ [Req]
                  },
  maybe_buffer(NewBuf);
maybe_buffer(Buf) ->
  {ok, Buf}.

-spec reply_buffered(#req{}) -> ok.
reply_buffered(#req{call_ref = CallRef}) ->
  Reply = #brod_produce_reply{ call_ref = CallRef
                             , result   = brod_produce_req_buffered
                             },
  safe_send(CallRef#brod_call_ref.caller, Reply).

-spec reply_acked(#req{}) -> ok.
reply_acked(#req{call_ref = CallRef}) ->
  Reply = #brod_produce_reply{ call_ref = CallRef
                             , result   = brod_produce_req_acked
                             },
  safe_send(CallRef#brod_call_ref.caller, Reply).

safe_send(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

safe_send_test() ->
  Ref = make_ref(),
  ok = safe_send(self(), Ref),
  receive Ref -> ok
  end,
  ok = safe_send(?undef, Ref).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
