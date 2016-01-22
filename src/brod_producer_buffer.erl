%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
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
-module(brod_producer_buffer).

-export([new/4]).
-export([maybe_send/4]).
-export([ack/2]).
-export([is_empty/1]).

-export_type([buf/0]).

-include("brod_int.hrl").

%% keep data in fun() to avoid huge log dumps in case of crash etc.
-type data() :: fun(() -> kafka_kv()).
-type call() :: brod_call_ref().

-record(req,
        { call :: call()
        , data :: data()
        }).

-type send_fun() :: fun(([{binary(), binary()}]) -> {ok, corr_id()}).
-define(ERR_FUN, fun() -> erlang:error(bad_init) end).

-record(buf,
        { buffer_limit = 1        :: pos_integer()
        , onwire_limit = 1        :: pos_integer()
        , msgset_bytes = 1        :: pos_integer()
        , send_fun     = ?ERR_FUN :: send_fun()
        , buffer_count = 0        :: non_neg_integer()
        , onwire_count = 0        :: non_neg_integer()
        , pending      = []       :: [#req{}]
        , buffer       = []       :: [#req{}]
        , onwire       = []       :: [{corr_id(), [call()]}]
        }).

-opaque buf() :: #buf{}.

%%%_* APIs =====================================================================

%% @doc Create a new buffer
%% For more detail: @see brod_producer:start_link/4
%% @end
-spec new(pos_integer(), pos_integer(), pos_integer(), send_fun()) -> buf().
new(BufferLimit, OnWireLimit, MsgSetBytes, SendFun) ->
  true = (BufferLimit > 0), %% assert
  true = (OnWireLimit > 0), %% assert
  true = (MsgSetBytes > 0), %% assert
  #buf{ buffer_limit = BufferLimit
      , onwire_limit = OnWireLimit
      , msgset_bytes = MsgSetBytes
      , send_fun     = SendFun
      }.

%% @doc Append a new produce request to pending list.
%% buffer immediately if the buffer limit is not yet reached.
%% send to socket immediately if onwire_limit is not yet reached.
%% @end
-spec maybe_send(buf(), call(), binary(), binary()) -> buf().
maybe_send(#buf{pending = Pending} = Buf, CallRef, Key, Value) ->
  Req = #req{ call = CallRef
            , data = fun() -> {Key, Value} end
            },
  NewBuf = Buf#buf{pending = Pending ++ [Req]},
  do_maybe_send(maybe_buffer(NewBuf)).

%% @doc Reply 'acked' to callers.
ack(#buf{ onwire_count = OnWireCount
        , onwire       = [{CorrId, CallRefs} | Rest]
        } = Buf, CorrIdReceived) ->
  CorrId = CorrIdReceived, %% assert
  ok = lists:foreach(fun reply_acked/1, CallRefs),
  NewBuf = Buf#buf{ onwire_count = OnWireCount - length(CallRefs)
                  , onwire       = Rest
                  },
  do_maybe_send(NewBuf).

%% @doc Return true if there is no message pending, buffered or waiting for ack.
is_empty(#buf{ pending = []
             , buffer  = []
             , onwire  = []
             }) -> true;
is_empty(#buf{}) -> false.

%%%_* Internal functions =======================================================

%% @private Maybe send a message set on wire.
-spec do_maybe_send(buf()) -> buf().
do_maybe_send(#buf{ onwire_limit = OnWireLimit
                  , send_fun     = SendFun
                  , onwire_count = OnWireCount
                  , onwire       = OnWire
                  } = Buf) when OnWireCount < OnWireLimit ->
  case take_reqs_to_send(Buf) of
    {Reqs, NewBuf} when Reqs =/= [] ->
      CallRefs   = lists:map(fun(#req{call = C}) -> C   end, Reqs),
      MessageSet = lists:map(fun(#req{data = F}) -> F() end, Reqs),
      case SendFun(MessageSet) of
        ok ->
          %% fire and forget
          ok = lists:foreach(fun reply_acked/1, CallRefs),
          NewBuf;
        {ok, CorrId} ->
          NewBuf#buf{ onwire_count = OnWireCount + length(CallRefs)
                    , onwire       = OnWire ++ [{CorrId, CallRefs}]
                    }
      end;
    {[], NewBuf} ->
      NewBuf
  end;
do_maybe_send(Buf) -> Buf.

take_reqs_to_send(Buf) ->
  take_reqs_to_send(Buf, _Acc = [], _AccLength = 0, _AccBytes = 0).

take_reqs_to_send(#buf{ pending = []
                      , buffer  = []
                      } = Buf, Acc, _AccLength, _AccBytes) ->
  %% no more requests in buffer&pending
  {lists:reverse(Acc), Buf};
take_reqs_to_send(#buf{buffer = []} = Buf, Acc, AccLength, AccBytes) ->
  %% no more requests in buffer, take one from pending
  take_reqs_to_send(maybe_buffer(Buf), Acc, AccLength, AccBytes);
take_reqs_to_send(#buf{ msgset_bytes = MsgSetBytes
                      , onwire_limit = OnWireLimit
                      } = Buf, Acc, AccLength, AccBytes)
 when AccLength >= OnWireLimit orelse AccBytes >= MsgSetBytes ->
  %% reached max number of requests on wire
  %% or reached max bytes in one message set
  {lists:reverse(Acc), Buf};
take_reqs_to_send(#buf{ msgset_bytes = MsgSetBytes
                      , buffer_count = BufferCount
                      , buffer       = [Req | Rest]
                      } = Buf, Acc, AccLength, AccBytes) ->
  ReqBytes = req_bytes(Req),
  case AccBytes =:= 0 orelse AccBytes + ReqBytes =< MsgSetBytes of
    true ->
      NewBuf = Buf#buf{ buffer_count = BufferCount - 1
                      , buffer       = Rest
                      },
      take_reqs_to_send(NewBuf, [Req | Acc], AccLength+1, AccBytes+ReqBytes);
    false ->
      {lists:reverse(Acc), Buf}
  end.

req_bytes(#req{data = F}) ->
  {Key, Value} = F(),
  size(Key) + size(Value).

%% @private Take pending requests into buffer and reply 'buffered' to caller.
-spec maybe_buffer(buf()) -> buf().
maybe_buffer(#buf{ buffer_limit = BufferLimit
                 , buffer_count = BufferCount
                 , pending      = [Req | Rest]
                 , buffer       = Buffer
                 } = Buf) when BufferCount < BufferLimit ->
  #req{call = Call} = Req,
  ok = reply_buffered(Call),
  NewBuf = Buf#buf{ buffer_count = BufferCount + 1
                  , pending      = Rest
                  , buffer       = Buffer ++ [Req]
                  },
  maybe_buffer(NewBuf);
maybe_buffer(Buf) -> Buf.

-spec reply_buffered(brod_call_ref()) -> ok.
reply_buffered(#brod_call_ref{caller = Pid} = CallRef) ->
  Reply = #brod_produce_reply{ call_ref = CallRef
                             , result   = brod_produce_req_buffered
                             },
  cast(Pid, Reply).

-spec reply_acked(brod_call_ref()) -> ok.
reply_acked(#brod_call_ref{caller = Pid} = CallRef) ->
  Reply = #brod_produce_reply{ call_ref = CallRef
                             , result   = brod_produce_req_acked
                             },
  cast(Pid, Reply).

cast(Pid, Msg) ->
  try
    Pid ! Msg,
    ok
  catch _ : _ ->
    ok
  end.

%%%_* Tests ====================================================================

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

cast_test() ->
  Ref = make_ref(),
  ok = cast(self(), Ref),
  receive Ref -> ok
  end,
  ok = cast(?undef, Ref).

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
