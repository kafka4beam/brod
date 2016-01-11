%%%
%%%   Copyright (c) 2014-2016, Klarna AB
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
%%% @copyright 2016 Klarna AB
%%% @end
%%%=============================================================================
-module(brod_consumer_impl).

-include_lib("brod/include/brod.hrl").

-export([ init_consumer/3
        , handle_messages/5
        , handle_error/4
        ]).

-record(state, { subscriber :: pid() | atom() }).

init_consumer(_Topic, _Partition, Subscriber) ->
  {ok, #state{subscriber = Subscriber}, []}.

handle_messages(Topic, Partition, HighWmOffset, Messages, State) ->
  %% we are in a separate process spawned by brod_consumer
  Ref = erlang:make_ref(),
  Subscriber = State#state.subscriber,
  Msg = #message_set{ topic = Topic
                    , partition = Partition
                    , high_wm_offset = HighWmOffset
                    , messages = Messages},
  Subscriber ! {Ref, self(), Msg},
  receive
    {Ref, Subscriber, ack} ->
      {ok, State};
    {Ref, Subscriber, ack, NewOptions} ->
      {ok, State, NewOptions}
  end.

handle_error(Topic, Partition, Error, State) ->
  Ref = erlang:make_ref(),
  Subscriber = State#state.subscriber,
  {ErrorCode, ErrorDesc} = Error,
  Msg = #brod_fetch_error{ topic = Topic
                         , partition = Partition
                         , error_code = ErrorCode
                         , error_desc = ErrorDesc},
  Subscriber ! {Ref, self(), Msg},
  receive
    {Ref, Subscriber, ack} ->
      {ok, State};
    {Ref, Subscriber, ack, NewOptions} ->
      {ok, State, NewOptions}
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
