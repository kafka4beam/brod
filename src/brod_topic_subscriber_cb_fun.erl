%%%
%%%   Copyright (c) 2020-2021 Klarna Bank AB (publ)
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
%%% @private
%%% A wrapper module that enables backward compatible use of
%%% `brod_topic_subscriber' with a fun instead of a callback module.
%%% @end
%%%=============================================================================
-module(brod_topic_subscriber_cb_fun).

-behavior(brod_topic_subscriber).

-export([init/2, handle_message/3]).

-include("brod_int.hrl").

%% @private This is needed to implement backward-consistent `cb_fun'
%% interface.
init(_Topic, #cbm_init_data{ committed_offsets = CommittedOffsets
                           , cb_fun            = CbFun
                           , cb_data           = CbState
                           }) ->
  {ok, CommittedOffsets, {CbFun, CbState}}.

handle_message(Partition, Msg, {CbFun, CbState0}) ->
  case CbFun(Partition, Msg, CbState0) of
    {ok, ack, CbState} ->
      {ok, ack, {CbFun, CbState}};
    {ok, CbState} ->
      {ok, {CbFun, CbState}};
    Err ->
      Err
  end.
