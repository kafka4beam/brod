%%%
%%%   Copyright (c) 2014-2017, Klarna AB
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
%%% This module manages an opaque of sent-request collection.
%%% @end
%%% ============================================================================

%% @private
-module(brod_kafka_requests).

%%%_* Exports ==================================================================

%% API
-export([ new/0
        , add/2
        , del/2
        , get_caller/2
        , get_corr_id/1
        , increment_corr_id/1
        , scan_for_max_age/1
        ]).

-export_type([requests/0]).

-record(requests,
        { corr_id = 0
        , sent    = gb_trees:empty() :: gb_trees:tree()
        }).

-opaque requests() :: #requests{}.

-define(REQ(Caller, Ts), {Caller, Ts}).
-define(MAX_CORR_ID_WINDOW_SIZE, (?MAX_CORR_ID div 2)).

%%%_* Includes =================================================================
-include("brod_int.hrl").

%%%_* APIs =====================================================================

-spec new() -> requests().
new() -> #requests{}.

%% @doc Add a new request to sent collection.
%% Return the last corrlation ID and the new opaque.
%% @end
-spec add(requests(), pid()) -> {brod:corr_id(), requests()}.
add(#requests{ corr_id = CorrId
             , sent    = Sent
             } = Requests, Caller) ->
  NewSent = gb_trees:insert(CorrId, ?REQ(Caller, os:timestamp()), Sent),
  NewRequests = Requests#requests{ corr_id = kpro:next_corr_id(CorrId)
                                 , sent    = NewSent
                                 },
  {CorrId, NewRequests}.

%% @doc Delete a request from the opaque collection.
%% Crash if correlation ID is not found.
%% @end
-spec del(requests(), brod:corr_id()) -> requests().
del(#requests{sent = Sent} = Requests, CorrId) ->
  Requests#requests{sent = gb_trees:delete(CorrId, Sent)}.

%% @doc Get caller of a request having the given correlation ID.
%% Crash if the request is not found.
%% @end
-spec get_caller(requests(), brod:corr_id()) -> pid().
get_caller(#requests{sent = Sent}, CorrId) ->
  ?REQ(Caller, _Ts) = gb_trees:get(CorrId, Sent),
  Caller.

%% @doc Get the correction to be sent for the next request.
-spec get_corr_id(requests()) -> brod:corr_id().
get_corr_id(#requests{ corr_id = CorrId }) ->
  CorrId.

%% @doc Fetch and increment the correlation ID
%% This is used if we don't want a response from the broker
%% @end
-spec increment_corr_id(requests()) -> {brod:corr_id(), requests()}.
increment_corr_id(#requests{corr_id = CorrId} = Requests) ->
  {CorrId, Requests#requests{ corr_id = kpro:next_corr_id(CorrId) }}.

%% @doc Scan the gb_tree to get oldest sent request.
%% Age is in milli-seconds.
%% 0 is returned if there is no pending response.
%% @end
-spec scan_for_max_age(requests()) -> timeout().
scan_for_max_age(#requests{sent = Sent}) ->
  Now = os:timestamp(),
  MinTs = lists:foldl(fun({_, ?REQ(_Caller, Ts)}, Min) ->
                          min(Ts, Min)
                      end, Now, gb_trees:to_list(Sent)),
  timer:now_diff(Now, MinTs) div 1000.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
