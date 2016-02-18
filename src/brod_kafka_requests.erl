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
%%% This module manages an opaque of sent-request collection.
%%%
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%% ============================================================================

%%%_* Module declaration =======================================================
%% @private
-module(brod_kafka_requests).

%%%_* Exports ==================================================================

%% API
-export([ new/0
        , add/3
        , del/2
        , get_api_key/2
        , get_caller/2
        , get_corr_id/1
        ]).

-export_type([requests/0]).

-record(requests,
        { corr_id = 0
        , sent    = gb_trees:empty() :: gb_trees:tree()
        }).

-opaque requests() :: #requests{}.

%%%_* Includes =================================================================
-include("brod_int.hrl").
-include("brod_kafka.hrl").

%%%_* APIs =====================================================================

new() -> #requests{}.

%% @doc Add a new request to sent collection.
%% Return the last corrlation ID and the new opaque.
%% @end
-spec add(requests(), pid(), api_key()) -> {corr_id(), requests()}.
add(#requests{ corr_id = CorrId
             , sent    = Sent
             } = Requests, Caller, ApiKey) ->
  NewSent = gb_trees:insert(CorrId, {Caller, ApiKey}, Sent),
  NewRequests = Requests#requests{ corr_id = next_corr_id(CorrId)
                                 , sent    = NewSent
                                 },
  {CorrId, NewRequests}.

%% @doc Delete a request from the opaque collection.
%% Crash if correlation ID is not found.
%% @end
-spec del(requests(), corr_id()) -> requests().
del(#requests{sent = Sent} = Requests, CorrId) ->
  Requests#requests{sent = gb_trees:delete(CorrId, Sent)}.

%% @doc Get api_key() a request having the given correlation ID.
%% Crash if the request is not found.
%% @end
-spec get_api_key(requests(), corr_id()) -> api_key().
get_api_key(#requests{sent = Sent}, CorrId) ->
  {_Caller, ApiKey} = gb_trees:get(CorrId, Sent),
  ApiKey.

%% @doc Get caller of a request having the given correlation ID.
%% Crash if the request is not found.
%% @end
-spec get_caller(requests(), corr_id()) -> pid().
get_caller(#requests{sent = Sent}, CorrId) ->
  {Caller, _ApiKey} = gb_trees:get(CorrId, Sent),
  Caller.


%% @doc Get the correction to be sent for the next request.
-spec get_corr_id(requests()) -> corr_id().
get_corr_id(#requests{ corr_id = CorrId }) ->
  CorrId.

%%%_* Internal functions =======================================================

next_corr_id(?MAX_CORR_ID) -> 0;
next_corr_id(CorrId)       -> CorrId + 1.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

next_corr_id_test() ->
  CorrId = (1 bsl 31) - 1,
  Req = #requests{corr_id = CorrId},
  {CorrId, NewReq} = add(Req, self(), api_key),
  ?assertEqual(0, get_corr_id(NewReq)).

-endif. % TEST


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
