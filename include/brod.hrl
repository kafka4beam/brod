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

-ifndef(__BROD_HRL).
-define(__BROD_HRL, true).

-type topic()      :: binary().
-type partition()  :: non_neg_integer().
-type offset()     :: integer().
-type error_code() :: atom() | integer().

-record(message, { offset     :: offset()
                 , crc        :: integer()
                 , magic_byte :: integer()
                 , attributes :: integer()
                 , key        :: binary()
                 , value      :: binary()
                 }).

-record(message_set, { topic          :: topic()
                     , partition      :: partition()
                     , high_wm_offset :: integer()
                     , messages       :: [#message{}]
                     }).

-record(brod_fetch_error, { topic      :: topic()
                          , partition  :: partition()
                          , error_code :: error_code()
                          , error_desc :: string()
                          }).

-type client_id() :: atom().

-define(BROD_DEFAULT_CLIENT_ID, brod_default_client).

-record(brod_call_ref, { caller :: pid()
                       , callee :: pid()
                       , ref    :: reference()
                       }).

-type brod_call_ref() :: #brod_call_ref{}.

-type brod_produce_result() :: brod_produce_req_buffered
                             | brod_produce_req_acked.

-record(brod_produce_reply, { call_ref :: brod_call_ref()
                            , result   :: brod_produce_result()
                            }).

-type brod_produce_reply() :: #brod_produce_reply{}.

-type brod_client_config() :: proplists:proplist().
-type brod_producer_config() :: proplists:proplist().
-type brod_consumer_config() :: proplists:proplist().

-endif. % include brod.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

