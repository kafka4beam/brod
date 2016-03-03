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

-type kafka_topic()                    :: binary().
-type kafka_partition()                :: non_neg_integer().
-type kafka_offset()                   :: integer().
-type kafka_error_code()               :: atom() | integer().
-type kafka_consumer_group_id()        :: binary().
-type kafka_consumer_group_member_id() :: binary().

-record(kafka_message,
        { offset     :: kafka_offset()
        , magic_byte :: integer()
        , attributes :: integer()
        , key        :: binary()
        , value      :: binary()
        }).

-record(kafka_message_set,
        { topic          :: kafka_topic()
        , partition      :: kafka_partition()
        , high_wm_offset :: integer() %% max offset of the partition
        , messages       :: [#kafka_message{}]
        }).

-record(kafka_fetch_error,
        { topic      :: kafka_topic()
        , partition  :: kafka_partition()
        , error_code :: error_code()
        , error_desc :: string()
        }).

-type brod_client_id() :: atom().

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
-type brod_group_config() :: proplists:proplist().

-define(BROD_CONSUMER_GROUP_PROTOCOL_VERSION, 0).

-type brod_partition_fun() :: fun(( Topic :: topic()
                                  , PartitionsCnt :: integer()
                                  , Key           :: binary()
                                  , Value         :: binary()) ->
                                     {ok, Partition :: integer()}).

-endif. % include brod.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

