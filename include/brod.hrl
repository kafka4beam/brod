%%%
%%%   Copyright (c) 2014-2021, Klarna Bank AB (publ)
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

%% -record(kafka_message, {...}).
-include_lib("kafka_protocol/include/kpro_public.hrl").

-define(BROD_DEFAULT_CLIENT_ID, brod_default_client).
-define(BROD_CONSUMER_GROUP_PROTOCOL_VERSION, 0).

-record(kafka_message_set,
        { topic          :: brod:topic()
        , partition      :: brod:partition()
        , high_wm_offset :: integer() %% max offset of the partition
        , messages       :: [brod:message()] %% exposed to brod user
                          | kpro:incomplete_batch() %% this union member
                                                    %% is internal only
        }).

-record(kafka_fetch_error,
        { topic      :: brod:topic()
        , partition  :: brod:partition()
        , error_code :: brod:error_code()
        , error_desc = ""
        }).

-record(brod_call_ref, { caller :: undefined | pid()
                       , callee :: undefined | pid()
                       , ref    :: undefined | reference()
                       }).

-define(BROD_PRODUCE_UNKNOWN_OFFSET, -1).

-record(brod_produce_reply, { call_ref :: brod:call_ref()
                            , base_offset :: undefined | brod:offset()
                            , result   :: brod:produce_result()
                            }).

-record(kafka_group_member_metadata,
        { version   :: non_neg_integer()
        , topics    :: [brod:topic()]
        , user_data :: binary()
        }).

-record(brod_received_assignment,
        { topic        :: brod:topic()
        , partition    :: brod:partition()
        , begin_offset :: undefined | brod:offset() | {begin_offset, brod:offset_time()}
        }).

-type brod_received_assignments() :: [#brod_received_assignment{}].

-type brod_partition_fun() :: fun(( Topic         :: brod:topic()
                                  , PartitionsCnt :: integer()
                                  , Key           :: brod:key()
                                  , Value         :: brod:value()) ->
                                     {ok, Partition :: brod:partition()}).


-record(brod_cg, { id :: brod:group_id()
                 , protocol_type :: brod:cg_protocol_type()
                 }).

-define(BROD_FOLD_RET(Acc, NextOffset, Reason), {Acc, NextOffset, Reason}).

-define(BROD_DEFAULT_TIMEOUT, timer:seconds(5)).

-endif. % include brod.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
