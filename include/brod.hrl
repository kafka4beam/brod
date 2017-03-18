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

-define(BROD_DEFAULT_CLIENT_ID, brod_default_client).
-define(BROD_CONSUMER_GROUP_PROTOCOL_VERSION, 0).

-record(kafka_message,
        { offset     :: brod:kafka_offset()
        , magic_byte :: integer()
        , attributes :: integer()
        , key        :: binary()
        , value      :: binary()
        , crc        :: integer()
        }).

-record(kafka_message_set,
        { topic          :: brod:kafka_topic()
        , partition      :: brod:kafka_partition()
        , high_wm_offset :: integer() %% max offset of the partition
        , messages       :: kpro:incomplete_message() | [brod:kafka_message()]
        }).

-record(kafka_fetch_error,
        { topic      :: brod:kafka_topic()
        , partition  :: brod:kafka_partition()
        , error_code :: brod:kafka_error_code()
        , error_desc :: binary()
        }).

-record(brod_call_ref, { caller :: pid()
                       , callee :: pid()
                       , ref    :: reference()
                       }).

-record(brod_produce_reply, { call_ref :: brod:brod_call_ref()
                            , result   :: brod:brod_produce_result()
                            }).

-record(kafka_group_member_metadata,
        { version   :: non_neg_integer()
        , topics    :: [brod:kafka_topic()]
        , user_data :: binary()
        }).

-record(brod_received_assignment,
        { topic        :: brod:kafka_topic()
        , partition    :: brod:kafka_partition()
        , begin_offset :: undefined | brod:kafka_offset()
        }).

-endif.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
