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

-ifndef(__BROD_HRL).
-define(__BROD_HRL, true).

-record(message, { offset     :: integer()
                 , crc        :: integer()
                 , magic_byte :: integer()
                 , attributes :: integer()
                 , key        :: binary()
                 , value      :: binary()
                 }).

%% delivered to subsriber by brod_consumer
-record(message_set, { topic          :: binary()
                     , partition      :: integer()
                     , high_wm_offset :: integer()
                     , messages       :: [#message{}]
                     }).

-type callback_fun() :: fun((#message_set{}) -> any()) |
                        fun((Offset :: integer(), Key :: binary(), Value :: binary()) -> any()).

-type client_id() :: atom().

-define(BROD_DEFAULT_CLIENT_ID, brod_default_client).

-define(BROD_PRODUCE_CALL(CallerPid, PartitionProducerPidMonitorRef),
        {CallerPid, PartitionProducerPidMonitorRef}).

-type brod_produce_call() :: ?BROD_PRODUCE_CALL(pid(), reference()).

-type brod_produce_reply_result() :: brod_produce_req_buffered
                                   | brod_produce_req_acked.

-record(brod_produce_reply, { call   :: brod_produce_call()
                            , result :: brod_produce_reply_result()
                            }).

-type brod_produce_reply() :: #brod_produce_reply{}.

-type brod_partitionner() :: random
                           | roundrobin
                           | fun((Key::binary(), NrOfPartitions::pos_integer()) -> partition()).

-type brod_client_config() :: proplists:proplist().
-type brod_producer_config() :: proplists:proplist().
-type brod_consumer_config() :: proplists:proplist().

-endif. % include brod.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

