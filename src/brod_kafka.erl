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
%%% @doc A kafka protocol implementation.
%%%      [https://cwiki.apache.org/confluence/display/KAFKA/
%%%       A+Guide+To+The+Kafka+Protocol].
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_kafka).

%% API
-export([ parse_stream/1
        , encode/3
        ]).

-export([ fetch_request/6
        , offset_request/4
        ]).

-include("brod_int.hrl").
-include("brod_kafka.hrl").

-define(INT, signed-integer).

%%%_* APIs =====================================================================

%% @doc Parse binary stream of kafka responses.
%%      Returns list of kpro_Response() and remaining binary.
%% @end
-spec parse_stream(binary()) -> {kpro_Response(), binary()}.
parse_stream(Bin) ->
  parse_stream(Bin, []).

parse_stream(Bin, Acc) ->
  case kpro:decode_response(Bin) of
    {incomplete, Rest} ->
      {lists:reverse(Acc), Rest};
    {Response, Rest} ->
      parse_stream(Rest, [Response | Acc])
  end.

encode(ClientId, CorrId, Request) ->
  R = #kpro_Request{ correlationId = CorrId
                   , clientId = ClientId
                   , requestMessage = Request
                   },
  kpro:encode_request(R).

-spec offset_request(topic(), partition(), integer(), non_neg_integer()) ->
        kpro_OffsetRequest().
offset_request(Topic, Partition, Time, MaxNoOffsets) ->
  PartitionReq =
    #kpro_OffsetRequestPartition{ partition          = Partition
                                , time               = Time
                                , maxNumberOfOffsets = MaxNoOffsets
                                },
  TopicReq =
    #kpro_OffsetRequestTopic{ topicName                = Topic
                            , offsetRequestPartition_L = [PartitionReq]
                            },
  #kpro_OffsetRequest{ replicaId            = ?REPLICA_ID
                     , offsetRequestTopic_L = [TopicReq]
                     }.

-spec fetch_request(topic(), partition(), offset(),
                    non_neg_integer(), non_neg_integer(), pos_integer()) ->
                      kpro_FetchRequest().
fetch_request(Topic, Partition, Offset, MaxWaitTime, MinBytes, MaxBytes) ->
  PerPartition =
    #kpro_FetchRequestPartition{ partition   = Partition
                               , fetchOffset = Offset
                               , maxBytes    = MaxBytes
                               },
  PerTopic =
    #kpro_FetchRequestTopic{ topicName               = Topic
                           , fetchRequestPartition_L = [PerPartition]
                           },
  #kpro_FetchRequest{ replicaId           = ?REPLICA_ID
                    , maxWaitTime         = MaxWaitTime
                    , minBytes            = MinBytes
                    , fetchRequestTopic_L = [PerTopic]
                    }.

%%%_* Internal Functions =======================================================

%%%_* Tests ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
