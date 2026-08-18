%%%
%%%   Copyright (c) 2026 Kafka4beam
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

-module(brod_kafka_request_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

offset_commit_request_test_() ->
  [ {lists:flatten(io_lib:format("version ~p", [Vsn])),
     fun() -> assert_offset_commit_request(Vsn) end}
    || Vsn <- lists:seq(2, 8)
  ].

assert_offset_commit_request(Vsn) ->
  GroupInstanceID = <<"unique-offset-commit-group-instance-id">>,
  Partition =
    [ {partition_index, 0}
    , {committed_offset, 1}
    , {committed_leader_epoch, -1}
    , {committed_metadata, <<>>}
    ],
  Fields =
    [ {group_id, <<"group">>}
    , {generation_id, 1}
    , {member_id, <<"member">>}
    , {group_instance_id, GroupInstanceID}
    , {retention_time_ms, -1}
    , {topics,
       [[ {name, <<"topic">>}
        , {partitions, [Partition]}
        ]]}
    ],
  Req = brod_kafka_request:offset_commit(Vsn, Fields),
  ?assertMatch(#kpro_req{api = offset_commit, vsn = Vsn}, Req),
  Encoded = iolist_to_binary(kpro:encode_request(<<"brod">>, 1, Req)),
  case Vsn >= 7 of
    true -> ?assertMatch({_, _}, binary:match(Encoded, GroupInstanceID));
    false -> ?assertEqual(nomatch, binary:match(Encoded, GroupInstanceID))
  end.
