%%%
%%%   Copyright (c) 2017-2018 Klarna Bank AB (publ)
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

%% @doc Help functions to build request messages.
-module(brod_kafka_request).

-export([ fetch/7
        , list_groups/1
        , list_offsets/4
        , join_group/2
        , metadata/2
        , offset_commit/2
        , offset_fetch/3
        , produce/7
        , sync_group/2
        ]).

-include("brod_int.hrl").

-type api() :: brod_kafka_apis:api().
-type vsn() :: brod_kafka_apis:vsn().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type conn() :: kpro:connection().

-define(MIN_MAGIC_2_PRODUCE_API_VSN, 3).

%% @doc Make a produce request, If the first arg is a connection pid, call
%% `brod_kafka_apis:pick_version/2' to resolve version.
-spec produce(conn() | vsn(), topic(), partition(),
              kpro:batch_input(), integer(), integer(),
              brod:compression()) -> kpro:req().
produce(MaybePid, Topic, Partition, BatchInput,
        RequiredAcks, AckTimeout, Compression) ->
  Vsn = pick_version(produce, MaybePid),
  kpro_req_lib:produce(Vsn, Topic, Partition, BatchInput,
                       #{ required_acks => RequiredAcks
                        , ack_timeout => AckTimeout
                        , compression => Compression
                        }).

%% @doc Make a fetch request, If the first arg is a connection pid, call
%% `brod_kafka_apis:pick_version/2' to resolve version.
-spec fetch(conn(), topic(), partition(), offset(),
            kpro:wait(), kpro:count(), kpro:count()) -> kpro:req().
fetch(Pid, Topic, Partition, Offset,
      WaitTime, MinBytes, MaxBytes) ->
  Vsn = pick_version(fetch, Pid),
  kpro_req_lib:fetch(Vsn, Topic, Partition, Offset,
                     #{ max_wait_time => WaitTime
                      , min_bytes => MinBytes
                      , max_bytes => MaxBytes
                      }).

%% @doc Make a `list_offsets' request message for offset resolution.
%% In kafka protocol, -2 and -1 are semantic 'time' to request for
%% 'earliest' and 'latest' offsets.
%% In brod implementation, -2, -1, 'earliest' and 'latest'
%% are semantic 'offset', this is why often a variable named
%% Offset is used as the Time argument.
-spec list_offsets(conn(), topic(), partition(), brod:offset_time()) ->
        kpro:req().
list_offsets(Connection, Topic, Partition, TimeOrSemanticOffset) ->
  Time = ensure_integer_offset_time(TimeOrSemanticOffset),
  Vsn = pick_version(list_offsets, Connection),
  kpro_req_lib:list_offsets(Vsn, Topic, Partition, Time).

%% @doc Make a metadata request.
-spec metadata(vsn() | conn(), all | [topic()]) -> kpro:req().
metadata(Connection, Topics) when is_pid(Connection) ->
  Vsn = brod_kafka_apis:pick_version(Connection, metadata),
  metadata(Vsn, Topics);
metadata(Vsn, Topics) ->
  kpro_req_lib:metadata(Vsn, Topics).

%% @doc Make a offset fetch request.
%% NOTE: empty topics list only works for kafka 0.10.2.0 or later
-spec offset_fetch(conn(), brod:group_id(), Topics) -> kpro:req()
        when Topics :: [{topic(), [partition()]}].
offset_fetch(Connection, GroupId, Topics0) ->
  Topics =
    lists:map(
      fun({Topic, Partitions}) ->
        [ {topic, Topic}
        , {partitions, [[{partition, P}] || P <- Partitions]}
        ]
      end, Topics0),
  Body = [ {group_id, GroupId}
         , {topics, case Topics of
                      [] -> ?kpro_null;
                      _  -> Topics
                    end}
         ],
  Vsn = pick_version(offset_fetch, Connection),
  kpro:make_request(offset_fetch, Vsn, Body).

%% @doc Make a `list_groups' request.
-spec list_groups(conn()) -> kpro:req().
list_groups(Connection) ->
  Vsn = pick_version(list_groups, Connection),
  kpro:make_request(list_groups, Vsn, []).

%% @doc Make a `join_group' request.
-spec join_group(conn(), kpro:struct()) -> kpro:req().
join_group(Conn, Fields) ->
  make_req(join_group, Conn, Fields).

%% @doc Make a `sync_group' request.
-spec sync_group(conn(), kpro:struct()) -> kpro:req().
sync_group(Conn, Fields) ->
  make_req(sync_group, Conn, Fields).

%% @doc Make a `offset_commit' request.
-spec offset_commit(conn(), kpro:struct()) -> kpro:req().
offset_commit(Conn, Fields) ->
  make_req(offset_commit, Conn, Fields).

%%%_* Internal Functions =======================================================

make_req(API, Conn, Fields) when is_pid(Conn) ->
  Vsn = pick_version(API, Conn),
  make_req(API, Vsn, Fields);
make_req(API, Vsn, Fields) ->
  kpro:make_request(API, Vsn, Fields).

-spec pick_version(api(), pid()) -> vsn().
pick_version(_API, Vsn) when is_integer(Vsn) -> Vsn;
pick_version(API, Connection) when is_pid(Connection) ->
  brod_kafka_apis:pick_version(Connection, API);
pick_version(API, _) ->
  brod_kafka_apis:default_version(API).

-spec ensure_integer_offset_time(brod:offset_time()) -> integer().
ensure_integer_offset_time(?OFFSET_EARLIEST)     -> -2;
ensure_integer_offset_time(?OFFSET_LATEST)       -> -1;
ensure_integer_offset_time(T) when is_integer(T) -> T.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
