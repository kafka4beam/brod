%%%
%%%   Copyright (c) 2017 Klarna AB
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

-export([ fetch_request/7
        , metadata_request/2
        , offsets_request/4
        , produce_request/7
        , offset_fetch_request/3
        ]).

-include("brod_int.hrl").

-type api() :: brod_kafka_apis:api().
-type vsn() :: brod_kafka_apis:vsn().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().

%% @doc Make a produce request, If the first arg is a `brod_sock' pid, call
%% `brod_kafka_apis:pick_version/2' to resolve version.
%%
%% NOTE: `pick_version' is essentially a ets lookup, for intensive callers
%%       like `brod_producer', we should pick version before hand
%%       and re-use it for each produce request.
%% @end
-spec produce_request(pid() | vsn(), topic(), partition(),
                      brod:kv_list(), integer(), integer(),
                      brod:compression()) -> kpro:req().
produce_request(MaybePid, Topic, Partition, KvList,
                RequiredAcks, AckTimeout, Compression) ->
  Vsn = pick_version(produce_request, MaybePid),
  kpro:produce_request(Vsn, Topic, Partition, KvList,
                       RequiredAcks, AckTimeout, Compression).

%% @doc Make a fetch request, If the first arg is a `brod_sock' pid, call
%% `brod_kafka_apis:pick_version/2' to resolve version.
%%
%% NOTE: `pick_version' is essentially a ets lookup, for intensive callers
%%       like `brod_producer', we should pick version before hand
%%       and re-use it for each produce request.
%% @end
-spec fetch_request(pid() | vsn(), topic(), partition(), offset(),
                    kpro:wait(), kpro:count(), kpro:count()) -> kpro:req().
fetch_request(MaybePid, Topic, Partition, Offset,
              WaitTime, MinBytes, MaxBytes) ->
  Vsn = pick_version(fetch_request, MaybePid),
  kpro:fetch_request(Vsn, Topic, Partition, Offset,
                     WaitTime, MinBytes, MaxBytes).

%% @doc Make a 'offsets_request' message for offset resolution.
%% In kafka protocol, -2 and -1 are semantic 'time' to request for
%% 'earliest' and 'latest' offsets.
%% In brod implementation, -2, -1, 'earliest' and 'latest'
%% are semantic 'offset', this is why often a variable named
%% Offset is used as the Time argument.
%% @end
-spec offsets_request(pid(), topic(), partition(), brod:offset_time()) ->
        kpro:req().
offsets_request(SockPid, Topic, Partition, TimeOrSemanticOffset) ->
  Time = ensure_integer_offset_time(TimeOrSemanticOffset),
  Vsn = pick_version(offsets_request, SockPid),
  kpro:offsets_request(Vsn, Topic, Partition, Time).

%% @doc Make a metadata request.
-spec metadata_request(pid(), [topic()]) -> kpro:req().
metadata_request(SockPid, Topics) ->
  Vsn = pick_version(metadata_request, SockPid),
  TopicsForEncoding =
    case Vsn of
      0                    -> Topics;
      _ when Topics =:= [] -> ?kpro_null;
      _                    -> Topics
    end,
  kpro:req(metadata_request, Vsn, [{topics, TopicsForEncoding}]).

%% @doc Make a offset fetch request.
%% NOTE: empty topics list only works for kafka 0.10.2.0 or later
%% @end
-spec offset_fetch_request(pid(), brod:group_id(), Topics) -> kpro:req()
        when Topics :: [{topic(), [partition()]}].
offset_fetch_request(SockPid, GroupId, Topics0) ->
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
  Vsn = pick_version(offset_fetch_request, SockPid),
  kpro:req(offset_fetch_request, Vsn, Body).

%% @private
-spec pick_version(api(), pid()) -> vsn().
pick_version(_API, Vsn) when is_integer(Vsn) -> Vsn;
pick_version(API, SockPid) when is_pid(SockPid) ->
  brod_kafka_apis:pick_version(SockPid, API);
pick_version(API, _) ->
  brod_kafka_apis:default_version(API).

%% @private
-spec ensure_integer_offset_time(brod:offset_time()) -> integer().
ensure_integer_offset_time(?OFFSET_EARLIEST)     -> -2;
ensure_integer_offset_time(?OFFSET_LATEST)       -> -1;
ensure_integer_offset_time(T) when is_integer(T) -> T.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
