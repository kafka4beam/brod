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

-export([ produce_request/7
        ]).

-type api() :: brod_kafka_apis:api().
-type vsn() :: brod_kafka_apis:vsn().
-type topic() :: brod:topic().
-type partition() :: brod:partition().

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
  Vsn = case is_pid(MaybePid) of
          true  -> pick_version(produce_request, MaybePid);
          false -> MaybePid
        end,
  kpro:produce_request(Vsn, Topic, Partition, KvList,
                       RequiredAcks, AckTimeout, Compression).

%% @private
-spec pick_version(api(), pid()) -> vsn().
pick_version(API, SockPid) ->
  brod_kafka_apis:pick_version(API, SockPid).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
