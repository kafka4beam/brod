%%%
%%%   Copyright (c) 2026 kafka4beam contributors
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

%% @doc Telemetry events emitted by brod.
%%
%% Brod emits events using the
%% <a href="https://github.com/beam-telemetry/telemetry">Beam Telemetry</a>
%% library. Users can attach handler functions to these events, for
%% example to report metrics. Each event is emitted by a function in
%% this module; see the function documentation for the measurements
%% and metadata attached to each event.
-module(brod_metrics).

-export([produce_request_sent/3]).

-type batch_input() :: brod:batch_input().
-type partition() :: brod:partition().
-type topic() :: brod:topic().

%% @doc Emit a `[brod, produce_request_sent]' event.
%%
%% Emitted by producers for each produce request successfully sent on
%% wire, i.e. one event per Kafka produce request (message batch).
%%
%% Note that retried batches emit the event again: the event counts
%% wire sends, so a summed `count' can exceed the number of unique
%% messages produced when retries occur.
%%
%% Measurements:
%% <ul>
%% <li>`count': number of messages in the request.</li>
%% <li>`bytes': total size of keys, values and headers of the messages
%%     in the request (before encoding and compression), plus 8 bytes
%%     per message accounting for the timestamp.</li>
%% </ul>
%%
%% Metadata: `topic' and `partition' the request was sent to.
-spec produce_request_sent(topic(), partition(), batch_input()) -> ok.
produce_request_sent(Topic, Partition, BatchInput) ->
  {Count, Bytes} = brod_utils:stats(BatchInput),
  telemetry:execute([brod, produce_request_sent],
                    #{ count => Count
                     , bytes => Bytes
                     },
                    #{ topic => Topic
                     , partition => Partition
                     }).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
