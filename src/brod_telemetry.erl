%%%
%%%   Copyright (c) 2017-2021 Klarna Bank AB (publ)
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

%% @private
-module(brod_telemetry).
-export([execute/2,
  execute/3,
  span/3]).

-ifndef(brod_use_telemetry).
  execute(_EventName, _Measurements) -> ok.
  execute(_EventName, _Measurements, _Metadata) -> ok.
  span(_EventPrefix, _StartMetadata, _SpanFunction) -> ok.
-else.
  execute(EventName, Measurements) -> telemetry:execute(EventName, Measurements).
  execute(EventName, Measurements, Metadata) -> telemetry:execute(EventName, Measurements, Metadata).
  span(EventPrefix, StartMetadata, SpanFunction) -> telemetry:span(EventPrefix, StartMetadata, SpanFunction).
-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
