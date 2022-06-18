%%%
%%%   Copyright (c) 2019-2021, Klarna Bank AB (publ)
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
-module(brod_telemetry_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , all/0
        , suite/0
        ,handle_event/4
        ]).

%% Test cases
-export([ t_init_client/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TIMEOUT, 280000).

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(telemetry),
  Config.

end_per_suite(_Config) -> ok.

all() -> [t_init_client].

%%%_* Test functions ===========================================================
handle_event(Event, Measurements, Metadata, #{pid := Pid}) -> Pid! {Event, Measurements, Metadata}.

t_init_client(Config) when is_list(Config) ->
  ok = telemetry:attach_many(
    <<"t-init-repo-handler">>,
    [[brod, client, init]],
    fun ?MODULE:handle_event/4,
    #{pid => self()}
  ),
  application:set_env(brod, clients, [{t_init_client,
    [
      {endpoints, [{"localhost", 9092}]}
    , {auto_start_producers, true}]}]),
  {ok, _} = application:ensure_all_started(brod),
  receive
    { [brod, client, init]
    , #{system_time := _Time}
    , #{client_id := t_init_client, args := _Args}} -> ok;

    Msg -> error({bad_message, t_init_client, Msg})
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
