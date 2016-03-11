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
%%% @doc
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_utils).

%% Exports
-export([ find_leader_in_metadata/3
        , get_metadata/1
        , get_metadata/2
        , is_normal_reason/1
        , is_pid_alive/1
        , os_time_utc_str/0
        , shutdown_pid/1
        , try_connect/1
        , log/3
        ]).

-include("brod_int.hrl").

%%%_* APIs =====================================================================

%% try to connect to any of bootstrapped nodes and fetch metadata
get_metadata(Hosts) ->
  get_metadata(Hosts, []).

get_metadata(Hosts, Topics) ->
  {ok, Pid} = try_connect(Hosts),
  Request = #kpro_MetadataRequest{topicName_L = Topics},
  Response = brod_sock:send_sync(Pid, Request, 10000),
  ok = brod_sock:stop(Pid),
  Response.

try_connect(Hosts) ->
  try_connect(Hosts, []).

try_connect([], LastError) ->
  LastError;
try_connect([{Host, Port} | Hosts], _) ->
  %% Do not 'start_link' to avoid unexpected 'EXIT' message.
  %% Should be ok since we're using a single blocking request which
  %% monitors the process anyway.
  case brod_sock:start(self(), Host, Port, ?BROD_DEFAULT_CLIENT_ID, []) of
    {ok, Pid} -> {ok, Pid};
    Error     -> try_connect(Hosts, Error)
  end.

%% @doc Check terminate reason for a gen_server implementation
is_normal_reason(normal)        -> true;
is_normal_reason(shutdown)      -> true;
is_normal_reason({shutdown, _}) -> true;
is_normal_reason(_)             -> false.

is_pid_alive(Pid) ->
  is_pid(Pid) andalso is_process_alive(Pid).

shutdown_pid(Pid) ->
  case is_pid_alive(Pid) of
    true  -> exit(Pid, shutdown);
    false -> ok
  end.

%% @doc Find leader broker ID for the given topic-partiton in
%% the metadata response received from socket.
%% @end
-spec find_leader_in_metadata(kpro_MetadataResponse(), topic(), partition()) ->
        {ok, endpoint()} | {error, any()}.
find_leader_in_metadata(Metadata, Topic, Partition) ->
  try
    {ok, do_find_leader_in_metadata(Metadata, Topic, Partition)}
  catch throw : Reason ->
    {error, Reason}
  end.

-spec os_time_utc_str() -> string().
os_time_utc_str() ->
  Ts = os:timestamp(),
  {{Y,M,D}, {H,Min,Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

%% @doc simple wrapper around error_logger.
%% NOTE: keep making MFA calls to error_logger to
%%       1. allow logging libraries such as larger parse_transform
%%       2. be more xref friendly
%% @end
-spec log(info | warning | error, string(), [any()]) -> ok.
log(info,    Fmt, Args) -> error_logger:info_msg(Fmt, Args);
log(warning, Fmt, Args) -> error_logger:warning_msg(Fmt, Args);
log(error,   Fmt, Args) -> error_logger:error_msg(Fmt, Args).

%%%_* Internal Functions =======================================================

-spec do_find_leader_in_metadata(kpro_MetadataResponse(),
                                 topic(), partition()) ->
                                    endpoint() | no_return().
do_find_leader_in_metadata(Metadata, Topic, Partition) ->
  #kpro_MetadataResponse{ broker_L        = Brokers
                        , topicMetadata_L = [TopicMetadata]
                        } = Metadata,
  #kpro_TopicMetadata{ errorCode           = TopicEC
                     , partitionMetadata_L = Partitions
                     } = TopicMetadata,
  kpro_ErrorCode:is_error(TopicEC) andalso erlang:throw(TopicEC),
  #kpro_PartitionMetadata{leader = Id} =
    lists:keyfind(Partition, #kpro_PartitionMetadata.partition, Partitions),
  Id >= 0 orelse erlang:throw({no_leader, {Topic, Partition}}),
  Broker = lists:keyfind(Id, #kpro_Broker.nodeId, Brokers),
  Host = Broker#kpro_Broker.host,
  Port = Broker#kpro_Broker.port,
  {binary_to_list(Host), Port}.

%%%_* Tests ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
