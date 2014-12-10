%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_utils).

%% Exports
-export([ fetch_response_to_message_set/1
        , get_metadata/1
        , get_metadata/2
        , try_connect/1
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* Code ---------------------------------------------------------------------
%% try to connect to any of bootstrapped nodes and fetch metadata
get_metadata(Hosts) ->
  get_metadata(Hosts, []).

get_metadata(Hosts, Topics) ->
  {ok, Pid} = try_connect(Hosts),
  Request = #metadata_request{topics = Topics},
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
  case brod_sock:start(self(), Host, Port, []) of
    {ok, Pid} -> {ok, Pid};
    Error     -> try_connect(Hosts, Error)
  end.

fetch_response_to_message_set(#fetch_response{topics = [TopicFetchData]}) ->
  #topic_fetch_data{topic = Topic, partitions = [PM]} = TopicFetchData,
  #partition_messages{ partition = Partition
                     , high_wm_offset = HighWmOffset
                     , messages = Messages} = PM,
  #message_set{ topic = Topic
              , partition = Partition
              , high_wm_offset = HighWmOffset
              , messages = Messages}.

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
