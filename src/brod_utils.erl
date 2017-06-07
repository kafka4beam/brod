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
-export([ assert_client/1
        , assert_group_id/1
        , assert_topics/1
        , assert_topic/1
        , bytes/1
        , find_leader_in_metadata/3
        , get_metadata/1
        , get_metadata/2
        , get_metadata/3
        , is_normal_reason/1
        , is_pid_alive/1
        , log/3
        , os_time_utc_str/0
        , shutdown_pid/1
        , try_connect/1
        , try_connect/2
        , fetch_offsets/5
        , map_messages/2
        , fetch/4
        , init_sasl_opt/1
        , get_sasl_opt/1
        ]).

-include("brod_int.hrl").

%%%_* APIs =====================================================================

%% @doc Try to connect to any of the bootstrap nodes and fetch metadata
%% for all topics
%% @end
-spec get_metadata([endpoint()]) ->
        {ok, kpro_MetadataResponse()} | {error, any()}.
get_metadata(Hosts) ->
  get_metadata(Hosts, []).

%% @doc Try to connect to any of the bootstrap nodes and fetch metadata
%% for the given topics
%% @end
-spec get_metadata([endpoint()], [topic()]) ->
        {ok, kpro_MetadataResponse()} | {error, any()}.
get_metadata(Hosts, Topics) ->
  get_metadata(Hosts, Topics, _Options = []).

%% @doc Try to connect to any of the bootstrap nodes using the given
%% connection options and fetch metadata for the given topics.
%% @end
-spec get_metadata([endpoint()], [topic()], brod_sock:options()) ->
        {ok, kpro_MetadataResponse()} | {error, any()}.
get_metadata(Hosts, Topics, Options) ->
  {ok, Pid} = try_connect(Hosts, Options),
  try
    Request = #kpro_MetadataRequest{topicName_L = Topics},
    brod_sock:request_sync(Pid, Request, 10000)
  after
    _ = brod_sock:stop(Pid)
  end.

%% @doc Try connect to any of the given bootstrap nodes.
-spec try_connect([endpoint()]) -> {ok, pid()} | {error, any()}.
try_connect(Hosts) ->
  try_connect(Hosts, [], ?undef).

%% @doc Try connect to any of the given bootstrap nodes using
%% the given connect options.
%% @end
try_connect(Hosts, Options) ->
  try_connect(Hosts, Options, ?undef).

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

%% @doc Request (sync) for topic-partition offsets.
-spec fetch_offsets(pid(), topic(), partition(),
                    offset_time(), pos_integer()) -> {ok, [offset()]}.
fetch_offsets(SocketPid, Topic, Partition, TimeOrSemanticOffset, NrOfOffsets) ->
  Request = offset_request(Topic, Partition, TimeOrSemanticOffset, NrOfOffsets),
  {ok, Response} = brod_sock:request_sync(SocketPid, Request, 5000),
  #kpro_OffsetResponse{topicOffsets_L = [TopicOffsets]} = Response,
  #kpro_TopicOffsets{partitionOffsets_L = [PartitionOffsets]} = TopicOffsets,
  #kpro_PartitionOffsets{offset_L = Offsets} = PartitionOffsets,
  {ok, Offsets}.

-spec assert_client(brod:client_id() | pid()) -> ok | no_return().
assert_client(Client) ->
  ok_when(is_atom(Client) orelse is_pid(Client),
          {bad_client, Client}).

-spec assert_group_id(brod:group_id()) -> ok | no_return().
assert_group_id(GroupId) ->
  ok_when(is_binary(GroupId) andalso size(GroupId) > 0,
          {bad_group_id, GroupId}).

-spec assert_topics([brod:topic()]) -> ok | no_return().
assert_topics(Topics) ->
  Pred = fun(Topic) -> ok =:= assert_topic(Topic) end,
  ok_when(is_list(Topics) andalso Topics =/= [] andalso lists:all(Pred, Topics),
          {bad_topics, Topics}).

-spec assert_topic(brod:topic()) -> ok | no_return().
assert_topic(Topic) ->
  ok_when(is_binary(Topic) andalso size(Topic) > 0,
          {bad_topic, Topic}).

%% @doc Map message to brod's format.
%% incomplete message indicator is kept when the only one message is incomplete.
%% Messages having offset earlier than the requested offset are discarded.
%% this might happen for compressed message sets
%% @end
-spec map_messages(offset(), [ {?incomplete_message, non_neg_integer()}
                             | kpro_Message()
                             ]) ->
       {?incomplete_message, non_neg_integer()} | [#kafka_message{}].
map_messages(BeginOffset, Messages) when is_binary(Messages) ->
  map_messages(BeginOffset, kpro:decode_message_set(Messages));
map_messages(_BeginOffset, [{?incomplete_message, Size}]) ->
  {?incomplete_message, Size};
map_messages(BeginOffset, Messages) when is_list(Messages) ->
  [kafka_message(M) || M <- Messages,
   is_record(M, kpro_Message) andalso
   M#kpro_Message.offset >= BeginOffset].

%% @doc For brod_cli (or Erlang shell debugging) to fetch one message-set.
-spec fetch(pid(), fun((non_neg_integer()) -> kpro_FetchRequest()),
            non_neg_integer(), offset()) ->
               {ok, [#kafka_message{}]} | {error, any()}.
fetch(SockPid, ReqFun, MaxBytes, Offset) ->
  Request = ReqFun(MaxBytes),
  %% infinity here because brod_sock has a global 'request_timeout' option
  {ok, Response} = brod_sock:request_sync(SockPid, Request, infinity),
  #kpro_FetchResponse{fetchResponseTopic_L = [TopicFetchData]} = Response,
  #kpro_FetchResponseTopic{fetchResponsePartition_L = [PM]} = TopicFetchData,
  #kpro_FetchResponsePartition{ errorCode  = ErrorCode
                              , message_L  = Messages0
                              } = PM,
  case kpro_ErrorCode:is_error(ErrorCode) of
    true ->
      {error, kpro_ErrorCode:desc(ErrorCode)};
    false ->
      case brod_utils:map_messages(Offset, Messages0) of
        {?incomplete_message, Size} ->
          fetch(SockPid, ReqFun, Size, Offset);
        Messages ->
          {ok, Messages}
      end
  end.

%% @doc Get sasl options from client config.
-spec get_sasl_opt(client_config()) -> sasl_opt().
get_sasl_opt(Config) ->
  case proplists:get_value(sasl, Config) of
    {plain, User, PassFun} when is_function(PassFun) ->
      {plain, User, PassFun()};
    {plain, File} ->
      {User, Pass} = read_sasl_file(File),
      {plain, User, Pass};
    Other ->
      Other
  end.

%% @doc Hide sasl plain password in an anonymous function to avoid
%% the plain text being dumped to crash logs
%% @end
-spec init_sasl_opt(client_config()) -> client_config().
init_sasl_opt(Config) ->
  case proplists:get_value(sasl, Config) of
    {plain, User, Pass} when is_list(Pass) orelse is_binary(Pass) ->
      replace_prop(sasl, {plain, User, fun() -> Pass end}, Config);
    {plain, File} ->
      {User, Pass} = read_sasl_file(File),
      replace_prop(sasl, {plain, User, fun() -> Pass end}, Config);
    _Other ->
      Config
  end.

%%%_* Internal Functions =======================================================

%% @private
-spec replace_prop(term(), term(), proplists:proplist()) ->
        proplists:proplist().
replace_prop(Key, Value, PropL0) ->
  PropL = proplists:delete(Key, PropL0),
  [{Key, Value} | PropL].

%% @private Read a regular file, assume it has two lines:
%% First line is the sasl-plain username
%% Second line is the password
%% @end
-spec read_sasl_file(file:name_all()) -> {binary(), binary()}.
read_sasl_file(File) ->
  {ok, Bin} = file:read_file(File),
  Lines = binary:split(Bin, <<"\n">>, [global]),
  [User, Pass] = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
  {User, Pass}.

%% @private
try_connect([], _Options, LastError) ->
  LastError;
try_connect([{Host, Port} | Hosts], Options, _) ->
  %% Do not 'start_link' to avoid unexpected 'EXIT' message.
  %% Should be ok since we're using a single blocking request which
  %% monitors the process anyway.
  case brod_sock:start(self(), Host, Port,
                       ?BROD_DEFAULT_CLIENT_ID, Options) of
    {ok, Pid} -> {ok, Pid};
    Error     -> try_connect(Hosts, Options, Error)
  end.

%% @private Convert a `kpro_Message' to a `kafka_message'.
-spec kafka_message(#kpro_Message{}) -> #kafka_message{}.
kafka_message(#kpro_Message{ offset     = Offset
                           , magicByte  = MagicByte
                           , attributes = Attributes
                           , key        = MaybeKey
                           , value      = Value
                           , crc        = Crc
                           }) ->
  Key = case MaybeKey of
          ?undef -> <<>>;
          _      -> MaybeKey
        end,
  #kafka_message{ offset     = Offset
                , magic_byte = MagicByte
                , attributes = Attributes
                , key        = Key
                , value      = Value
                , crc        = Crc
                }.

%% @private Raise an 'error' exception when first argument is not 'true'.
%% The second argument is used as error reason.
%% @end
-spec ok_when(boolean(), any()) -> ok | no_return().
ok_when(true, _) -> ok;
ok_when(_, Reason) -> erlang:error(Reason).

%% @private Make a 'OffsetRequest' request message for fetching offsets.
%% In kafka protocol, -2 and -1 are semantic 'time' to request for
%% 'earliest' and 'latest' offsets.
%% In brod implementation, -2, -1, 'earliest' and 'latest'
%% are semantic 'offset', this is why often a variable named
%% Offset is used as the Time argument.
%% @end
-spec offset_request(topic(), partition(),
                     offset_time(), pos_integer()) -> kpro_OffsetRequest().
offset_request(Topic, Partition, TimeOrSemanticOffset, MaxOffsets) ->
  Time = ensure_integer_offset_time(TimeOrSemanticOffset),
  kpro:offset_request(Topic, Partition, Time, MaxOffsets).

ensure_integer_offset_time(?OFFSET_EARLIEST)     -> -2;
ensure_integer_offset_time(?OFFSET_LATEST)       -> -1;
ensure_integer_offset_time(T) when is_integer(T) -> T.

-spec do_find_leader_in_metadata(kpro_MetadataResponse(),
                                 topic(), partition()) -> endpoint().
do_find_leader_in_metadata(Metadata, Topic, Partition) ->
  #kpro_MetadataResponse{ broker_L        = Brokers
                        , topicMetadata_L = [TopicMetadata]
                        } = Metadata,
  #kpro_TopicMetadata{ errorCode           = TopicEC
                     , topicName           = RealTopic
                     , partitionMetadata_L = Partitions
                     } = TopicMetadata,
  RealTopic /= Topic andalso erlang:throw(?EC_UNKNOWN_TOPIC_OR_PARTITION),
  kpro_ErrorCode:is_error(TopicEC) andalso erlang:throw(TopicEC),
  Id = case lists:keyfind(Partition,
                          #kpro_PartitionMetadata.partition, Partitions) of
         #kpro_PartitionMetadata{leader = Leader} when Leader >= 0 ->
           Leader;
         #kpro_PartitionMetadata{} ->
           erlang:throw(?EC_LEADER_NOT_AVAILABLE);
         false ->
           erlang:throw(?EC_UNKNOWN_TOPIC_OR_PARTITION)
       end,
  Broker = lists:keyfind(Id, #kpro_Broker.nodeId, Brokers),
  Host = Broker#kpro_Broker.host,
  Port = Broker#kpro_Broker.port,
  {binary_to_list(Host), Port}.

-define(IS_BYTE(I), (I>=0 andalso I<256)).

-spec bytes(key() | value() | kv_list()) -> non_neg_integer().
bytes([]) -> 0;
bytes(undefined) -> 0;
bytes(I) when ?IS_BYTE(I) -> 1;
bytes(B) when is_binary(B) -> erlang:size(B);
bytes({K, V}) -> bytes(K) + bytes(V);
bytes([H | T]) -> bytes(H) + bytes(T).

%%%_* Tests ====================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif. % TEST

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
