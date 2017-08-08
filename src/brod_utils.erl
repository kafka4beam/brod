%%%
%%%   Copyright (c) 2014-2017, Klarna AB
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

-module(brod_utils).

%% Exports
-export([ assert_client/1
        , assert_group_id/1
        , assert_topics/1
        , assert_topic/1
        , bytes/1
        , connect_group_coordinator/3
        , decode_messages/2
        , describe_groups/3
        , fetch/8
        , fetch_committed_offsets/2
        , fetch_committed_offsets/3
        , find_leader_in_metadata/3
        , find_struct/3
        , get_metadata/1
        , get_metadata/2
        , get_metadata/3
        , get_sasl_opt/1
        , init_sasl_opt/1
        , is_normal_reason/1
        , is_pid_alive/1
        , list_all_groups/2
        , list_groups/2
        , log/3
        , make_fetch_fun/6
        , os_time_utc_str/0
        , resolve_offset/4
        , resolve_offset/5
        , shutdown_pid/1
        , try_connect/1
        , try_connect/2
        , resolve_group_coordinator/3
        ]).

-include("brod_int.hrl").

-type req_fun() :: fun((offset(), kpro:count()) -> kpro:req()).
-type fetch_fun() :: fun((offset()) -> {ok, [brod:message()]} | {error, any()}).
-type sock_opts() :: brod:sock_opts().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type endpoint() :: brod:endpoint().
-type offset_time() :: brod:offset_time().
-type group_id() :: brod:group_id().

%%%_* APIs =====================================================================

%% @doc Try to connect to any of the bootstrap nodes and fetch metadata
%% for all topics
%% @end
-spec get_metadata([endpoint()]) -> {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts) ->
  get_metadata(Hosts, []).

%% @doc Try to connect to any of the bootstrap nodes and fetch metadata
%% for the given topics
%% @end
-spec get_metadata([endpoint()], [topic()]) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics) ->
  get_metadata(Hosts, Topics, _Options = []).

%% @doc Try to connect to any of the bootstrap nodes using the given
%% connection options and fetch metadata for the given topics.
%% @end
-spec get_metadata([endpoint()], [topic()], sock_opts()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics, Options) ->
  with_sock(
    try_connect(Hosts, Options),
    fun(Pid) ->
      Vsn = 0, %% TODO pick version
      Body = [{topics, Topics}],
      Request = kpro:req(metadata_request, Vsn, Body),
      #kpro_rsp{ tag = metadata_response
               , vsn = Vsn
               , msg = Msg
               } = request_sync(Pid, Request),
      {ok, Msg}
    end).

%% @doc Resolve timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(),
                     offset_time(), sock_opts()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time, Options) when is_list(Options) ->
  with_sock(
    brod:connect_leader(Hosts, Topic, Partition, Options),
    fun(Pid) ->
        resolve_offset(Pid, Topic, Partition, Time)
    end).

%% @doc Resolve timestamp to real offset.
-spec resolve_offset(pid(), topic(), partition(), offset_time()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(SocketPid, Topic, Partition, Time) ->
  Request = offsets_request(Topic, Partition, Time),
  #kpro_rsp{tag = offsets_response
           , vsn = Vsn
           , msg = Msg
           } = request_sync(SocketPid, Request),
  [Response] = kf(responses, Msg),
  [PartitionRespons] = kf(partition_responses, Response),
  Ec = kf(error_code, PartitionRespons),
  ?IS_ERROR(Ec) andalso erlang:throw(Ec),
  case Vsn of
    0 ->
      case kf(offsets, PartitionRespons) of
        [Offset] -> {ok, Offset};
        []       -> {error, not_found}
      end;
    1 ->
      {ok, kf(offset, PartitionRespons)}
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
-spec find_leader_in_metadata(kpro:struct(), topic(), partition()) ->
        {ok, endpoint()} | {error, any()}.
find_leader_in_metadata(Metadata, Topic, Partition) ->
  try
    {ok, do_find_leader_in_metadata(Metadata, Topic, Partition)}
  catch throw : Reason ->
    {error, Reason}
  end.

%% @doc Get now timestamp, and format as UTC string.
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

%% @doc Assert client_id is an atom().
-spec assert_client(brod:client_id() | pid()) -> ok | no_return().
assert_client(Client) ->
  ok_when(is_atom(Client) orelse is_pid(Client),
          {bad_client, Client}).

%% @doc Assert group_id is a binary().
-spec assert_group_id(group_id()) -> ok | no_return().
assert_group_id(GroupId) ->
  ok_when(is_binary(GroupId) andalso size(GroupId) > 0,
          {bad_group_id, GroupId}).

%% @doc Assert a list of topic names [binary()].
-spec assert_topics([topic()]) -> ok | no_return().
assert_topics(Topics) ->
  Pred = fun(Topic) -> ok =:= assert_topic(Topic) end,
  ok_when(is_list(Topics) andalso Topics =/= [] andalso lists:all(Pred, Topics),
          {bad_topics, Topics}).

%% @doc Assert topic is a binary().
-spec assert_topic(topic()) -> ok | no_return().
assert_topic(Topic) ->
  ok_when(is_binary(Topic) andalso size(Topic) > 0,
          {bad_topic, Topic}).

%% @doc Map message to brod's format.
%% incomplete message indicator is kept when the only one message is incomplete.
%% Messages having offset earlier than the requested offset are discarded.
%% this might happen for compressed message sets
%% @end
-spec decode_messages(offset(), kpro:incomplete_message() | [brod:message()]) ->
        kpro:incomplete_message() | [brod:message()].
decode_messages(BeginOffset, Messages) when is_binary(Messages) ->
  decode_messages(BeginOffset, kpro:decode_message_set(Messages));
decode_messages(_BeginOffset, ?incomplete_message(_) = Incomplete) ->
  Incomplete;
decode_messages(BeginOffset, Messages) when is_list(Messages) ->
  drop_old_messages(BeginOffset, Messages).

%% @doc Fetch a single message set from the given topic-partition.
-spec fetch([endpoint()], topic(), partition(), offset(),
            non_neg_integer(), non_neg_integer(), pos_integer(),
            sock_opts()) -> {ok, [#kafka_message{}]} | {error, any()}.
fetch(Hosts, Topic, Partition, Offset, WaitTime,
      MinBytes, MaxBytes, Options) ->
  with_sock(
    brod:connect_leader(Hosts, Topic, Partition, Options),
    fun(Pid) ->
        Fetch = make_fetch_fun(Pid, Topic, Partition,
                               WaitTime, MinBytes, MaxBytes),
        Fetch(Offset)
    end).

%% @doc Make a fetch function which should expand `max_bytes' when
%% it is not big enough to fetch one signle message.
%% @end
-spec make_fetch_fun(pid(), topic(), partition(), kpro:wait(),
                     kpro:count(), kpro:count()) -> fetch_fun().
make_fetch_fun(SockPid, Topic, Partition, WaitTime, MinBytes, MaxBytes) ->
  ReqFun = make_req_fun(SockPid, Topic, Partition, WaitTime, MinBytes),
  fun(Offset) -> fetch(SockPid, ReqFun, Offset, MaxBytes) end.

%% @doc Get sasl options from client config.
-spec get_sasl_opt(brod:client_config()) -> sasl_opt().
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
-spec init_sasl_opt(brod:client_config()) -> brod:client_config().
init_sasl_opt(Config) ->
  case get_sasl_opt(Config) of
    {plain, User, Pass} when not is_function(Pass) ->
      replace_prop(sasl, {plain, User, fun() -> Pass end}, Config);
    _Other ->
      Config
  end.

%% @doc Fethc ommitted offsets for ALL topics in the given consumer group.
-spec fetch_committed_offsets([endpoint()], sock_opts(), group_id()) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(BootstrapEndpoints, SockOpts, GroupId) ->
  with_sock(
    connect_group_coordinator(BootstrapEndpoints, SockOpts, GroupId),
    fun(Pid) -> fetch_committed_offsets(Pid, GroupId) end).

%% @doc Same as `fetch_committed_offsets/3', only work on a socket
%% connected to the group coordinator broker.
%% @end
-spec fetch_committed_offsets(pid(), group_id()) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(SockPid, GroupId) ->
  Body = [ {group_id, GroupId}
         , {topics, ?undef} %% Fetch ALL when topics array is null
         ],
  %% {topics, ?undef} works only with version 2
  Vsn = 2,
  Req = kpro:req(offset_fetch_request, Vsn, Body),
  try
    #kpro_rsp{ tag = offset_fetch_response
             , vsn = Vsn
             , msg = Msg
             } = request_sync(SockPid, Req),
    TopicsArray = kf(responses, Msg),
    {ok, TopicsArray}
  catch
    throw : Reason ->
      {error, Reason}
  end.

%%%_* Internal Functions =======================================================

%% @private Make a function to build fetch requests.
%% The function takes offset and max_bytes as input as these two parameters
%% are varient when continuously polling a specific topic-partition.
%% @end
-spec make_req_fun(pid(), topic(), partition(),
                   kpro:wait(), kpro:count()) -> req_fun().
make_req_fun(_SockPid, Topic, Partition, WaitTime, MinBytes) ->
  Vsn = 0, %% TODO pick version (make use of SockPid)
  fun(Offset, MaxBytes) ->
      kpro:fetch_request(Vsn, Topic, Partition, Offset,
                         WaitTime, MinBytes, MaxBytes)
  end.

%% @doc Fetch a message-set. If the given MaxBytes is not enough to fetch a
%% single message, expand it to fetch exactly one message
%% @end
-spec fetch(pid(), req_fun(), offset(), kpro:count()) ->
               {ok, [brod:message()]} | {error, any()}.
fetch(SockPid, ReqFun, Offset, MaxBytes) when is_pid(SockPid) ->
  Request = ReqFun(Offset, MaxBytes),
  #kpro_rsp{ tag = fetch_response
           , msg = Msg
           } = request_sync(SockPid, Request, infinity),
  [Response] = kf(responses, Msg),
  [PartitionResponse] = kf(partition_responses, Response),
  Header = kf(partition_header, PartitionResponse),
  Messages0 = kf(record_set, PartitionResponse),
  ErrorCode = kf(error_code, Header),
  case ?IS_ERROR(ErrorCode) of
    true ->
      {error, kpro_error_code:desc(ErrorCode)};
    false ->
      case decode_messages(Offset, Messages0) of
        ?incomplete_message(Size) ->
          fetch(SockPid, ReqFun, Offset, Size);
        Messages ->
          {ok, Messages}
      end
  end.

%% @doc List all groups in the given cluster.
%% NOTE: Exception if failed against any of the coordinator brokers.
%% @end
-spec list_all_groups([endpoint()], sock_opts()) ->
        [{endpoint(), [brod:cg()] | {error, any()}}].
list_all_groups(Endpoints, Options) ->
  {ok, Metadata} = get_metadata(Endpoints, [], Options),
  Brokers0 = kf(brokers, Metadata),
  Brokers = [{binary_to_list(kf(host, B)), kf(port, B)} || B <- Brokers0],
  lists:foldl(
    fun(Broker, Acc) ->
        case list_groups(Broker, Options) of
          {ok, Groups}    -> [{Broker, Groups} | Acc];
          {error, Reason} -> [{Broker, {error, Reason}} | Acc]
        end
    end, [], Brokers).

%% @doc List all groups in the given coordinator broker.
-spec list_groups(endpoint(), sock_opts()) ->
        {ok, [brod:cg()]} | {error, any()}.
list_groups(Endpoint, Options) ->
  with_sock(
    try_connect([Endpoint], Options),
    fun(Pid) ->
      Vsn = 0, %% only one version
      Body = [], %% this request has no struct field
      Request = kpro:req(list_groups_request, Vsn, Body),
      #kpro_rsp{ tag = list_groups_response
               , vsn = Vsn
               , msg = Msg
               } = request_sync(Pid, Request),
      ErrorCode = kf(error_code, Msg),
      case ?IS_ERROR(ErrorCode) of
        true ->
          {error, ErrorCode};
        false ->
          Groups =
            lists:map(
              fun(Struct) ->
                  Id = kf(group_id, Struct),
                  Type = kf(protocol_type, Struct),
                  #brod_cg{ id = Id
                          , protocol_type = Type
                          }
              end, kf(groups, Msg)),
          {ok, Groups}
      end
    end).

%% @doc Send describe_groups_request and wait for describe_groups_response.
-spec describe_groups(endpoint(), sock_opts(), [brod:group_id()]) ->
        {ok, kpro:struct()} | {error, any()}.
describe_groups(Coordinator, SockOpts, IDs) ->
  with_sock(
    try_connect([Coordinator], SockOpts),
    fun(Pid) ->
        Req = kpro:req(describe_groups_request, 0, [{group_ids, IDs}]),
        #kpro_rsp{ tag = describe_groups_response
                 , vsn = 0
                 , msg = Msg
                 } = request_sync(Pid, Req),
        Groups = kf(groups, Msg),
        {ok, Groups}
    end).

%% @doc Connect to consumer group coordinator broker.
%% Done in steps: 1) connect to any of the given bootstrap ednpoints;
%% 2) send group_coordinator_request to resolve group coordinator endpoint;
%% 3) connect to the resolved endpoint and return the brod_sock pid
%% @end
-spec connect_group_coordinator([endpoint()], sock_opts(), group_id()) ->
        {ok, pid()} | {error, any()}.
connect_group_coordinator(BootstrapEndpoints, SockOpts, GroupId) ->
  case resolve_group_coordinator(BootstrapEndpoints, SockOpts, GroupId) of
    {ok, Endpoint} -> try_connect([Endpoint]);
    {error, Reason} -> {error, Reason}
  end.

%% @doc Send group_coordinator_request to any of the bootstrap endpoints.
%% return resolved coordinator broker endpoint.
%% @end
-spec resolve_group_coordinator([endpoint()], sock_opts(), group_id()) ->
        {ok, endpoint()} | {error, any()}.
resolve_group_coordinator(BootstrapEndpoints, SockOpts, GroupId) ->
  with_sock(
    try_connect(BootstrapEndpoints, SockOpts),
    fun(BootstrapSockPid) ->
        Req = kpro:req(group_coordinator_request, 0, [{group_id, GroupId}]),
        #kpro_rsp{ tag = group_coordinator_response
                 , vsn = 0
                 , msg = Struct
                 } = request_sync(BootstrapSockPid, Req),
        EC = kf(error_code, Struct),
        ?IS_ERROR(EC) andalso erlang:throw(EC),
        Coordinator = kf(coordinator, Struct),
        Host = kf(host, Coordinator),
        Port = kf(port, Coordinator),
        {ok, {binary_to_list(Host), Port}}
    end).

%%%_* Internal functions =======================================================

%% @private
with_sock({ok, Pid}, Fun) ->
  try
    Fun(Pid)
  catch
    throw : Reason ->
      {error, Reason}
  after
    _ = brod_sock:stop(Pid)
  end;
with_sock({error, Reason}, _Fun) ->
  {error, Reason}.

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

%% @private Try to connect to one of the given endpoints.
%% Try next in the list if failed. Return the last failure reason
%% if failed on all endpoints.
%% @end
-spec try_connect([endpoint()], sock_opts(), any()) ->
        {ok, pid()} | {error, any()}.
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

%% @private A fetched batch may contain offsets earlier than the
%% requested begin-offset because the batch might be compressed on
%% kafka side. Here we drop the leading messages.
%% @end
drop_old_messages(_BeginOffset, []) -> [];
drop_old_messages(BeginOffset, [Message | Rest] = All) ->
  case Message#kafka_message.offset < BeginOffset of
    true  -> drop_old_messages(BeginOffset, Rest);
    false -> All
  end.

%% @private Raise an 'error' exception when first argument is not 'true'.
%% The second argument is used as error reason.
%% @end
-spec ok_when(boolean(), any()) -> ok | no_return().
ok_when(true, _) -> ok;
ok_when(_, Reason) -> erlang:error(Reason).

%% @private Make a 'offset_request' message for offset resolution.
%% In kafka protocol, -2 and -1 are semantic 'time' to request for
%% 'earliest' and 'latest' offsets.
%% In brod implementation, -2, -1, 'earliest' and 'latest'
%% are semantic 'offset', this is why often a variable named
%% Offset is used as the Time argument.
%% @end
-spec offsets_request(brod:topic(), brod:partition(),
                      brod:offset_time()) -> kpro:req().
offsets_request(Topic, Partition, TimeOrSemanticOffset) ->
  Time = ensure_integer_offset_time(TimeOrSemanticOffset),
  kpro:offsets_request(_Vsn = 0, Topic, Partition, Time).

%% @private
-spec ensure_integer_offset_time(brod:offset_time()) -> integer().
ensure_integer_offset_time(?OFFSET_EARLIEST)     -> -2;
ensure_integer_offset_time(?OFFSET_LATEST)       -> -1;
ensure_integer_offset_time(T) when is_integer(T) -> T.

%% @private
-spec do_find_leader_in_metadata(kpro:struct(), brod:topic(),
                                 brod:partition()) -> brod:endpoint().
do_find_leader_in_metadata(Msg, Topic, Partition) ->
  Brokers = kf(brokers, Msg),
  [TopicMetadata] = kf(topic_metadata, Msg),
  TopicEC = kf(topic_error_code, TopicMetadata),
  RealTopic = kf(topic, TopicMetadata),
  Partitions = kf(partition_metadata, TopicMetadata),
  RealTopic /= Topic andalso erlang:throw(?EC_UNKNOWN_TOPIC_OR_PARTITION),
  ?IS_ERROR(TopicEC) andalso erlang:throw(TopicEC),
  Id = case find_struct(partition_id, Partition, Partitions) of
         false -> erlang:throw(?EC_UNKNOWN_TOPIC_OR_PARTITION);
         PartitionMetadata -> kf(leader, PartitionMetadata)
       end,
  Id >= 0 orelse erlang:throw(?EC_LEADER_NOT_AVAILABLE),
  Broker = find_struct(node_id, Id, Brokers),
  Host = kf(host, Broker),
  Port = kf(port, Broker),
  {binary_to_list(Host), Port}.

-define(IS_BYTE(I), (I>=0 andalso I<256)).

%% @private
-spec bytes(brod:key() | brod:value() | brod:kv_list()) -> non_neg_integer().
bytes([]) -> 0;
bytes(?undef) -> 0;
bytes(I) when ?IS_BYTE(I) -> 1;
bytes(B) when is_binary(B) -> erlang:size(B);
bytes({K, V}) -> bytes(K) + bytes(V);
bytes([H | T]) -> bytes(H) + bytes(T).

%% @private
-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

%% @private Find kpro struct in array.
%% Return false if no struct matches the given field name and value
%% @end
-spec find_struct(kpro:field_name(), kpro:field_value(), [kpro:struct()]) ->
        false | kpro:struct().
find_struct(_FieldName, _Value, []) -> false;
find_struct(FieldName, Value, [Struct | Rest]) ->
  case kf(FieldName, Struct) =:= Value of
    true  -> Struct;
    false -> find_struct(FieldName, Value, Rest)
  end.

%% @private
-spec request_sync(pid(), kpro:req()) -> kpro:rsp().
request_sync(Pid, Req) ->
  request_sync(Pid, Req, infinity).

%% @private
-spec request_sync(pid(), kpro:req(), infinity | timeout()) -> kpro:rsp().
request_sync(Pid, Req, Timeout) ->
  % brod_sock has a global 'request_timeout' option
  % the socket pid will exit if that one times out
  case brod_sock:request_sync(Pid, Req, Timeout) of
    {ok, Rsp} -> Rsp;
    {error, Reason} -> erlang:throw(Reason)
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
