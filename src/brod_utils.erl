%%%
%%%   Copyright (c) 2014-2018, Klarna Bank AB (publ)
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
        , describe_groups/3
        , epoch_ms/0
        , fetch/5
        , fetch_committed_offsets/3
        , fetch_committed_offsets/4
        , flatten_batches/2
        , get_metadata/1
        , get_metadata/2
        , get_metadata/3
        , group_per_key/1
        , group_per_key/2
        , init_sasl_opt/1
        , is_normal_reason/1
        , is_pid_alive/1
        , list_all_groups/2
        , list_groups/2
        , log/3
        , make_batch_input/2
        , make_fetch_fun/4
        , make_part_fun/1
        , os_time_utc_str/0
        , parse_rsp/1
        , request_sync/2
        , request_sync/3
        , resolve_offset/4
        , resolve_offset/5
        ]).

-include("brod_int.hrl").

-type req_fun() :: fun((offset(), kpro:count()) -> kpro:req()).
-type fetch_fun() :: fun((offset()) -> {ok, {offset(), [brod:message()]}} |
                                       {error, any()}).
-type connection() :: kpro:connection().
-type conn_config() :: brod:conn_config().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type endpoint() :: brod:endpoint().
-type offset_time() :: brod:offset_time().
-type group_id() :: brod:group_id().

%%%_* APIs =====================================================================

%% @doc Try to connect to any of the bootstrap nodes and fetch metadata
%% for all topics
-spec get_metadata([endpoint()]) -> {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts) ->
  get_metadata(Hosts, all).

%% @doc Try to connect to any of the bootstrap nodes and fetch metadata
%% for the given topics
-spec get_metadata([endpoint()], all | [topic()]) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics) ->
  get_metadata(Hosts, Topics, _ConnCfg = []).

%% @doc Try to connect to any of the bootstrap nodes using the given
%% connection options and fetch metadata for the given topics.
-spec get_metadata([endpoint()], all | [topic()], conn_config()) ->
        {ok, kpro:struct()} | {error, any()}.
get_metadata(Hosts, Topics, ConnCfg) ->
  with_conn(Hosts, ConnCfg,
            fun(Pid) ->
                Request = brod_kafka_request:metadata(Pid, Topics),
                request_sync(Pid, Request)
            end).

%% @doc Resolve timestamp to real offset.
-spec resolve_offset([endpoint()], topic(), partition(),
                     offset_time(), conn_config()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Hosts, Topic, Partition, Time, ConnCfg) ->
  with_conn(
    kpro:connect_partition_leader(Hosts, ConnCfg, Topic, Partition),
    fun(Pid) -> resolve_offset(Pid, Topic, Partition, Time) end).

%% @doc Resolve timestamp or semantic offset to real offset.
%% The give pid should be the connection to partition leader broker.
-spec resolve_offset(pid(), topic(), partition(), offset_time()) ->
        {ok, offset()} | {error, any()}.
resolve_offset(Pid, Topic, Partition, Time) ->
  Req = brod_kafka_request:list_offsets(Pid, Topic, Partition, Time),
  case request_sync(Pid, Req) of
    {ok, #{error_code := EC}} when ?IS_ERROR(EC) ->
      {error, EC};
    {ok, #{offset := Offset}} ->
      {ok, Offset};
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Check terminate reason for a gen_server implementation
is_normal_reason(normal)        -> true;
is_normal_reason(shutdown)      -> true;
is_normal_reason({shutdown, _}) -> true;
is_normal_reason(_)             -> false.

is_pid_alive(Pid) ->
  is_pid(Pid) andalso is_process_alive(Pid).

%% @doc Get now timestamp, and format as UTC string.
-spec os_time_utc_str() -> string().
os_time_utc_str() ->
  Ts = os:timestamp(),
  {{Y, M, D}, {H, Min, Sec}} = calendar:now_to_universal_time(Ts),
  {_, _, Micro} = Ts,
  S = io_lib:format("~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
                    [Y, M, D, H, Min, Sec, Micro]),
  lists:flatten(S).

%% @doc Milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).
-spec epoch_ms() -> kpro:msg_ts().
epoch_ms() ->
  {Mega, S, Micro} = os:timestamp(),
  (((Mega * 1000000)  + S) * 1000) + Micro div 1000.

%% @doc simple wrapper around error_logger.
%% NOTE: keep making MFA calls to error_logger to
%%       1. allow logging libraries such as larger parse_transform
%%       2. be more xref friendly
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
  ok_when(is_binary(Topic) andalso size(Topic) > 0, {bad_topic, Topic}).

%% @doc Make a flat message list from decoded batch list.
-spec flatten_batches(offset(), [kpro:batch()]) -> [kpro:message()].
flatten_batches(BeginOffset, Batches) ->
  MsgList = lists:append([Msgs || {_Meta, Msgs} <- Batches]),
  drop_old_messages(BeginOffset, MsgList).

%% @doc Fetch a single message set from the given topic-partition.
-spec fetch(connection() | {[endpoint()], conn_config()},
            topic(), partition(), offset(), brod:fetch_opts()) ->
              {ok, {offset(), [brod:message()]}} | {error, any()}.
fetch({Hosts, ConnCfg}, Topic, Partition, Offset, Opts) ->
  with_conn(
    kpro:connect_partition_leader(Hosts, ConnCfg, Topic, Partition),
    fun(Conn) -> fetch(Conn, Topic, Partition, Offset, Opts) end);
fetch(Conn, Topic, Partition, Offset, Opts) ->
  Fetch = make_fetch_fun(Conn, Topic, Partition, Opts),
  Fetch(Offset).

%% @doc Make a fetch function which should expand `max_bytes' when
%% it is not big enough to fetch one signle message.
-spec make_fetch_fun(pid(), topic(), partition(), brod:fetch_opts()) ->
        fetch_fun().
make_fetch_fun(Conn, Topic, Partition, FetchOpts) ->
  WaitTime = maps:get(max_wait_time, FetchOpts, 1000),
  MinBytes = maps:get(min_bytes, FetchOpts, 1),
  MaxBytes = maps:get(max_bytes, FetchOpts, 1 bsl 20),
  ReqFun = make_req_fun(Conn, Topic, Partition, WaitTime, MinBytes),
  fun(Offset) -> fetch(Conn, ReqFun, Offset, MaxBytes) end.

-spec make_part_fun(brod:partitioner()) -> brod:partition_fun().
make_part_fun(random) ->
  fun(_, PartitionCount, _, _) ->
      {ok, rand:uniform(PartitionCount) -1}
  end;
make_part_fun(hash) ->
  fun(_, PartitionCount, Key, _) ->
      {ok, erlang:phash2(Key) rem PartitionCount}
  end;
make_part_fun(F) -> F.

%% @doc Hide sasl plain password in an anonymous function to avoid
%% the plain text being dumped to crash logs
-spec init_sasl_opt(brod:client_config()) -> brod:client_config().
init_sasl_opt(Config) ->
  case get_sasl_opt(Config) of
    {Mechanism, User, Pass} when Mechanism =/= callback ->
      replace_prop(sasl, {Mechanism, User, fun() -> Pass end}, Config);
    _Other ->
      Config
  end.

%% @doc Fetch committed offsets for the given topics in a consumer group.
%% 1. try find out the group coordinator broker from the bootstrap hosts
%% 2. send `offset_fetch' request and wait for response.
%% If Topics is an empty list, fetch offsets for all topics in the group
-spec fetch_committed_offsets([endpoint()], conn_config(),
                              group_id(), [topic()]) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(BootstrapEndpoints, ConnCfg, GroupId, Topics) ->
  Args = #{type => group, id => GroupId},
  with_conn(
    kpro:connect_coordinator(BootstrapEndpoints, ConnCfg, Args),
    fun(Pid) -> do_fetch_committed_offsets(Pid, GroupId, Topics) end).

%% @doc Fetch commited offsts for the given topics in a consumer group.
%% 1. Get broker endpoint by calling
%%    `brod_client:get_group_coordinator'
%% 2. Establish a connecton to the discovered endpoint.
%% 3. send `offset_fetch' request and wait for response.
%% If Topics is an empty list, fetch offsets for all topics in the group
-spec fetch_committed_offsets(brod:client(), group_id(), [topic()]) ->
        {ok, [kpro:struct()]} | {error, any()}.
fetch_committed_offsets(Client, GroupId, Topics) ->
  case brod_client:get_group_coordinator(Client, GroupId) of
    {ok, {Endpoint, ConnCfg}} ->
      case kpro:connect(Endpoint, ConnCfg) of
        {ok, Conn} ->
          do_fetch_committed_offsets(Conn, GroupId, Topics);
        {error, Reason} ->
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

-spec get_sasl_opt(brod:client_config()) -> sasl_opt().
get_sasl_opt(Config) ->
  case proplists:get_value(sasl, Config) of
    ?undef -> {sasl, ?undef};
    {callback, Module, Args} ->
      %% Module should implement kpro_auth_backend behaviour
      {callback, Module, Args};
    {Mechanism, File} when is_list(File) orelse is_binary(File) ->
      {User, Pass} = read_sasl_file(File),
      {Mechanism, User, Pass};
    Other ->
      Other
  end.

%% With a connection to the group coordinator broker,
%% send `offset_fetch' and wait for response.
-spec do_fetch_committed_offsets(brod:client_id() | pid(),
                                 group_id(), [topic()]) ->
        {ok, [kpro:struct()]} | {error, any()}.
do_fetch_committed_offsets(Conn, GroupId, Topics) when is_pid(Conn) ->
  Req = brod_kafka_request:offset_fetch(Conn, GroupId, Topics),
  case request_sync(Conn, Req) of
    {ok, Msg} ->
      {ok, kf(responses, Msg)};
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Fetch a message-set. If the given MaxBytes is not enough to fetch a
%% single message, expand it to fetch exactly one message
-spec fetch(connection(), req_fun(), offset(), kpro:count()) ->
               {ok, {offset(), [brod:message()]}} | {error, any()}.
fetch(Conn, ReqFun, Offset, MaxBytes) ->
  Request = ReqFun(Offset, MaxBytes),
  case request_sync(Conn, Request, infinity) of
    {ok, #{error_code := ErrorCode}} when ?IS_ERROR(ErrorCode) ->
      {error, ErrorCode};
    {ok, #{batches := ?incomplete_batch(Size)}} ->
      fetch(Conn, ReqFun, Offset, Size);
    {ok, #{header := Header, batches := Batches0}} ->
      HighWmOffset = kpro:find(high_watermark, Header),
      Batches = flatten_batches(Offset, Batches0),
      case Offset < HighWmOffset andalso Batches =:= [] of
        true -> fetch(Conn, ReqFun, Offset + 1, MaxBytes);
        false -> {ok, {HighWmOffset, Batches}}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc List all groups in the given cluster.
%% NOTE: Exception if failed against any of the coordinator brokers.
-spec list_all_groups([endpoint()], conn_config()) ->
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
-spec list_groups(endpoint(), conn_config()) ->
        {ok, [brod:cg()]} | {error, any()}.
list_groups(Endpoint, ConnCfg) ->
  with_conn([Endpoint], ConnCfg,
    fun(Pid) ->
      Request = brod_kafka_request:list_groups(Pid),
      case request_sync(Pid, Request) of
        {ok, Groups0} ->
          Groups =
            lists:map(
              fun(Struct) ->
                  Id = kf(group_id, Struct),
                  Type = kf(protocol_type, Struct),
                  #brod_cg{ id = Id
                          , protocol_type = Type
                          }
              end, Groups0),
          {ok, Groups};
        {error, Reason} ->
          {error, Reason}
      end
    end).

%% @doc Send describe_groups_request and wait for describe_groups_response.
-spec describe_groups(endpoint(), conn_config(), [brod:group_id()]) ->
        {ok, kpro:struct()} | {error, any()}.
describe_groups(CoordinatorEndpoint, ConnCfg, IDs) ->
  with_conn([CoordinatorEndpoint], ConnCfg,
    fun(Pid) ->
        Req = kpro:make_request(describe_groups, 0, [{group_ids, IDs}]),
        request_sync(Pid, Req)
    end).

-define(IS_BYTE(I), (I>=0 andalso I<256)).

%% @doc Return message set size in number of bytes.
%% NOTE: This does not include the overheads of encoding protocol.
%% such as magic bytes, attributes, and length tags etc.
-spec bytes(bord:batch_input()) -> non_neg_integer().
bytes(Msgs) ->
  F = fun(#{key := Key, value := Value} = Msg, Acc) ->
          %% this is message from a magic v2 batch
          %% last 8 bytes are for timestamp although it is encoded as varint
          %% which in best case scenario takes only 1 byte.
          HeaderSize = lists:foldl(
                         fun({K, V}, AccH) ->
                             size(K) + size(V) + AccH
                         end, 0, maps:get(headers, Msg, [])),
          size(Key) + size(Value) + HeaderSize + 8 + Acc
      end,
  lists:foldl(F, 0, Msgs).

%% @doc Group values per-key in a key-value list.
-spec group_per_key([{Key, Val}]) -> [{Key, [Val]}]
        when Key :: term(), Val :: term().
group_per_key(List) ->
  lists:foldl(
    fun({Key, Value}, Acc) ->
        orddict:append_list(Key, [Value], Acc)
    end, [], List).

%% @doc Group values per-key for the map result of a list.
-spec group_per_key(fun((term()) -> {Key, Val}), [term()]) -> [{Key, [Val]}]
        when Key :: term(), Val :: term().
group_per_key(MapFun, List) ->
  group_per_key(lists:map(MapFun, List)).

%% @doc Parse decoded kafka response (`#kpro_rsp{}') into a more generic
%% representation.
%% Return `ok' if it is a trivial 'ok or not' response without data fields
%% Return `{ok, Result}' for some of the APIs when no error-code found in
%% response. Result could be a transformed representation of response message
%% body `#kpro_rsp.msg' or the response body itself.
%% For some APIs, it returns `{error, CodeOrMessage}' when error-code is not
%% `no_error' in the message body.
%% NOTE: Not all error codes are interpreted as `{error, CodeOrMessage}' tuple.
%%       for some of the complex response bodies, error-codes are retained
%%       for caller to parse.
-spec parse_rsp(kpro:rsp()) -> ok | {ok, term()} | {error, any()}.
parse_rsp(#kpro_rsp{api = API, vsn = Vsn, msg = Msg}) ->
  try parse(API, Vsn, Msg) of
    ok -> ok;
    Result -> {ok, Result}
  catch
    throw : ErrorCodeOrMessage ->
      {error, ErrorCodeOrMessage}
  end.

-spec request_sync(connection(), kpro:req()) ->
        ok | {ok, term()} | {error, any()}.
request_sync(Conn, Req) ->
  request_sync(Conn, Req, infinity).

-spec request_sync(connection(), kpro:req(), infinity | timeout()) ->
        ok | {ok, term()} | {error, any()}.
request_sync(Conn, #kpro_req{ref = Ref} = Req, Timeout) ->
  % kpro_connection has a global 'request_timeout' option
  % the connection pid will exit if that one times out
  case kpro:request_sync(Conn, Req, Timeout) of
    {ok, #kpro_rsp{ref = Ref} = Rsp} -> parse_rsp(Rsp);
    {error, Reason} -> {error, Reason}
  end.

%% @doc Make batch input for kafka_protocol.
-spec make_batch_input(brod:key(), brod:value()) -> brod:batch_input().
make_batch_input(Key, Value) ->
  case is_batch(Value) of
    true  -> unify_batch(Value);
    false -> [unify_msg(make_msg_input(Key, Value))]
  end.

%%%_* Internal functions =======================================================

%% Make a function to build fetch requests.
%% The function takes offset and max_bytes as input as these two parameters
%% are varient when continuously polling a specific topic-partition.
-spec make_req_fun(connection(), topic(), partition(),
                   kpro:wait(), kpro:count()) -> req_fun().
make_req_fun(Conn, Topic, Partition, WaitTime, MinBytes) ->
  fun(Offset, MaxBytes) ->
      brod_kafka_request:fetch(Conn, Topic, Partition, Offset,
                               WaitTime, MinBytes, MaxBytes)
  end.

%% Parse fetch response into a more user-friendly representation.
-spec parse_fetch_rsp(kpro:struct()) -> map().
parse_fetch_rsp(Msg) ->
  EC1 = kpro:find(error_code, Msg, ?no_error),
  SessionID = kpro:find(session_id, Msg, 0),
  {Header, Batches, EC2} =
    case kpro:find(responses, Msg) of
      [] ->
        %% a session init without data
        {undefined, [], ?no_error};
      _ ->
        PartitionRsp = get_partition_rsp(Msg),
        HeaderX = kpro:find(partition_header, PartitionRsp),
        throw_error_code([HeaderX]),
        Records = kpro:find(record_set, PartitionRsp),
        ECx = kpro:find(error_code, HeaderX),
        {HeaderX, kpro:decode_batches(Records), ECx}
    end,
  ErrorCode = case EC2 =:= ?no_error of
                true  -> EC1;
                false -> EC2
              end,
  case ?IS_ERROR(ErrorCode) of
    true -> erlang:throw(ErrorCode);
    false -> #{ session_id => SessionID
              , header => Header
              , batches => Batches
              }
  end.

get_partition_rsp(Struct) ->
  [TopicRsp] = kpro:find(responses, Struct),
  [PartitionRsp] = kpro:find(partition_responses, TopicRsp),
  PartitionRsp.

-spec replace_prop(term(), term(), proplists:proplist()) ->
        proplists:proplist().
replace_prop(Key, Value, PropL0) ->
  PropL = proplists:delete(Key, PropL0),
  [{Key, Value} | PropL].

%% Read a regular file, assume it has two lines:
%% First line is the sasl-plain username
%% Second line is the password
-spec read_sasl_file(file:name_all()) -> {binary(), binary()}.
read_sasl_file(File) ->
  {ok, Bin} = file:read_file(File),
  Lines = binary:split(Bin, <<"\n">>, [global]),
  [User, Pass] = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
  {User, Pass}.

%% A fetched batch may contain offsets earlier than the
%% requested begin-offset because the batch might be compressed on
%% kafka side. Here we drop the leading messages.
drop_old_messages(_BeginOffset, []) -> [];
drop_old_messages(BeginOffset, [Message | Rest] = All) ->
  case Message#kafka_message.offset < BeginOffset of
    true  -> drop_old_messages(BeginOffset, Rest);
    false -> All
  end.

%% Raise an 'error' exception when first argument is not 'true'.
%% The second argument is used as error reason.
-spec ok_when(boolean(), any()) -> ok | no_return().
ok_when(true, _) -> ok;
ok_when(_, Reason) -> erlang:error(Reason).

-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

with_conn({ok, Pid}, Fun) ->
  try
    Fun(Pid)
  after
    kpro:close_connection(Pid)
  end;
with_conn({error, Reason}, _Fun) ->
  {error, Reason}.

with_conn(Endpoints, ConnCfg, Fun) when is_list(ConnCfg) ->
  with_conn(Endpoints, maps:from_list(ConnCfg), Fun);
with_conn(Endpoints, ConnCfg, Fun) ->
  kpro_brokers:with_connection(Endpoints, ConnCfg, Fun).

parse(produce, _Vsn,  Msg) ->
  kpro:find(base_offset, get_partition_rsp(Msg));
parse(fetch, _Vsn, Msg) ->
  parse_fetch_rsp(Msg);
parse(list_offsets, _, Msg) ->
  case get_partition_rsp(Msg) of
    #{offsets := []} = M -> M#{offset => -1};
    #{offsets := [Offset]} = M -> M#{offset => Offset};
    #{offset := _} = M -> M
  end;
parse(metadata, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_metadata, Msg)),
  Msg;
parse(find_coordinator, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(join_group, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(heartbeat, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(leave_group, _, Msg) ->
  ok = throw_error_code([Msg]);
parse(sync_group, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(describe_groups, _, Msg) ->
  %% return groups
  Groups = kpro:find(groups, Msg),
  ok = throw_error_code(Groups),
  Groups;
parse(list_groups, _, Msg) ->
  %% return groups
  ok = throw_error_code([Msg]),
  kpro:find(groups, Msg);
parse(create_topics, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_errors, Msg));
parse(delete_topics, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_error_codes, Msg));
parse(init_producer_id, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(create_partitions, _, Msg) ->
  ok = throw_error_code(kpro:find(topic_errors, Msg));
parse(end_txn, _, Msg) ->
  ok = throw_error_code([Msg]);
parse(describe_acls, _, Msg) ->
  ok = throw_error_code([Msg]),
  Msg;
parse(create_acls, _, Msg) ->
  ok = throw_error_code(kpro:find(creation_responses, Msg));
parse(_API, _Vsn, Msg) ->
  %% leave it to the caller to parse:
  %% offset_commit
  %% offset_fetch
  %% sasl_handshake
  %% api_versions
  %% delete_records
  %% add_partitions_to_txn
  %% txn_offset_commit
  %% delete_acls
  %% describe_acls
  %% describe_configs
  %% alter_configs
  %% alter_replica_log_dirs
  %% describe_log_dirs
  %% sasl_authenticate
  %% create_partitions
  %% create_delegation_token
  %% renew_delegation_token
  %% expire_delegation_token
  %% describe_delegation_token
  %% delete_groups
  Msg.

%% This function takes a list of kpro structs,
%% return ok if all structs have 'no_error' as error code.
%% Otherwise throw an exception with the first error.
throw_error_code([]) -> ok;
throw_error_code([Struct | Structs]) ->
  EC = kpro:find(error_code, Struct),
  case ?IS_ERROR(EC) of
    true ->
      Err = kpro:find(error_message, Struct, EC),
      erlang:throw(Err);
    false ->
      throw_error_code(Structs)
  end.

-spec make_msg_input(brod:key(), brod:value()) -> brod:msg_input().
make_msg_input(Key, {Ts, Value}) when is_integer(Ts) ->
  #{ts => Ts, key => Key, value => Value};
make_msg_input(Key, M) when is_map(M) ->
  ensure_ts(ensure_key(M, Key));
make_msg_input(Key, Value) ->
  ensure_ts(#{key => Key, value => Value}).

%% put 'Key' arg as 'key' field in the 'Value' map
ensure_key(#{key := _} = M, _) -> M;
ensure_key(M, Key) -> M#{key => Key}.

%% take current timestamp if 'ts' field is not found in the 'Value' map
ensure_ts(#{ts := _} = M) -> M;
ensure_ts(M) -> M#{ts => kpro_lib:now_ts()}.

-spec unify_batch([?KV(_, _) | ?TKV(_, _, _) | map()]) ->
        brod:batch_input().
unify_batch(BatchInput) ->
  F = fun(M, Acc) -> [unify_msg(M) | Acc] end,
  lists:reverse(foldl_batch(F, [], BatchInput)).

bin(?undef) -> <<>>;
bin(X) -> iolist_to_binary(X).

unify_msg(?KV(K, V)) ->
  #{ts => kpro_lib:now_ts(), key => bin(K), value => bin(V)};
unify_msg(?TKV(T, K, V)) ->
  #{ts => T, key => bin(K), value => bin(V)};
unify_msg(M) when is_map(M) ->
  M#{key => bin(maps:get(key, M, <<>>)),
     value => bin(maps:get(value, M, <<>>)),
     headers => lists:map(fun({K, V}) -> {bin(K), bin(V)} end,
                          maps:get(headers, M, []))
    }.

%% Get nested kv-list.
nested(?KV(_K, [Msg | _] = Nested)) when is_tuple(Msg) -> Nested;
nested(?TKV(_T, _K, [Msg | _] = Nested)) when is_tuple(Msg) -> Nested;
nested(_Msg) -> false.

foldl_batch(_Fun, Acc, []) -> Acc;
foldl_batch(Fun, Acc, [Msg | Rest]) ->
  NewAcc = case nested(Msg) of
             false -> Fun(Msg, Acc);
             Nested -> foldl_batch(Fun, Acc, Nested)
           end,
  foldl_batch(Fun, NewAcc, Rest).

is_batch([M | _]) when is_map(M) -> true;
is_batch([T | _]) when is_tuple(T) -> true;
is_batch(_) -> false.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
