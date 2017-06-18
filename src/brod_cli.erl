%%%
%%%   Copyright (c) 2017, Klarna AB
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

-module(brod_cli).

-ifdef(BROD_CLI).

-export([main/2]).

-include("brod_int.hrl").

-define(CLIENT, brod_cli_client).

-define(MAIN_DOC, "usage:
  brod -h|--help
  brod -v|--version
  brod <command> [options] [-h|--help] [--verbose|--debug]

commands:
  meta:    Inspect topic metadata
  offset:  Inspect offsets
  fetch:   Fetch messages
  send:    Produce messages
  pipe:    Pipe file or stdin as messages to kafka
  groups:  List or describe consumer group
  commits: List consumer group offset commits
").

%% NOTE: bad indentation at the first line is intended
-define(COMMAND_COMMON_OPTIONS,
"  --ssl                  Use TLS, validate server using trusted CAs
  --cacertfile=<cacert>  Use TLS, validate server using the given certificate
  --certfile=<certfile>  Client certificate in case client authentication
                         is enabled in borkers
  --keyfile=<keyfile>    Client private key in case client authentication
                         is enabled in borkers
  --sasl-plain=<file>    Tell brod to use username/password stored in the
                         given file, the file should have username and
                         password in two lines.
  --ebin-paths=<dirs>    Comma separated directory names for extra beams,
                         This is to support user compiled message formatters
"
).

-define(META_CMD, "meta").
-define(META_DOC, "usage:
  brod meta [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  -t,--topic=<topic>     Topic name [default: *]
  -T,--text              Print metadata as aligned texts (default)
  -J,--json              Print metadata as JSON object
  -L,--list              List topics, no partition details,
                         Applicable only for --text option
  -U,--under-replicated  Display only under-replicated partitions
"
?COMMAND_COMMON_OPTIONS
"Text output schema (out of sync replicas are marked with *):
brokers <count>:
  <broker-id>: <endpoint>
topics <count>:
  <name> <count>: [[ERROR] [<reason>]]
    <partition>: <leader-broker-id> (replicas[*]...) [<error-reason>]
"
).

-define(OFFSET_CMD, "offset").
-define(OFFSET_DOC, "usage:
  brod offset [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  -t,--topic=<topic>     Topic name
  -p,--partition=<parti> Partition number
  -T,--time=<time>       Unix epoch (in milliseconds) of the correlated offset
                         to fetch. Special values:
                           'latest' or -1 for latest offset
                           'earliest' or -2 for earliest offset
                         [default: latest]
"
?COMMAND_COMMON_OPTIONS
).

-define(FETCH_CMD, "fetch").
-define(FETCH_DOC, "usage:
  brod fetch [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  -t,--topic=<topic>     Topic name
  -p,--partition=<parti> Partition number
  -o,--offset=<offset>   Offset to start fetching from
                           latest: From the latest offset (not last)
                           earliest: From earliest offset (first)
                           last: From offset (latest - 1)
                           <integer>: From a specific offset
                         [default: last]
  -c,--count=<count>     Number of messages to fetch (-1 as infinity)
                         [default: 1]
  -w,--wait=<seconds>    Time in seconds to wait for one message set
                         [default: 5s]
  --kv-deli=<deli>       Delimiter for offset, key and value output [default: :]
  --msg-deli=<deli>      Delimiter between messages. [default: \\n]
  --max-bytes=<bytes>    Max number of bytes kafka should try to accumulate
                         within the --wait time
                         [default: 1K]
  --fmt=<fmt>            Output format. Assume keys and values are utf8 strings
                         v:     Print 'V <msg-deli>'
                         kv:    Print 'K <kv-deli> V <msg-deli>'
                         okv:   Print 'O <kv-deli> K <kv-deli> V <msg-deli>'
                         eterm: Pretty print tuple '{Offse, Key, Value}.'
                                to a consultable Erlang term format.
                         Fun:   An anonymous Erlang function literal to be
                                evaluated for each message, the function is
                                expected to be of below spec:
                                fun((Offset, Key, Value) -> ok | iodata()).
                                Print nothing if 'ok' is returned otherwise
                                print the return value. [default: v]
"
?COMMAND_COMMON_OPTIONS
"NOTE: Reaching either --count or --wait limit will cause script to exit"
).

-define(SEND_CMD, "send").
-define(SEND_DOC, "usage:
  brod send [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  -t,--topic=<topic>     Topic name
  -p,--partition=<parti> Partition number [default: 0]
                         Special values:
                           random: randomly pick a partition
                           hash: hash key to a partition
  -k,--key=<key>         Key to produce [default: null]
  -v,--value=<value>     Value to produce. Special values:
                           null: No payload
                           @/path/to/file: Send a whole file as payload
  --acks=<acks>          Required acks. Supported values:
                           all or -1: Require acks from all in-sync replica
                           1: Require acks from only partition leader
                           0: Require no acks
                         [default: all]
  --ack-timeout=<time>   How long the partition leader should wait for replicas
                         to ack before sending response to producer
                         The value can be an integer to indicate number of
                         milliseconds or followed by s/m to indicate seconds
                         or minutes [default: 10s]
  --compression=<compre> Supported values: none / gzip / snappy
                         [default: none]
"
?COMMAND_COMMON_OPTIONS
).

-define(PIPE_CMD, "pipe").
-define(PIPE_DOC, "usage:
  brod pipe [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  -t,--topic=<topic>     Topic name
  -p,--partition=<parti> Partition number [default: 0]
                         Special values:
                           random: randomly pick a partition
                           hash: hash key to a partition
  -s,--source=<source>   Data source. Special value:
                           stdin: Reads messages from standard-input
                           @path/to/file: Reads from file
                         [default: stdin]
  --prompt               Applicable when --source is stdin, enable input prompt
  --no-eof-exit          Do not exit when reaching EOF
  --tail                 Applicable when --source is a file
                         brod will start from EOF and keep tailing for new bytes
  --msg-deli=<msg-deli>  Message delimiter
                         NOTE: A message is always delimited when reaching EOF
                         [default: \\n]
  --kv-deli=<kv-deli>    Key-Value delimiter.
                         when not provided, messages are produced with
                         only value, key is set to null. [default: none]
  --blk-size=<size>      Block size (bytes) when reading bytes from a file.
                         Applicable when --source is file and --msg-deli is
                         not \\n.  [default: 1M]
  --acks=<acks>          Required acks. [default: all]
                         Supported values:
                           all or -1: Require acks from all in-sync replica
                           1: Require acks from only partition leader
                           0: Require no acks
  --ack-timeout=<time>   How long the partition leader should wait for replicas
                         to ack before sending response to producer
                         not applicable when --acks is not 'all'
                         [default: 10s]
  --max-linger-ms=<ms>   Max ms for messages to linger in buffer [default: 200]
  --max-linger-cnt=<N>   Max messages to linger in buffer [default: 100]
  --max-batch=<bytes>    Max size for one message-set (before compression)
                         The value can be either an integer to indicate bytes
                         or followed by K/M to indicate KBytes or MBytes
                         [default: 1M]
  --compression=<compr>  Supported values: none/gzip/snappy [default: none]
"
?COMMAND_COMMON_OPTIONS
"NOTE: When --source is path/to/file, it by default reads from BOF
      unless --tail is given.
"
).

-define(GROUPS_CMD, "groups").
-define(GROUPS_DOC, "usage:
  brod groups [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  --describe             Describe group status details
  --ids=<group-id>       Comma separated group IDs to describe [default: all]
"
?COMMAND_COMMON_OPTIONS
).


-define(COMMITS_CMD, "commits").
-define(COMMITS_DOC, "usage:
  brod commits [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
                         [default: localhost:9092]
  --id=<group-id>        Comma separated group IDs to describe
"
?COMMAND_COMMON_OPTIONS
).

-define(DOCS,
        [ {?META_CMD,    ?META_DOC}
        , {?OFFSET_CMD,  ?OFFSET_DOC}
        , {?SEND_CMD,    ?SEND_DOC}
        , {?FETCH_CMD,   ?FETCH_DOC}
        , {?PIPE_CMD,    ?PIPE_DOC}
        , {?GROUPS_CMD,  ?GROUPS_DOC}
        , {?COMMITS_CMD, ?COMMITS_DOC}
        ]).

-define(LOG_LEVEL_QUIET, 0).
-define(LOG_LEVEL_VERBOSE, 1).
-define(LOG_LEVEL_DEBUG, 2).

-type log_level() :: non_neg_integer().

-type command() :: string().

main(["-h" | _], _Stop) ->
  print(?MAIN_DOC);
main(["--help" | _], _Stop) ->
  print(?MAIN_DOC);
main(["-v" | _], _Stop) ->
  print_version();
main(["--version" | _], _Stop) ->
  print_version();
main(["-" ++ _ = Arg | _], Stop) ->
  logerr("Unknown option: ~s\n", [Arg]),
  print(?MAIN_DOC),
  erlang:Stop(?LINE);
main([Command | _] = Args, Stop) ->
  case lists:keyfind(Command, 1, ?DOCS) of
    {_, Doc} ->
      main(Command, Doc, Args, Stop);
    false ->
      logerr("Unknown command: ~s\n", [Command]),
      print(?MAIN_DOC),
      erlang:Stop(?LINE)
  end;
main(_, Stop) ->
  print(?MAIN_DOC),
  erlang:Stop(?LINE).

%% @private
-spec main(command(), string(), [string()], halt | exit) -> _ | no_return().
main(Command, Doc, Args0, Stop) ->
  IsHelp = lists:member("--help", Args0) orelse lists:member("-h", Args0),
  IsVerbose = lists:member("--verbose", Args0),
  IsDebug = lists:member("--debug", Args0),
  Args = Args0 -- ["--verbose", "--debug"],
  LogLevels = [{IsDebug, ?LOG_LEVEL_DEBUG},
               {IsVerbose, ?LOG_LEVEL_VERBOSE},
               {true, ?LOG_LEVEL_QUIET}],
  {true, LogLevel} = lists:keyfind(true, 1, LogLevels),
  erlang:put(brod_cli_log_level, LogLevel),
  case IsHelp of
    true ->
      print(Doc);
    false ->
      main(Command, Doc, Args, Stop, LogLevel)
  end.

%% @private
-spec main(command(), string(), [string()], halt | exit,
           log_level()) -> _ | no_return().
main(Command, Doc, Args, Stop, LogLevel) ->
  ParsedArgs =
    try
      docopt:docopt(Doc, Args, [debug || LogLevel =:= ?LOG_LEVEL_DEBUG])
    catch
      C1 : E1 ->
        Stack1 = erlang:get_stacktrace(),
        verbose("~p:~p\n~p\n", [C1, E1, Stack1]),
        print(Doc),
        erlang:Stop(?LINE)
    end,
  case LogLevel =:= ?LOG_LEVEL_QUIET of
    true ->
      _ = error_logger:logfile({open, 'brod.log'}),
      _ = error_logger:tty(false);
    false ->
      ok
  end,
  {ok, _} = application:ensure_all_started(sasl),
  {ok, _} = application:ensure_all_started(brod),
  try
    Brokers = parse(ParsedArgs, "--brokers", fun parse_brokers/1),
    SockOpts = parse_sock_opts(ParsedArgs),
    Paths = parse(ParsedArgs, "--ebin-paths", fun parse_paths/1),
    ok = code:add_pathsa(Paths),
    verbose("sock opts: ~p\n", [SockOpts]),
    run(Command, Brokers, SockOpts, ParsedArgs)
  catch
    throw : Reason when is_binary(Reason) ->
      %% invalid options etc.
      logerr([Reason, "\n"]),
      logerr(Doc),
      erlang:Stop(?LINE);
    C2 : E2 ->
      %% crashed on desensive matches etc.
      Stack2 = erlang:get_stacktrace(),
      logerr("~p:~p\n~p\n", [C2, E2, Stack2]),
      erlang:Stop(?LINE)
  end.

%% @private
run(?META_CMD, Brokers, Topic, SockOpts, Args) ->
  Topics = case Topic of
             <<"*">> -> []; %% fetch all topics
             _       -> [Topic]
           end,
  IsJSON = parse(Args, "--json", fun parse_boolean/1),
  IsText = parse(Args, "--text", fun parse_boolean/1),
  Format = kf(true, [ {IsJSON, json}
                    , {IsText, text}
                    , {true, text}
                    ]),
  IsList = parse(Args, "--list", fun parse_boolean/1),
  IsUrp = parse(Args, "--under-replicated", fun parse_boolean/1),
  {ok, Metadata} = brod:get_metadata(Brokers, Topics, SockOpts),
  format_metadata(Metadata, Format, IsList, IsUrp);
run(?OFFSET_CMD, Brokers, Topic, SockOpts, Args) ->
  Partition = parse(Args, "--partition", fun int/1), %% not parse_partition/1
  Time = parse(Args, "--time", fun parse_offset_time/1),
  case brod:resolve_offset(Brokers, Topic, Partition, Time, SockOpts) of
    {ok, Offset} ->
      print(integer_to_list(Offset));
    {error, not_found} ->
      print("no such offset")
  end;
run(?FETCH_CMD, Brokers, Topic, SockOpts, Args) ->
  Partition = parse(Args, "--partition", fun int/1), %% not parse_partition/1
  Count0 = parse(Args, "--count", fun int/1),
  Offset0 = parse(Args, "--offset", fun parse_offset_time/1),
  Wait = parse(Args, "--wait", fun parse_timeout/1),
  KvDeli = parse(Args, "--kv-deli", fun parse_delimiter/1),
  MsgDeli = parse(Args, "--msg-deli", fun parse_delimiter/1),
  FmtFun = parse(Args, "--fmt",
                 fun(FmtOption) ->
                     parse_fmt(FmtOption, KvDeli, MsgDeli)
                 end),
  MaxBytes = parse(Args, "--max-bytes", fun parse_size/1),
  {ok, Sock} = brod:connect_leader(Brokers, Topic, Partition, SockOpts),
  Offset = resolve_begin_offset(Sock, Topic, Partition, Offset0),
  FetchFun = brod_utils:make_fetch_fun(Sock, Topic, Partition,
                                       Wait, _MinBytes = 1, MaxBytes),
  Count = case Count0 < 0 of
            true -> 1000000000; %% as if an infinite loop
            false -> Count0
          end,
  fetch_loop(FmtFun, FetchFun, Offset, Count);
run(?SEND_CMD, Brokers, Topic, SockOpts, Args) ->
  Partition = parse(Args, "--partition", fun parse_partition/1),
  Acks = parse(Args, "--acks", fun parse_acks/1),
  AckTimeout = parse(Args, "--ack-timeout", fun parse_timeout/1),
  Compression = parse(Args, "--compression", fun parse_compression/1),
  Key = parse(Args, "--key", fun("null") -> <<"">>;
                                (K) -> bin(K)
                             end),
  Value0 = parse(Args, "--value", fun("null") -> <<"">>;
                                     ("@" ++ F) -> {file, F};
                                     (V) -> bin(V)
                                  end),
  Value =
    case Value0 of
      {file, File} ->
        {ok, Bin} = file:read_file(File),
        Bin;
      <<_/binary>> ->
        Value0
    end,
  ProducerConfig =
    [ {required_acks, Acks}
    , {ack_timeout, AckTimeout}
    , {compression, Compression}
    , {min_compression_batch_size, 0}
    , {max_linger_ms, 0}
    ],
  ClientConfig =
    [ {auto_start_producers, true}
    , {default_producer_config, ProducerConfig}
    ] ++ SockOpts,
  {ok, _Pid} = brod_client:start_link(Brokers, ?CLIENT, ClientConfig),
  ok = brod:produce_sync(?CLIENT, Topic, Partition, Key, Value);
run(?PIPE_CMD, Brokers, Topic, SockOpts, Args) ->
  Partition = parse(Args, "--partition", fun parse_partition/1),
  Acks = parse(Args, "--acks", fun parse_acks/1),
  AckTimeout = parse(Args, "--ack-timeout", fun parse_timeout/1),
  Compression = parse(Args, "--compression", fun parse_compression/1),
  KvDeli = parse(Args, "--kv-deli", fun parse_delimiter/1),
  MsgDeli = parse(Args, "--msg-deli", fun parse_delimiter/1),
  MaxLingerMs = parse(Args, "--max-linger-ms", fun int/1),
  MaxLingerCnt = parse(Args, "--max-linger-cnt", fun int/1),
  MaxBatch = parse(Args, "--max-batch", fun parse_size/1),
  Source = parse(Args, "--source", fun parse_source/1),
  IsPrompt = parse(Args, "--prompt", fun parse_boolean/1),
  IsTail = parse(Args, "--tail", fun parse_boolean/1),
  IsNoExit = parse(Args, "--no-eof-exit", fun parse_boolean/1),
  BlkSize = parse(Args, "--blk-size", fun parse_size/1),
  ProducerConfig =
    [ {required_acks, Acks}
    , {ack_timeout, AckTimeout}
    , {compression, Compression}
    , {min_compression_batch_size, 0}
    , {max_linger_ms, MaxLingerMs}
    , {max_linger_count, MaxLingerCnt}
    , {max_batch_size, MaxBatch}
    ],
  ClientConfig =
    [ {auto_start_producers, true}
    , {default_producer_config, ProducerConfig}
    ] ++ SockOpts,
  {ok, _Pid} = brod_client:start_link(Brokers, ?CLIENT, ClientConfig),
  SendFun =
    fun({Key, Value}, PendingAcks) ->
        {ok, CallRef} =
          brod:produce(?CLIENT, Topic, Partition, Key, Value),
        debug("sent: ~w\n", [CallRef]),
        debug("value: ~P\n", [Value, 9]),
        queue:in(CallRef, PendingAcks)
    end,
  KvDeliForReader = case none =:= KvDeli of
                      true -> none;
                      false -> bin(KvDeli)
                    end,
  ReaderArgs = [ {source, Source}
               , {kv_deli, KvDeliForReader}
               , {msg_deli, bin(MsgDeli)}
               , {prompt, IsPrompt}
               , {tail, IsTail}
               , {no_exit, IsNoExit}
               , {blk_size, BlkSize}
               , {retry_delay, 100}
               ],
  {ok, ReaderPid} =
    brod_cli_pipe:start_link(ReaderArgs),
  _ = erlang:monitor(process, ReaderPid),
  pipe(ReaderPid, SendFun, queue:new()).

%% @private
run(?GROUPS_CMD, Brokers, SockOpts, Args) ->
  IsDesc = parse(Args, "--describe", fun parse_boolean/1),
  IDs = parse(Args, "--ids", fun parse_cg_ids/1),
  cg(Brokers, SockOpts, IsDesc, IDs);
run(?COMMITS_CMD, Brokers, SockOpts, Args) ->
  ID = parse(Args, "--id",
             fun(?undef) -> erlang:throw(<<"option --id missing">>);
                (X)      -> X
             end),
  commits(Brokers, SockOpts, ID);
run(Cmd, Brokers, SockOpts, Args) ->
  %% Clause for all per-topic commands
  Topic = parse(Args, "--topic", fun bin/1),
  run(Cmd, Brokers, Topic, SockOpts, Args).

%% @private
commits(BootstrapEndpoints, SockOpts, GroupId) ->
  case brod:fetch_committed_offsets(BootstrapEndpoints, SockOpts, GroupId) of
    {ok, PerTopicStructs} ->
      lists:foreach(fun print_commits/1, PerTopicStructs);
    {error, Reason} ->
      logerr("Failed to fetch commited offsets ~p\n", [Reason])
  end.

%% @private
print_commits(Struct) ->
  Topic = kf(topic, Struct),
  PartRsps = kf(partition_responses, Struct),
  print([Topic, ":\n"]),
  print([pp_fmt_struct(1, P) || P <- PartRsps]).

%% @private
cg(BootstrapEndpoints, SockOpts, _IsDesc = false, _IDs) ->
  %% Describe all groups
  All = list_groups(BootstrapEndpoints, SockOpts),
  lists:foreach(fun print_cg_cluster/1, All);
cg(BootstrapEndpoints, SockOpts, _IsDesc = true, IDs0) ->
  CgClusters = list_groups(BootstrapEndpoints, SockOpts),
  IDs =
    case IDs0 =:= all of
      true ->
        lists:foldl(fun({_, Cgs}, Acc) ->
                        [ID || #brod_cg{id = ID} <- Cgs] ++ Acc
                    end, [], CgClusters);
      false ->
        IDs0
    end,
  describe_cgs(CgClusters, SockOpts, IDs).

%% @private
describe_cgs(_, _SockOpts, []) -> ok;
describe_cgs([], _SockOpts, IDs) ->
  logerr("Unknown group IDs: ~s", [infix(IDs, ", ")]);
describe_cgs([{Coordinator, CgList} | Rest], SockOpts, IDs) ->
  %% Get all IDs managed by current coordinator.
  ThisIDs = [ID || #brod_cg{id = ID} <- CgList, lists:member(ID, IDs)],
  ok = do_describe_cgs(Coordinator, SockOpts, ThisIDs),
  IDsRest = IDs -- ThisIDs,
  describe_cgs(Rest, SockOpts, IDsRest).

%% @private
do_describe_cgs(_Coordinator, _SockOpts, []) ->
  ok;
do_describe_cgs(Coordinator, SockOpts, IDs) ->
  case brod:describe_groups(Coordinator, SockOpts, IDs) of
    {ok, DescArray} ->
      ok = print("~s\n", [fmt_endpoint(Coordinator)]),
      lists:foreach(fun print_cg_desc/1, DescArray);
    {error, Reason} ->
      logerr("Failed to describe IDs [~s] at broker ~s\nreason:~p\n",
             [infix(IDs, ","), fmt_endpoint(Coordinator), Reason])
  end.

%% @private
print_cg_desc(Desc) ->
  EC = kf(error_code, Desc),
  GroupId = kf(group_id, Desc),
  case ?IS_ERROR(EC) of
    true ->
      logerr("Failed to describe group id=~s\nreason:~p\n", [GroupId, EC]);
    false ->
      D1 = lists:keydelete(error_code, 1, Desc),
      D  = lists:keydelete(group_id, 1, D1),
      print("  ~s\n~s", [GroupId, pp_fmt_struct(_Indent = 2, D)])
  end.

%% @private
pp_fmt_struct(_Indent, []) -> [];
pp_fmt_struct(Indent, [{Field, Value} | Rest]) ->
  [ indent_fmt(Indent, "~p: ~s", [Field, pp_fmt_struct_value(Indent, Value)])
  | pp_fmt_struct(Indent, Rest)
  ].

%% @private
pp_fmt_struct_value(_Indent, X) when is_integer(X) orelse
                                     is_atom(X) orelse
                                     is_binary(X) orelse
                                     X =:= [] ->
  [pp_fmt_prim(X), "\n"];
pp_fmt_struct_value(Indent, [{_,_}|_] = SubStruct) ->
  ["\n", pp_fmt_struct(Indent + 1, SubStruct)];
pp_fmt_struct_value(Indent, Array) when is_list(Array) ->
  case hd(Array) of
    [{_,_}|_] ->
      %% array of sub struct
      ["\n",
       lists:map(fun(Item) ->
                     pp_fmt_struct(Indent + 1, Item)
                 end, Array)
      ];
    _ ->
      %% array of primitive values
      [[pp_fmt_prim(V) || V <- Array], "\n"]
  end.

%% @private
pp_fmt_prim([]) -> "[]";
pp_fmt_prim(N) when is_integer(N) -> integer_to_list(N);
pp_fmt_prim(A) when is_atom(A) -> atom_to_list(A);
pp_fmt_prim(S) when is_binary(S) -> S.

%% @private
indent_fmt(Indent, Fmt, Args) ->
  io_lib:format(lists:duplicate(Indent * 2, $\s) ++ Fmt, Args).

%% @private
print_cg_cluster({Endpoint, Cgs}) ->
  ok = print([fmt_endpoint(Endpoint), "\n"]),
  IoData = [ io_lib:format("  ~s (~s)\n", [Id, Type])
             || #brod_cg{id = Id, protocol_type = Type} <- Cgs
           ],
  print(IoData).

%% @private
fmt_endpoint({Host, Port}) ->
  bin(io_lib:format("~s:~B", [Host, Port])).

%% @private Return consumer groups clustered by group coordinator
%% {CoordinatorEndpoint, [group_id()]}.
%% @end
list_groups(Brokers, SockOpts) ->
  Cgs = brod:list_all_groups(Brokers, SockOpts),
  lists:keysort(1, lists:foldl(fun do_list_groups/2, [], Cgs)).

%% @private
do_list_groups({_Endpoint, []}, Acc) -> Acc;
do_list_groups({Endpoint, {error, Reason}}, Acc) ->
  logerr("Failed to list groups at kafka ~s\nreason~p",
         [fmt_endpoint(Endpoint), Reason]),
  Acc;
do_list_groups({Endpoint, Cgs}, Acc) ->
  [{Endpoint, Cgs} | Acc].

%% @private
pipe(ReaderPid, SendFun, PendingAcks0) ->
  PendingAcks1 = flush_pending_acks(PendingAcks0, _Timeout = 0),
  receive
    {pipe, ReaderPid, Messages} ->
      PendingAcks = lists:foldl(SendFun, PendingAcks1, Messages),
      pipe(ReaderPid, SendFun, PendingAcks);
    {'DOWN', _Ref, process, ReaderPid, Reason} ->
      %% Reader is down, flush pending acks
      debug("reader down, reason: ~p\n", [Reason]),
      _ = flush_pending_acks(PendingAcks1, infinity);
    #brod_produce_reply{ call_ref = CallRef
                       , result = brod_produce_req_acked
                       } ->
      {{value, CallRef}, PendingAcks} = queue:out(PendingAcks1),
      pipe(ReaderPid, SendFun, PendingAcks)
  end.

%% @private
flush_pending_acks(Queue, Timeout) ->
  case queue:peek(Queue) of
    empty ->
      Queue;
    {value, CallRef} ->
      case brod:sync_produce_request(CallRef, Timeout) of
        ok ->
          debug("acked: ~w\n", [CallRef]),
          {_, Rest} = queue:out(Queue),
          flush_pending_acks(Rest, Timeout);
        {error, timeout} ->
          Queue
      end
  end.

%% @private
fetch_loop(_FmtFun, _FetchFun, _Offset, 0) ->
  verbose("done (count)\n"),
  ok;
fetch_loop(FmtFun, FetchFun, Offset, Count) ->
  debug("Fetching from offset: ~p\n", [Offset]),
  case FetchFun(Offset) of
    {ok, []} ->
      %% reached max_wait, no message received
      verbose("done (wait)\n"),
      ok;
    {ok, Messages0} ->
      {Messages, NewCount} =
        case length(Messages0) of
          N when N > Count ->
            {lists:sublist(Messages0, Count), 0};
          N ->
            {Messages0, Count - N}
        end,
      #kafka_message{offset = LastOffset} = lists:last(Messages),
      lists:foreach(
        fun(M) ->
            #kafka_message{offset = O, key = K, value = V} = M,
            case FmtFun(O, ensure_kafka_bin(K), ensure_kafka_bin(V)) of
              ok -> ok;
              IoData -> io:put_chars(IoData)
            end
        end, Messages),
      fetch_loop(FmtFun, FetchFun, LastOffset + 1, NewCount)
  end.

%% @private
resolve_begin_offset(_Sock, _T, _P, Offset) when is_integer(Offset) ->
  Offset;
resolve_begin_offset(Sock, Topic, Partition, last) ->
  Earliest = resolve_begin_offset(Sock, Topic, Partition, earliest),
  Latest = resolve_begin_offset(Sock, Topic, Partition, latest),
  case Latest =:= Earliest of
    true  -> erlang:throw(bin("partition is empty"));
    false -> Latest - 1
  end;
resolve_begin_offset(Sock, Topic, Partition, Time) ->
  {ok, Offset} = brod_utils:resolve_offset(Sock, Topic, Partition, Time),
  Offset.

%% @private
parse_source("stdin") ->
  standard_io;
parse_source("@" ++ Path) ->
  parse_source(Path);
parse_source(Path) ->
  case filelib:is_regular(Path) of
    true -> {file, Path};
    false -> erlang:throw(bin(["bad file ", Path]))
  end.

%% @private
parse_size(Size) ->
  case lists:reverse(Size) of
    "K" ++ N -> int(lists:reverse(N)) * (1 bsl 10);
    "M" ++ N -> int(lists:reverse(N)) * (1 bsl 20);
    N        -> int(lists:reverse(N))
  end.

%% @private
format_metadata(Metadata, Format, IsList, IsToListUrp) ->
  Brokers = kf(brokers, Metadata),
  Topics0 = kf(topic_metadata, Metadata),
  Topics1 = case IsToListUrp of
              true -> lists:filter(fun is_ur_topic/1, Topics0);
              false -> Topics0
            end,
  Topics = format_topics(Topics1),
  case Format of
    json ->
      JSON = jsone:encode([ {brokers, Brokers}
                          , {topics, Topics}
                          ]),
      print([JSON, "\n"]);
    text ->
      %% When listing topics do not display hosts
      BL = case IsList of
             true -> [];
             false -> format_broker_lines(Brokers)
           end,
      TL = format_topics_lines(Topics, IsList),
      print([BL, TL])
  end.

%% @private
format_broker_lines(Brokers) ->
  Header = io_lib:format("brokers [~p]:\n", [length(Brokers)]),
  F = fun(Broker) ->
          Id = kf(node_id, Broker),
          Host = kf(host, Broker),
          Port = kf(port, Broker),
          %% TODO display rack
          io_lib:format("  ~p: ~s\n", [Id, fmt_endpoint({Host, Port})])
      end,
  [Header, lists:map(F, Brokers)].

%% @private
format_topics_lines(Topics, true) ->
  Header = io_lib:format("topics [~p]:\n", [length(Topics)]),
  [Header, lists:map(fun format_topic_list_line/1, Topics)];
format_topics_lines(Topics, false) ->
  Header = io_lib:format("topics [~p]:\n", [length(Topics)]),
  [Header, lists:map(fun format_topic_lines/1, Topics)].

%% @private
format_topic_list_line({Name, Partitions}) when is_list(Partitions) ->
  io_lib:format("  ~s\n", [Name]);
format_topic_list_line({Name, ErrorCode}) ->
  ErrorStr = format_error_code(ErrorCode),
  io_lib:format("  ~s: [ERROR] ~s\n", [Name, ErrorStr]).

%% @private
format_topic_lines({Name, Partitions}) when is_list(Partitions) ->
  Header = io_lib:format("  ~s [~p]:\n", [Name, length(Partitions)]),
  PartitionsText = format_partitions_lines(Partitions),
  [Header, PartitionsText];
format_topic_lines({Name, ErrorCode}) ->
  ErrorStr = format_error_code(ErrorCode),
  io_lib:format("  ~s: [ERROR] ~s\n", [Name, ErrorStr]).

%% @private
format_error_code(E) when is_atom(E) -> atom_to_list(E);
format_error_code(E) when is_integer(E) -> integer_to_list(E).

%% @private
format_partitions_lines(Partitions0) ->
  Partitions1 =
    lists:map(fun({Pnr, Info}) ->
                  {binary_to_integer(Pnr), Info}
              end, Partitions0),
  Partitions = lists:keysort(1, Partitions1),
  lists:map(fun format_partition_lines/1, Partitions).

%% @private
format_partition_lines({Partition, Info}) ->
  LeaderNodeId = kf(leader, Info),
  Status = kf(status, Info),
  Isr = kf(isr, Info),
  Osr = kf(osr, Info),
  MaybeWarning = case ?IS_ERROR(Status) of
                   true -> [" [", atom_to_list(Status), "]"];
                   false -> ""
                 end,
  ReplicaList =
    case Osr of
      [] -> format_list(Isr, "");
      _  -> [format_list(Isr, ""), ",", format_list(Osr, "*")]
    end,
  io_lib:format("~7s: ~2s (~s)~s\n",
                [integer_to_list(Partition),
                 integer_to_list(LeaderNodeId),
                 ReplicaList, MaybeWarning]).

%% @private
format_list(List, Mark) ->
  infix(lists:map(fun(I) -> [integer_to_list(I), Mark] end, List), ",").

%% @private
infix([], _Sep) -> [];
infix([_] = L, _Sep) -> L;
infix([H | T], Sep) -> [H, Sep, infix(T, Sep)].

%% @private
format_topics(Topics) ->
  TL = lists:map(fun format_topic/1, Topics),
  lists:keysort(1, TL).

%% @private
format_topic(Topic) ->
  ErrorCode = kf(topic_error_code, Topic),
  TopicName = kf(topic, Topic),
  PL = kf(partition_metadata, Topic),
  Data =
    case ?IS_ERROR(ErrorCode) of
      true  -> ErrorCode;
      false -> format_partitions(PL)
    end,
  {TopicName, Data}.

%% @private
format_partitions(Partitions) ->
  PL = lists:map(fun format_partition/1, Partitions),
  lists:keysort(1, PL).

%% @private
format_partition(P) ->
  ErrorCode = kf(partition_error_code, P),
  PartitionNr = kf(partition_id, P),
  LeaderNodeId = kf(leader, P),
  Replicas = kf(replicas, P),
  Isr = kf(isr, P),
  Data = [ {leader, LeaderNodeId}
         , {status, ErrorCode}
         , {isr, Isr}
         , {osr, Replicas -- Isr}
         ],
  {integer_to_binary(PartitionNr), Data}.

%% @private Return true if a topics is under-replicated
is_ur_topic(Topic) ->
  ErrorCode = kf(topic_error_code, Topic),
  Partitions = kf(partition_metadata, Topic),
  %% when there is an error, we do not know if
  %% it is under-replicated or not
  %% retrun true to alert user
  ?IS_ERROR(ErrorCode) orelse lists:any(fun is_ur_partition/1, Partitions).

%% @private Return true if a partition is under-replicated
is_ur_partition(Partition) ->
  ErrorCode = kf(partition_error_code, Partition),
  Replicas = kf(replicas, Partition),
  Isr = kf(isr, Partition),
  ?IS_ERROR(ErrorCode) orelse lists:sort(Isr) =/= lists:sort(Replicas).

%% @private
parse_delimiter("none") -> none;
parse_delimiter(EscappedStr) -> eval_str(EscappedStr).

%% @private
eval_str([]) -> [];
eval_str([$\\, $n | Rest]) ->
  [$\n | eval_str(Rest)];
eval_str([$\\, $t | Rest]) ->
  [$\t | eval_str(Rest)];
eval_str([$\\, $s | Rest]) ->
  [$\s | eval_str(Rest)];
eval_str([C | Rest]) ->
  [C | eval_str(Rest)].

%% @private
parse_fmt("v", _KvDel, MsgDeli) ->
  fun(_Offset, _Key, Value) -> [Value, MsgDeli] end;
parse_fmt("kv", KvDeli, MsgDeli) ->
  fun(_Offset, Key, Value) -> [Key, KvDeli, Value, MsgDeli] end;
parse_fmt("okv", KvDeli, MsgDeli) ->
  fun(Offset, Key, Value) ->
      [integer_to_list(Offset), KvDeli,
       Key, KvDeli, Value, MsgDeli]
  end;
parse_fmt("eterm", _KvDeli, _MsgDeli) ->
  fun(Offset, Key, Value) ->
      io_lib:format("~p.\n", [{Offset, Key, Value}])
  end;
parse_fmt(FunLiteral0, _KvDeli, _MsgDeli) ->
  FunLiteral = ensure_end_with_dot(FunLiteral0),
  {ok, Tokens, _Line} = erl_scan:string(FunLiteral),
  {ok, [Expr]} = erl_parse:parse_exprs(Tokens),
  {value, Fun, _Bindings} = erl_eval:expr(Expr, []),
  true = is_function(Fun, 3),
  Fun.

%% @private Append a dot to the function literal.
ensure_end_with_dot(Str0) ->
  Str = rstrip(Str0, [$\n, $\t, $\s, $.]),
  Str ++ ".".

%% @private
rstrip(Str, CharSet) ->
  lists:reverse(lstrip(lists:reverse(Str), CharSet)).

%% @private
lstrip([], _) -> [];
lstrip([C | Rest] = Str, CharSet) ->
  case lists:member(C, CharSet) of
    true -> lstrip(Rest, CharSet);
    false -> Str
  end.

%% @private
parse_partition("random") ->
  fun(_Topic, PartitionsCount, _Key, _Value) ->
      {_, _, Micro} = os:timestamp(),
      {ok, Micro rem PartitionsCount}
  end;
parse_partition("hash") ->
  fun(_Topic, PartitionsCount, Key, _Value) ->
      Hash = erlang:phash2(Key),
      {ok, Hash rem PartitionsCount}
  end;
parse_partition(I) ->
  try
    list_to_integer(I)
  catch
    _ : _ ->
      erlang:throw(bin(["Bad partition: ", I]))
  end.

%% @private
parse_acks("all") -> -1;
parse_acks("-1") -> -1;
parse_acks("0") -> 0;
parse_acks("1") -> 1;
parse_acks(X) -> erlang:throw(bin(["Bad --acks value: ", X])).

%% @private
parse_timeout(Str) ->
  case lists:reverse(Str) of
    "s" ++ R -> int(lists:reverse(R)) * 1000;
    "m" ++ R -> int(lists:reverse(R)) * 60 * 1000;
    _        -> int(Str)
  end.

%% @private
parse_compression("none") -> no_compression;
parse_compression("gzip") -> gzip;
parse_compression("snappy") -> snappy;
parse_compression(X) -> erlang:throw(bin(["Unknown --compresion value: ", X])).

%% @private
parse_offset_time("earliest") -> earliest;
parse_offset_time("latest") -> latest;
parse_offset_time("last") -> last;
parse_offset_time(T) -> int(T).

%% @private
parse_sock_opts(Args) ->
  SslBool = parse(Args, "--ssl", fun parse_boolean/1),
  CaCertFile = parse(Args, "--cacertfile", fun parse_file/1),
  CertFile = parse(Args, "--certfile", fun parse_file/1),
  KeyFile = parse(Args, "--keyfile", fun parse_file/1),
  FilterPred = fun({_, V}) -> V =/= ?undef end,
  SslOpt =
    case CaCertFile of
      ?undef ->
        SslBool;
      _ ->
        Files =
          [{cacertfile, CaCertFile},
           {certfile, CertFile},
           {keyfile, KeyFile}],
        lists:filter(FilterPred, Files)
    end,
  SaslOpt = parse(Args, "--sasl-plain", fun parse_file/1),
  SaslOpts = sasl_opts(SaslOpt),
  lists:filter(FilterPred, [{ssl, SslOpt} | SaslOpts]).

%% @private
sasl_opts(?undef) -> [];
sasl_opts(File)   -> [{sasl, {plain, File}}].

%% @private
parse_boolean(true) -> true;
parse_boolean(false) -> false;
parse_boolean("true") -> true;
parse_boolean("false") -> false;
parse_boolean(?undef) -> ?undef.

%% @private
parse_cg_ids("") -> [];
parse_cg_ids("all") -> all;
parse_cg_ids(Str) -> [bin(I) || I <- string:tokens(Str, ",")].

%% @private
parse_file(?undef) ->
  ?undef;
parse_file(Path) ->
  case filelib:is_regular(Path) of
    true  -> Path;
    false -> erlang:throw(bin(["bad file ", Path]))
  end.

%% @private
parse(Args, OptName, ParseFun) ->
  case lists:keyfind(OptName, 1, Args) of
    {_, Arg} ->
      try
        ParseFun(Arg)
      catch
        C : E ->
          Stack = erlang:get_stacktrace(),
          verbose("~p:~p\n~p\n", [C, E, Stack]),
          Reason =
            case Arg of
              ?undef -> ["Missing option ", OptName];
              _      -> ["Failed to parse ", OptName, ": ", Arg]
            end,
          erlang:throw(bin(Reason))
      end;
    false ->
      Reason = [OptName, " is missing"],
      erlang:throw(bin(Reason))
  end.

%% @private
print_version() ->
  _ = application:load(brod),
  {_, _, V} = lists:keyfind(brod, 1, application:loaded_applications()),
  print(V).

%% @private
print(IoData) -> io:put_chars(IoData).

%% @private
print(Fmt, Args) -> io:format(Fmt, Args).

%% @private
logerr(IoData) -> io:put_chars(standard_error, ["*** ", IoData]).

%% @private
logerr(Fmt, Args) -> io:format(standard_error, "*** " ++ Fmt, Args).

%% @private
verbose(Str) -> verbose(Str, []).

%% @private
verbose(Fmt, Args) ->
  case erlang:get(brod_cli_log_level) >= ?LOG_LEVEL_VERBOSE of
    true  -> io:format(standard_error, "[verbo]: " ++ Fmt, Args);
    false -> ok
  end.

%% @private
debug(Fmt, Args) ->
  case erlang:get(brod_cli_log_level) >= ?LOG_LEVEL_DEBUG of
    true  -> io:format(standard_error, "[debug]: " ++ Fmt, Args);
    false -> ok
  end.

%% @private
int(Str) -> list_to_integer(Str).

%% @private
bin(IoData) -> iolist_to_binary(IoData).

%% @private
ensure_kafka_bin(?undef) -> <<>>;
ensure_kafka_bin(Bin) -> Bin.

%% @private
parse_brokers(HostsStr) ->
  F = fun(HostPortStr) ->
          Pair = string:tokens(HostPortStr, ":"),
          case Pair of
            [Host, PortStr] -> {Host, list_to_integer(PortStr)};
            [Host]          -> {Host, 9092}
          end
      end,
  shuffle(lists:map(F, string:tokens(HostsStr, ","))).

%% @private Parse code paths.
parse_paths(?undef) -> [];
parse_paths(Str) -> string:tokens(Str, ",").

%% @private Randomize the order.
shuffle(L) ->
  RandList = lists:map(fun(_) -> element(3, os:timestamp()) end, L),
  {_, SortedL} = lists:unzip(lists:keysort(1, lists:zip(RandList, L))),
  SortedL.

%% @private
-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
