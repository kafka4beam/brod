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
  brod --help
  brod --version
  brod <command> [options] [--verbose | --debug]

commands:
  meta:   Inspect topic metadata
  offset: Inspect offsets
  fetch:  Fetch messages
  send:   Produce messages
  pipe:   Pipe file or stdin as messages to kafka
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
"
).

-define(META_CMD, "meta").
-define(META_DOC, "usage:
  brod meta [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
  -t,--topic=<topic>     Topic name [default: *]
  -T,--text              Print metadata as aligned texts (default)
  -J,--json              Print metadata as JSON object
  -L,--list              List topics, no partition details,
                         Applicable only for --text option
  -U,--under-replicated  Display only under-replicated partitions
"
?COMMAND_COMMON_OPTIONS
"Text output schema:
brokers <count>:
  <broker-id>: <endpoint>
topics <count>:
  <name> <count>: [[ERROR] [<reason>]]
    <partition>: [[ERROR] [<reason>]] | <broker-id> (isr...) (osr...) [WARNING]
"
).

-define(OFFSET_CMD, "offset").
-define(OFFSET_DOC, "usage:
  brod offset [options]

options:
  -b,--brokers=<brokers> Comma separated host:port pairs
  -t,--topic=<topic>     Topic name
  -p,--partition=<parti> Partition number
  -c,--count=<count>     Number of offsets to fetch [default: 1]
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
  -t,--topic=<topic>     Topic name
  -p,--partition=<parti> Partition number
  -o,--offset=<offset>   Offset to start fetching from
                           latest: From the latest offset (not last)
                           earliest: From earliest offset (first)
                           last: From offset (latest - 1)
                           <integer>: from a specific offset
                         [default: last]
  -c,--count=<count>     Number of messages to fetch (-1 to as infinity)
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

-define(DOCS, [{?META_CMD, ?META_DOC},
               {?OFFSET_CMD, ?OFFSET_DOC},
               {?SEND_CMD, ?SEND_DOC},
               {?FETCH_CMD, ?FETCH_DOC},
               {?PIPE_CMD, ?PIPE_DOC}
               ]).

-define(LOG_LEVEL_QUIET, 0).
-define(LOG_LEVEL_VERBOSE, 1).
-define(LOG_LEVEL_DEBUG, 2).

-type log_level() :: non_neg_integer().

-type command() :: string().

main(["--help" | _], _Stop) ->
  print(?MAIN_DOC);
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
  IsHelp = lists:member("--help", Args0),
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
      ok = error_logger:logfile({open, 'brod.log'}),
      ok = error_logger:tty(false);
    false ->
      ok
  end,
  {ok, _} = application:ensure_all_started(brod),
  try
    Brokers = parse(ParsedArgs, "--brokers", fun parse_brokers/1),
    Topic = parse(ParsedArgs, "--topic", fun bin/1),
    SockOpts = parse_sock_opts(ParsedArgs),
    verbose("sock opts: ~p\n", [SockOpts]),
    run(Command, Brokers, Topic, SockOpts, ParsedArgs)
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
  Format = proplists:get_value(true, [ {IsJSON, json}
                                     , {IsText, text}
                                     , {true, text}
                                     ]),
  IsList = parse(Args, "--list", fun parse_boolean/1),
  IsUrp = parse(Args, "--under-replicated", fun parse_boolean/1),
  {ok, Metadata} = brod:get_metadata(Brokers, Topics, SockOpts),
  format_metadata(Metadata, Format, IsList, IsUrp);
run(?OFFSET_CMD, Brokers, Topic, SockOpts, Args) ->
  Partition = parse(Args, "--partition", fun int/1), %% not parse_partition/1
  Count = parse(Args, "--count", fun int/1),
  Time = parse(Args, "--time", fun parse_offset_time/1),
  {ok, Offsets} =
    brod:get_offsets(Brokers, Topic, Partition, Time, Count, SockOpts),
  print("~w\n", [Offsets]);
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
  {ok, Sock} = brod:connect_leader(Brokers, Topic, Partition, SockOpts),
  MaxBytes = parse(Args, "--max-bytes", fun parse_size/1),
  Offset = resolve_begin_offset(Sock, Topic, Partition, Offset0),
  FetchFun =
    fun(BeginOffset) ->
        ReqFun =
          fun(MaxBytes_) ->
            kpro:fetch_request(Topic, Partition, BeginOffset, Wait, 1,
                               MaxBytes_)
          end,
        brod_utils:fetch(Sock, ReqFun, MaxBytes, BeginOffset)
    end,
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
resolve_begin_offset(Sock, Topic, Partition, Offset0) ->
  {ok, [Offset]} =
    brod_utils:fetch_offsets(Sock, Topic, Partition, Offset0, 1),
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
format_metadata(Metadata, Format, IsList, IsUrp) ->
  #kpro_MetadataResponse{ broker_L = Brokers0
                        , topicMetadata_L = Topics0
                        } = Metadata,
  Topics1 = case IsUrp of
              true -> lists:filter(fun is_ur_topic/1, Topics0);
              false -> Topics0
            end,
  Brokers = format_brokers(Brokers0),
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
      TL0 = lists:map(
              fun([{Name, Partitions}]) ->
                  {Name, Partitions}
              end, Topics),
      TL1 = lists:keysort(1, TL0),
      TL = format_topics_lines(TL1, IsList),
      print([BL, TL])
  end.

%% @private
format_brokers(Brokers) ->
  lists:map(
    fun(#kpro_Broker{ nodeId = Id
                    , host = Host
                    , port = Port
                    }) ->
        {integer_to_binary(Id), bin([Host, ":", integer_to_list(Port)])}
    end, lists:keysort(#kpro_Broker.nodeId, Brokers)).

%% @private
format_broker_lines(Brokers) ->
  Header = io_lib:format("brokers [~p]:\n", [length(Brokers)]),
  F = fun({Id, Endpoint}) ->
          io_lib:format("  ~s: ~s\n", [Id, Endpoint])
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
    lists:map(fun([{Pnr, Info}]) ->
                  {binary_to_integer(Pnr), Info}
              end, Partitions0),
  Partitions = lists:keysort(1, Partitions1),
  lists:map(fun format_partition_lines/1, Partitions).

%% @private
format_partition_lines({Partition, Info}) when is_list(Info) ->
  [ {leader, LeaderNodeId}
  , {isr, Isr}
  , {osr, Osr}
  ] = Info,
  io_lib:format("~7s: ~p ~s ~s\n",
                [integer_to_list(Partition), LeaderNodeId,
                 format_list(Isr),
                 format_list(Osr)]);
format_partition_lines({Partition, ErrorCode}) ->
  ErrorStr = format_error_code(ErrorCode),
  io_lib:format("    ~s: [ERROR] ~s\n", [Partition, ErrorStr]).

%% @doc
format_list(List) ->
  Str = lists:flatten(io_lib:format("~w", [List])),
  ["(", rstrip(lstrip(Str , "["), "]"), ")"].

%% @private
format_topics(Topics) ->
  lists:map(fun format_topic/1, Topics).

%% @private Return true if a topics is under-replicated
is_ur_topic(Topic) ->
  #kpro_TopicMetadata{ errorCode = ErrorCode
                     , partitionMetadata_L = PL
                     } = Topic,
  %% when there is an error, we do not know if
  %% it is under-replicated or not
  %% retrun true to alert user
  kpro_ErrorCode:is_error(ErrorCode) orelse
  lists:any(fun is_ur_partition/1, PL).

%% @private Return true if a partition is under-replicated
is_ur_partition(Partition) ->
  #kpro_PartitionMetadata{ errorCode = ErrorCode
                         , replicas = Replicas
                         , isr = Isr
                         } = Partition,
  kpro_ErrorCode:is_error(ErrorCode) orelse
  lists:sort(Isr) =/= lists:sort(Replicas).

%% @private
format_topic(Topic) ->
  #kpro_TopicMetadata{ errorCode = ErrorCode
                     , topicName = TopicName
                     , partitionMetadata_L = PL
                     } = Topic,
  Data =
    case kpro_ErrorCode:is_error(ErrorCode) of
      true ->
        ErrorCode;
      false ->
        format_partitions(PL)
    end,
  [{TopicName, Data}].

%% @private
format_partitions(Partitions) ->
  lists:map(fun format_partition/1, Partitions).

format_partition(Partition) ->
  #kpro_PartitionMetadata{ errorCode = ErrorCode
                         , partition = PartitionNr
                         , leader = LeaderNodeId
                         , replicas = Replicas
                         , isr = Isr
                         } = Partition,
  Data =
    case kpro_ErrorCode:is_error(ErrorCode) of
      true ->
        ErrorCode;
      false ->
        [ {leader, LeaderNodeId}
        , {isr, Isr}
        , {osr, Replicas -- Isr}
        ]
    end,
  [{integer_to_binary(PartitionNr), Data}].

%% @private
parse_delimiter("none") ->
  none;
parse_delimiter(EscappedStr) ->
  eval_str(EscappedStr).

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
parse_fmt("term", _KvDeli, _MsgDeli) ->
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
  SaslOpts = maybe_read_username_password(SaslOpt),
  lists:filter(FilterPred, [{ssl, SslOpt} | SaslOpts]).

%% @private
maybe_read_username_password(?undef) ->
  [];
maybe_read_username_password(File) ->
  {ok, Bin} = file:read_file(File),
  Lines = binary:split(Bin, <<"\n">>, [global]),
  [Username, Password] = lists:filter(fun(Line) -> Line =/= <<>> end, Lines),
  [{sasl, {plain, Username, Password}}].

%% @private
parse_boolean(true) -> true;
parse_boolean(false) -> false;
parse_boolean("true") -> true;
parse_boolean("false") -> false;
parse_boolean(?undef) -> ?undef.

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
              _      -> ["Failed to parse ", OptName, ": '", Arg, "'"]
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
logerr(IoData) -> io:put_chars(standard_error, IoData).

%% @private
logerr(Fmt, Args) -> io:format(standard_error, Fmt, Args).

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
  lists:map(F, string:tokens(HostsStr, ",")).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
