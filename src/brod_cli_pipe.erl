%%%
%%%   Copyright (c) 2017-2018, Klarna Bank AB (publ)
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

%% @doc The input source of brod-cli pipe command
%% This module implements a process that reads off the bytes
%% from the data source (either stdin or a file)
%% and sends the bytes to parent process.
%% Messages sent to parent process:
%%   {pipe, self(), [{Ts :: integer(), Key :: binary(), Val :: binary()}]}
%% @end
-module(brod_cli_pipe).

-ifdef(build_brod_cli).

-behaviour(gen_server).

-export([ start_link/1
        , stop/1
        ]).

-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-include("brod_int.hrl").

-type arg_name() :: source
                  | kv_deli
                  | msg_deli
                  | prompt
                  | tail
                  | no_exit
                  | blk_size
                  | retry_delay.

-type arg_value() :: term().

-define(LINE_BREAK, <<"\n">>).
-define(STDIN, standard_io).
-define(EOF_RETRY_DELAY_MS, 100).
-define(NOT_APPLICABLE, 'N/A').
-define(CONTINUE_MSG, continue).
-define(PARENT_BUSY_MSG_QUEUE_LEN_THRESHOLD, 100).

-type delimiter() :: binary().
-type epoch_ms() :: integer().
-type read_fun() ::
        fun((?STDIN | file:io_device(), [binary()]) -> eof |
              {[{epoch_ms(), Key :: binary(), Val :: binary()}], [binary()]}).

-record(state, { parent :: pid()
               , source :: ?STDIN | {file, string()}
               , read_fun :: read_fun()
               , is_eof_exit :: boolean()
               , is_tail :: boolean()
               , io_device :: ?undef | ?STDIN | file:io_device()
               , acc_bytes = [] :: [binary()]
               , retry_delay :: timeout()
               }).

%% @doc Args explained:
%% source:   'standard_io' | {file, "path/to/srouce"}
%% kv_deli:  'none' | binary().
%%           Delimiter bytes for message key and value
%% msg_deli: binary(). Delimiter between kafka messages
%%           NOTE: eof is always considered a message delimiter
%% prompt:   boolean(). Applicable when source is standard_io AND
%%           when kv_deli and msg_deli are both '\n'
%%           prompts 'key> ' for key input and 'val> ' for value input
%% tail:     boolean(). Applicable when source is a file
%%           tell brod-cli to start reading from EOF
%% no_exit:  boolean(). Do not exit when reaching EOF
%% blk_size: Read block size
-spec start_link([{arg_name(), arg_value()}]) -> {ok, pid()}.
start_link(Args) ->
  Parent = self(),
  Arg = fun(Name) -> {_, V} = lists:keyfind(Name, 1, Args), V end,
  KvDeli = Arg(kv_deli),
  MsgDeli = Arg(msg_deli),
  Source = Arg(source),
  IsLineMode = MsgDeli =:= ?LINE_BREAK,
  BlkSize = Arg(blk_size),
  IsPrompt = Arg(prompt),
  IsTail = Arg(tail),
  IsNoExit = Arg(no_exit),
  IsEofExit = not (IsTail orelse IsNoExit),
  ReadFun =
    case IsLineMode of
      true when IsPrompt andalso Source =:= ?STDIN ->
        make_prompt_line_reader(KvDeli);
      true ->
        make_line_reader(KvDeli, _PromptStr = "");
      false ->
        make_stream_reader(KvDeli, MsgDeli, BlkSize, IsEofExit)
    end,
  State = #state{ parent = Parent
                , source = Source
                , read_fun = ReadFun
                , is_tail = IsTail
                , is_eof_exit = IsEofExit
                , retry_delay = Arg(retry_delay)
                },
  gen_server:start_link({local, ?MODULE}, ?MODULE, State, []).

%% @doc Stop gen_server.
stop(Pid) -> gen_server:cast(Pid, stop).

%% @doc Tell reader to continue.
continue() -> self() ! ?CONTINUE_MSG.

%% @hidden
init(#state{source = Source, is_tail = IsTail} = State0) ->
  IoDevice =
    case Source of
      ?STDIN ->
        ?STDIN;
      {file, File} ->
        {ok, Fd} = file:open(File, [read, binary]),
        IsTail andalso file:position(Fd, eof),
        Fd
    end,
  State = State0#state{io_device = IoDevice},
  _ = continue(),
  {ok, State}.

%% @hidden
handle_info(?CONTINUE_MSG, #state{parent = Parent} = State) ->
  case erlang:process_info(Parent, message_queue_len) of
    {_, Len} when Len >= ?PARENT_BUSY_MSG_QUEUE_LEN_THRESHOLD ->
      ok = delay_continue(State),
      {noreply, State};
    _ ->
      handle_read(State)
  end;
handle_info(_Info, State) ->
  {noreply, State}.

%% @hidden
handle_cast(stop, State) ->
  {stop, normal, State};
handle_cast(_Cast, State) ->
  {noreply, State}.

%% @hidden
handle_call(Call, _From, State) ->
  {reply, {error, {unknown_call, Call}}, State}.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% @hidden
terminate(_Reason, _State) ->
  ok.

%%%_* Privates =================================================================

send_to_parent(Parent, Msgs0) ->
  FilterF = fun(?TKV(_T, K, V)) -> K =/= <<>> orelse V =/= <<>> end,
  Msgs = lists:filter(FilterF, Msgs0),
  Msgs =/= [] andalso erlang:send(Parent, {pipe, self(), Msgs}),
  ok.

handle_read(#state{ read_fun = ReadFun
                  , acc_bytes = Acc0
                  , io_device = IoDevice
                  , parent = Parent
                  } = State0) ->
  case ReadFun(IoDevice, Acc0) of
    eof ->
      handle_eof(State0);
    {Msgs, Acc} ->
      _ = continue(), %% continue next trunk
      ok = send_to_parent(Parent, Msgs),
      State = State0#state{acc_bytes = Acc},
      {noreply, State}
  end.

handle_eof(#state{io_device = ?STDIN} = State) ->
  %% standard_io pipe closed
  {stop, normal, State};
handle_eof(#state{is_eof_exit = true} = State) ->
  {stop, normal, State};
handle_eof(#state{io_device = Fd} = State) ->
  %% Get current position
  {ok, LastPos} = file:position(Fd, {cur, 0}),
  %% Try set position to EOF,
  %% see if it is the current position
  case file:position(Fd, eof) of
    {ok, NewPos} when NewPos < LastPos ->
      %% File has been truncated.
      %% Don't know what to do because
      %% we can not assume the file is truncated to empty
      {stop, pipe_source_truncated, State};
    {ok, _Pos} ->
      _ = file:position(Fd, LastPos),
      ok = delay_continue(State),
      {noreply, State}
  end.

delay_continue(#state{retry_delay = Delay}) ->
  _ = erlang:send_after(Delay, self(), ?CONTINUE_MSG),
  ok.

-spec make_prompt_line_reader(none | delimiter()) -> read_fun().
make_prompt_line_reader(_KvDeli = none) ->
  %% Read only value, no key
  fun(?STDIN, _Acc) ->
      case read_line(?STDIN, "VAL> ") of
        eof   -> eof;
        Value -> {[{_Key = <<>>, Value}], []}
      end
  end;
make_prompt_line_reader(_KvDeli = ?LINE_BREAK) ->
  fun(?STDIN, _Acc) ->
      case read_line(?STDIN, "KEY> ") of
        eof -> eof;
        Key ->
          case read_line(?STDIN, "VAL> ") of
            eof -> {[make_msg(Key, <<>>)], []};
            Value -> {[make_msg(Key, Value)], []}
          end
      end
  end;
make_prompt_line_reader(KvDeli) ->
  Prompt = "KEY" ++ binary_to_list(KvDeli) ++ "VAL> ",
  make_line_reader(KvDeli, Prompt).

-spec make_line_reader(none | binary(), string()) -> read_fun().
make_line_reader(KvDeli, Prompt) ->
  fun(IoDevice, _Acc) ->
      case read_line(IoDevice, Prompt) of
        eof ->
          eof;
        Key when KvDeli =:= <<"\n">> ->
          case read_line(IoDevice, Prompt) of
            eof ->
              eof;
            Val ->
              {[make_msg(Key, Val)], []}
          end;
        Val when KvDeli =:= none ->
          {[make_msg(<<>>, Val)], []};
        Line ->
          [Key, Value] = binary:split(Line, bin(KvDeli)),
          {[make_msg(Key, Value)], []}
      end
  end.

-spec make_stream_reader(none | delimiter(), delimiter(),
                         pos_integer(), boolean()) -> read_fun().
make_stream_reader(KvDeli, MsgDeli, BlkSize, IsEofExit) ->
  IsSameDeli = MsgDeli =:= KvDeli,
  KvDeliCp = case is_binary(KvDeli) of
               true -> binary:compile_pattern(KvDeli);
               false -> none
             end,
  MsgDeliCp = binary:compile_pattern(MsgDeli),
  fun(IoDevice, Acc) ->
      case file:read(IoDevice, BlkSize) of
        eof ->
          case IsEofExit of
            true when Acc =:= [] ->
              %% Reached EOF
              eof;
            true ->
              %% Configured to exit when reaching EOF
              %% try split kv-pairs NOW
              LastMsg = bin(lists:reverse(Acc)),
              KvPairs = split_kv_pairs([LastMsg], KvDeliCp, IsSameDeli),
              {KvPairs, []};
            false ->
              %% Keep looping for the next message delimiter
              {[], Acc}
          end;
        {ok, Bytes} ->
          Acc1 = add_acc(size(MsgDeli), Bytes, Acc),
          {Messages, NewAcc} = split_messages(MsgDeliCp, Acc1),
          KvPairs = split_kv_pairs(Messages, KvDeliCp, IsSameDeli),
          {KvPairs, NewAcc}
      end
  end.

-spec add_acc(pos_integer(), binary(), [binary()]) -> [binary()].
add_acc(_DeliSize = 1, Bytes, Acc) ->
  %% Delimiter is only one byte, in no way coult it be cut in half
  [Bytes | Acc];
add_acc(_DeliSize, Bytes, []) ->
  [Bytes];
add_acc(DeliSize, Bytes, [Tail | Header]) ->
  Size = size(Tail) - DeliSize,
  case Size =< 0 of
    true ->
      [<<Tail/binary, Bytes/binary>> | Header];
    false ->
      %% cut a DeliSize tail from acc and prepend as current head
      %% to make sure we will not cut delimiter into two chunks
      <<TailH:Size/binary, TailT/binary>> = Tail, %% cut
      NewTail = <<TailT/binary, Bytes/binary>>, %% new tail
      [NewTail, TailH | Header]
  end.

-spec split_messages(binary:cp(), [binary()]) -> {[binary()], [binary()]}.
split_messages(MsgDeliCp, [Tail | Header]) ->
  case binary:split(Tail, MsgDeliCp, [global]) of
    [_] ->
      %% no delimiter found
      {[], [Tail | Header]};
    [First0 | More] ->
      First = bin([lists:reverse(Header), First0]),
      case lists:reverse(More) of
        [<<>> | Msgs] ->
          {[First | lists:reverse(Msgs)], []};
        [NewTail | Msgs] ->
          {[First | lists:reverse(Msgs)], [NewTail]}
      end
  end.

-spec split_kv_pairs([binary()], none | delimiter(), boolean()) -> brod:value().
split_kv_pairs(Msgs, none, _IsSameDeli) ->
  lists:map(fun(Msg) -> make_msg(<<>>, Msg) end, Msgs);
split_kv_pairs(Msgs, _KvDeliCp, _IsSameDeli = true) ->
  make_msgs(Msgs);
split_kv_pairs(Msgs, KvDeliCp, _IsSameDeli = false) ->
  lists:map(fun(Msg) ->
                [K, V] = binary:split(Msg, KvDeliCp),
                make_msg(K, V)
            end, Msgs).

make_msgs([]) -> [];
make_msgs([K, V | Rest]) ->
  [make_msg(K, V) | make_msgs(Rest)].

make_msg(K, V) ->
  CreateTs = brod_utils:epoch_ms(),
  ?TKV(CreateTs, K, V).

-spec read_line(?STDIN | file:io_device(), string()) -> eof | binary().
read_line(IoDevice, Prompt) ->
  case io:get_line(IoDevice, Prompt) of
    eof -> eof;
    Line ->
      Chars = unicode:characters_to_list(Line),
      unicode:characters_to_binary(rstrip(Chars, "\n"))
  end.

-spec rstrip(string(), string()) -> string().
rstrip(Str, CharSet) ->
  lists:reverse(lstrip(lists:reverse(Str), CharSet)).

-spec lstrip(string(), string()) -> string().
lstrip([], _) -> [];
lstrip([C | Rest] = Str, CharSet) ->
  case lists:member(C, CharSet) of
    true -> lstrip(Rest, CharSet);
    false -> Str
  end.

-spec bin(iodata()) -> binary().
bin(X) -> iolist_to_binary(X).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
