%%%
%%%   Copyright (c) 2023 @axs-mvd and contributors
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

%% @doc `brod_transaction_processor' allows the execution of a function in the context
%% of a transaction. It abstracts the usage of a group subscriber reading and writing
%% using a transaction in each fetch cycle.
%%  For example, the following snippets are equivalent
%%
%%-------------------------------------------------
%%
%%function_that_does_something(Messages, ...) ->
%%    write_some_messages_into_some_topic(Messages, ...),
%%    write_some_other_messages_into_yet_another_topic(Messages, ...).
%%
%%handle_message(Topic, Partition, Messages, State) ->
%%    {ok, Tx} = brod:transaction(...)             % opens a transaction
%%    function_that_does_something(Messages, ...)  % adds the writes to the transaction
%%    ok = brod:txn_add_offsets(...)               % add offsets to the transsaction
%%    ok = btrod:commit(Tx)                        % commit
%%    {ok, ack_no_commit, State}
%%
%%-------------------------------------------------
%%
%%brod_transaction_processor:do(
%%    fun(Transaction, Messages) ->
%%        write_some_messages_into_some_topic(Messages, ...),
%%        write_some_other_messages_into_yet_another_topic(Messages, ...)
%%     end,
%%     ...)
%%
%%-------------------------------------------------
%%
-module(brod_transaction_processor).

-include("brod.hrl").

%public API
-export([do/3]).

% group subscriber callbacks
-export([ init/2
        , handle_message/4
        , get_committed_offsets/3]).

% type exports
-export_type([ do_options/0
             , process_function/0]).

%%==============================================================================
%% Type declarations
%%==============================================================================

-type client() :: client_id() | pid().
-type client_id() :: atom().
-type do_options() :: #{ group_config => proplists:proplist()
                       , consumer_config => proplists:proplist()
                       , transaction_config => proplists:proplist()
                       , group_id => binary()
                       , topics => [binary()]}.
-type message_set() :: #kafka_message_set{}.
-type transaction() :: brod_transaction:transaction().


-type process_function() :: fun((transaction(), message_set()) -> ok
                                                                | {error, any()}).

%% @doc executes the ProcessFunction within the context of a transaction.
%% Options is a map that can include
%% `group_config' as the configuration for the group suscriber.
%% `consumer_config' as the configuration for the consumer suscriber.
%% `transaction_config' transacction config.
%% `group_id' as the subscriber group id.
%% `topics' topics to fetch from.
%%
%% FizzBuzz sample:
%%
%% fizz_buzz(N) when (N rem 15) == 0 -> "FizzBuzz"
%% fizz_buzz(N) when (N rem 3) == 0 -> "Fizz"
%% fizz_buzz(N) when (N rem 5) == 0 -> "Buzz";
%% fizz_buzz(N) -> N end.
%%
%% brod_transaction_processor:do(
%%     fun(Transaction, #kafka_message_set{ topic     = _Topic
%%                                        , partition = Partition
%%                                        , messages  = Messages} = _MessageSet) ->
%%         FizzBuzzed =
%%         lists:map(fun(#kafka_message{ key = Key
%%                                     , value = Value}) ->
%%                       #{ key => Key
%%                        , value => fizz_buzz(Value)}
%%                       end, Messages),
%%
%%         brod:txn_produce(Transaction,
%%                           ?OUTPUT_TOPIC,
%%                           Partition,
%%                           FizzBuzzed),
%%
%%         ok
%%     end, Client, #{ topics => [?INPUT_TOPIC]
%%                   , group_id => ?PROCESSOR_GROUP_ID}).
%%
-spec do(process_function(), client(), do_options()) -> {ok, pid()}
                                                      | {error, any()}.
do(ProcessFun, Client, Opts) ->

  Defaults = #{ group_config => [{offset_commit_policy, consumer_managed}]
                %% note that if you change the group_config you must include
                %% the above option, as it enables our fetcher to manage
                %% the offsets itself
              , consumer_config => []},

  #{ group_id := GroupId
   , topics := Topics
   , group_config := GroupConfig
   , consumer_config := ConsumerConfig} = maps:merge(Defaults, Opts),

  InitState = #{client => Client,
                process_function => ProcessFun},

  brod:start_link_group_subscriber(Client,
                                   GroupId,
                                   Topics,
                                   GroupConfig,
                                   ConsumerConfig,
                                   message_set,
                                   ?MODULE,
                                   InitState).

%%==============================================================================
%% group subscriber callbacks
%%==============================================================================
init(GroupId, #{ client := Client
               , process_function := ProcessFun
               } = Opts) ->
  #{ tx_id := TxId
   , transaction_config := Config} =
  maps:merge(#{ tx_id => make_transactional_id()
              , transaction_config => []
              }, Opts),

  {ok, #{ client => Client
        , transaction_config => Config
        , tx_id => TxId
        , process_function => ProcessFun
        , group_id => GroupId
        }}.

handle_message(Topic,
               Partition,
               #kafka_message_set{ topic     = Topic
                                 , partition = Partition
                                 , messages  = _Messages
                                 } = MessageSet,
               #{ process_function := ProcessFun
                , client := Client
                , tx_id := TxId
                , transaction_config := TransactionConfig
                , group_id := GroupId
                } = State) ->

  {ok, Tx} = brod:transaction(Client, TxId, TransactionConfig),
  ok = ProcessFun(Tx, MessageSet),
  ok = brod:txn_add_offsets(Tx, GroupId, offsets_to_commit(MessageSet)),
  ok = brod:commit(Tx),
  {ok, ack_no_commit, State}.

get_committed_offsets(GroupId, TPs, #{client := Client} = State) ->
  {ok, Offsets} = brod:fetch_committed_offsets(Client, GroupId),
  TPOs =
  lists:flatmap( fun(#{name := Topic, partitions := Partitions}) ->
                     [{{Topic, Partition}, COffset} ||
                      #{ partition_index := Partition
                       , committed_offset := COffset
                       } <- Partitions,
                      lists:member({Topic, Partition}, TPs)]
                 end
                 , Offsets
               ),
  {ok, TPOs, State}.

%%==============================================================================
%% Internal functions
%%==============================================================================

make_transactional_id() ->
  iolist_to_binary([atom_to_list(?MODULE), "-txn-",
                    base64:encode(crypto:strong_rand_bytes(8))]).


offsets_to_commit(#kafka_message_set{ topic     = Topic
                                    , partition = Partition
                                    , messages  = Messages
                                    }) ->
  #kafka_message{offset = Offset} = lists:last(Messages),
  #{{Topic, Partition} => Offset}.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
