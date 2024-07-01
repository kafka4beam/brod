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

%% @doc A `brod_transaction' is a process that orchestates a set of
%% producers to store messages within a transaction, it also supports
%% committing offsets in the same transaction.
%%
%% Simple produce sample:
%%
%%  ```
%%  {ok, Tx} = brod_transaction:new(Client, TxId, []),
%%  lists:foreach(fun(Partition) ->
%%                    Key = rand(), Value = rand(),
%%                    {ok, _Offset} =
%%                    brod_transaction:produce(Tx,
%%                                             Topic,
%%                                             Partition,
%%                                             Key,
%%                                             Value),
%%                end, Partitions),
%%  brod_transaction:commit(Tx),
%%  '''
%%
%% handle callback of a group subscriber using offset commit within a
%% transaction:
%%
%%  ```
%%  handle_message(Topic,
%%                 Partition,
%%                 #kafka_message{ offset = Offset
%%                               , key = Key
%%                               , value = Value},
%%                 #{ client := Client
%%                  , group_id := GroupId} =  State) ->
%%    {ok, Tx} = brod_transaction:new(Client),
%%    {ok, _ProducedOffset} = brod_transaction:produce(Tx, ?TOPIC_OUTPUT, Partition, Key, Value),
%%    ok = brod_transaction:txn_add_offsets(Tx, GroupId, #{{Topic, Partition} => Offset}),
%%    ok = brod_transaction:commit(Tx)
%%
%%    {ok, ack_no_commit, State}.
%%  '''
%%

-module(brod_transaction).
-behaviour(gen_server).

% public API
-export([ produce/5
        , produce/4
        , add_offsets/3
        , commit/1
        , abort/1
        , stop/1
        , new/3
        , start_link/3
        ]).

% gen_server callbacks
-export([ init/1
        , handle_cast/2
        , handle_call/3
        , terminate/2
        ]).

% type exports
-export_type([ batch_input/0
             , call_ref/0
             , client/0
             , client_id/0
             , transaction_config/0
             , group_id/0
             , key/0
             , offset/0
             , offsets_to_commit/0
             , partition/0
             , topic/0
             , transaction/0
             , transactional_id/0
             , txn_ctx/0
             , value/0
             ]).

%%==============================================================================
%% Type declarations
%%==============================================================================

-type call_ref() :: brod:call_ref().
-type client() :: client_id() | pid().
-type client_id() :: atom().
-type transaction_config() :: [ {timeout,      non_neg_integer()}
                              | {backoff_step, non_neg_integer()}
                              | {max_retries,  non_neg_integer()}
                              ].
-type group_id() :: kpro:group_id().
-type key() :: brod:key().
-type offset() :: kpro:offset().
-type offsets_to_commit() :: kpro:offsets_to_commit().
-type partition() :: kpro:partition().
-type topic() :: kpro:topic().
-type transaction() :: pid().
-type transactional_id() :: kpro:transactional_id().
-type txn_ctx() :: kpro:txn_ctx().
-type value() :: brod:value().
-type batch_input() :: kpro:batch_input().

-record(state,
        { client_pid      :: client()
        , context         :: txn_ctx()
        , timeout         :: pos_integer()
        , sequences       :: map()
        , sent_partitions :: map()
        , max_retries     :: pos_integer()
        , backoff_step    :: pos_integer()
        }).

-type state() :: #state{}.

%%==============================================================================
%% API functions
%%==============================================================================

%% @see start_link/3
-spec new(client(), transactional_id(), transaction_config()) -> {ok, transaction()}.
new(Client, TxId, Config) ->
  gen_server:start_link(?MODULE,
                        {Client, TxId, Config},
                        []).

%% @doc Start a new transaction, `TxId'will be the id of the transaction
%% `Config' is a proplist, all values are optional:
%% `timeout':`Connection timeout in millis
%% `backoff_step': after each retry it will sleep for 2^Attempt * backoff_step
%%  millis
%% `max_retries'
-spec start_link(client(), transactional_id(), transaction_config()) -> {ok, pid()}.
start_link(Client, TxId, Config) ->
  gen_server:start_link(?MODULE, {Client, TxId, Config}, []).

%% @doc Produce the message (key and value) to the indicated topic-partition
%% synchronously.
-spec produce(transaction(), topic(), partition(), key(), value()) ->
  {ok, offset()} | {error, any()}.
produce(Transaction, Topic, Partition, Key, Value) ->
  gen_server:call(Transaction, {produce, Topic, Partition, Key, Value}).

%% @doc Synchronously produce the batch of messages to the indicated
%% topic-partition
-spec produce(transaction(), topic(), partition(), batch_input()) ->
  {ok, offset()} | {error, any()}.
produce(Transaction, Topic, Partition, Batch) ->
  gen_server:call(Transaction, {produce, Topic, Partition, Batch}).

%% @doc Add the offset consumed by a group to the transaction.
-spec add_offsets(transaction(), group_id(), offsets_to_commit()) -> ok | {error, any()}.
add_offsets(Transaction, ConsumerGroup, Offsets) ->
  gen_server:call(Transaction, {add_offsets, ConsumerGroup, Offsets}).

%% @doc Commit the transaction, after this, the gen_server will stop
-spec commit(transaction()) -> ok | {error, any()}.
commit(Transaction) ->
  gen_server:call(Transaction, commit).

%% @doc Abort the transaction, after this, the gen_server will stop
-spec abort(transaction()) -> ok | {error, any()}.
abort(Transaction) ->
  gen_server:call(Transaction, abort).

%% @doc Stop the transaction.
-spec stop(transaction()) -> ok | {error, any()}.
stop(Transaction) ->
  gen_server:call(Transaction, terminate).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================

init({Client, TxId, PropListConfig}) ->
  ClientPid = pid(Client),
  erlang:process_flag(trap_exit, true),

  Config =
  #{ max_retries := MaxRetries
   , backoff_step := BackOffStep
   , timeout := Timeout
   } = lists:foldl(fun({K, V}, M) ->
                       M#{K => V}
                   end,
                   #{ max_retries => 5
                    , backoff_step => 100
                    , timeout => 1000
                    }, PropListConfig),

  {ok, CTX} = make_txn_context(ClientPid, TxId, Config),
  {ok, #state{ client_pid = ClientPid
             , context = CTX
             , max_retries = MaxRetries
             , backoff_step= BackOffStep
             , timeout = Timeout
             , sequences = #{}
             , sent_partitions = #{}
             }}.
handle_call({add_offsets, ConsumerGroup, Offsets}, _From,
            #state{ client_pid = Client
                  , context = CTX
                  , max_retries = MaxRetries
                  , backoff_step = BackOffStep
                  } = State) ->
  Resp = do_add_offsets(Client, CTX, ConsumerGroup, Offsets,
                        #{ max_retries => MaxRetries
                         , backoff_step => BackOffStep
                         }),
  {reply, Resp, State};
handle_call({produce, Topic, Partition, Key, Value}, _From,
            #state{} = OldState) ->
  case do_produce(Topic, Partition, Key, Value, OldState) of
    {ok, {Offset, State}} -> {reply, {ok, Offset}, State};
    {error, Reason} -> {reply, {error, Reason}, OldState}
  end;
handle_call({produce, Topic, Partition, Batch}, _From,
            #state{} = OldState) ->
  case do_batch_produce(Topic, Partition, Batch, OldState) of
    {ok, {Offset, State}} -> {reply, {ok, Offset}, State};
    {error, Reason} -> {reply, {error, Reason}, OldState}
  end;
handle_call(commit, _From, #state{context = CTX} = State) ->
  {stop, normal, kpro:txn_commit(CTX), State};
handle_call(terminate, _From, State) ->
  {stop, normal, ok, #state{} = State};
handle_call(abort, _From,
            #state{context = CTX} = State) ->
  {stop, normal, kpro:txn_abort(CTX), State};
handle_call({abort, Timeout}, _From,
            #state{context = CTX} = State) ->
  {stop, normal,
   kpro:txn_abort(CTX, #{timeout => Timeout}),
   State};
handle_call(stop, _From, #state{} = State) ->
  {stop, normal, ok, State};
handle_call(Call, _From, #state{} = State) ->
  {reply, {error, {unsupported_call, Call}}, State}.
handle_cast(_Cast, #state{} = State) ->
  {noreply, State}.
terminate(_Reason, #state{context = CTX}) ->
  kpro:txn_abort(CTX).

%%==============================================================================
%% Internal functions
%%==============================================================================

make_txn_context(Client, TxId, #{ max_retries := MaxRetries
                                , backoff_step := BackOffStep
                                })->
  persistent_call(fun() ->
                    make_txn_context_internal(Client, TxId)
                  end, MaxRetries, BackOffStep).

make_txn_context_internal(Client, TxId) ->
  case brod_client:get_transactional_coordinator(Client, TxId) of
    {ok, {{Host, Port}, _}} ->
      case brod_client:get_connection(Client, Host, Port) of
        {ok, Conn} -> kpro:txn_init_ctx(Conn, TxId);

        {error, Reason} -> {error, Reason}
      end;
    {error, Reason} -> {error, Reason}
  end.

do_add_offsets(Client, CTX, ConsumerGroup, Offsets,
               #{ max_retries := MaxRetries
                , backoff_step := BackOffStep
                }) ->
  persistent_call(fun() ->
                    do_add_offsets_internal(Client, CTX,
                                            ConsumerGroup, Offsets)
                  end,
                  MaxRetries, BackOffStep).

do_add_offsets_internal(Client, CTX, ConsumerGroup, Offsets) ->
  case brod_client:get_group_coordinator(Client, ConsumerGroup) of
    {ok, {{Host, Port}, _}} ->
      case brod_client:get_connection(Client, Host, Port) of
        {ok, Conn} -> send_cg_and_offset(Conn, CTX, ConsumerGroup, Offsets);
        {error, Reason} -> {error, Reason}
      end;
      {error, Reason} -> {error, Reason}
  end.

send_cg_and_offset(GroupCoordConn, CTX, ConsumerGroup, Offsets) ->
  %% before adding the offset we need to let kafka know we are going to commit
  %% the offsets.
  case kpro:txn_send_cg(CTX, ConsumerGroup) of
    ok -> kpro:txn_offset_commit(GroupCoordConn, ConsumerGroup, CTX, Offsets);
    {error, Reason} -> {error, Reason}
  end.

-spec do_produce(topic(), partition(), key(), value(), state()) ->
  {error, string()} | {ok, {offset(), state()}}.
do_produce(Topic, Partition, Key, Value, State) ->
  do_batch_produce(Topic, Partition, [#{ key => Key
                                       , value => Value
                                       , ts => kpro_lib:now_ts()
                                       }], State).

-spec do_batch_produce(topic(), partition(), batch_input(), state()) ->
  {error, string()} | {ok, {offset(), state()}}.
do_batch_produce(Topic, Partition, Batch, #state{ max_retries = MaxRetries
                                                , backoff_step = BackOffStep
                                                } = State) ->
  persistent_call(fun() ->
                      do_batch_produce_internal(Topic, Partition,
                                                Batch, State)
                  end, MaxRetries, BackOffStep).

do_batch_produce_internal(Topic, Partition, Batch,
                          #state{ client_pid = ClientPid
                                , timeout = Timeout
                                , context = CTX
                                , sequences = Sequences
                                , sent_partitions = OldSentPartitions
                                } = State) ->

  case conn_and_vsn(ClientPid, Topic, Partition) of
    {ok, {Connection, Vsn}} ->
      FirstSequence = maps:get({Topic, Partition}, Sequences, 0),

      ProduceReq = kpro_req_lib:produce(Vsn, Topic, Partition,
                                        Batch,
                                        #{ txn_ctx => CTX
                                         , first_sequence => FirstSequence
                                         }),

      SentPartitions =
      case maps:get({Topic, Partition}, OldSentPartitions, not_found) of
        not_found ->
          ok = kpro:txn_send_partitions(CTX, [{Topic, Partition}]),
          maps:put({Topic, Partition}, true, OldSentPartitions);
        _ -> OldSentPartitions
      end,

      case send_req(Connection, ProduceReq, Timeout) of
        {ok, Offset} ->
          {ok, {Offset, State#state{ sent_partitions = SentPartitions
                                   , sequences = maps:put({Topic, Partition},
                                                         FirstSequence + length(Batch),
                                                         Sequences)
                                   }}};
        {error, Reason} -> {error, Reason}
      end;
    {error, Reason} -> {error, Reason}
  end.

send_req(Connection, ProduceReq, Timeout) ->
  case kpro:request_sync(Connection, ProduceReq, Timeout) of
    {ok, Rsp} -> brod_utils:parse_rsp(Rsp);
    {error, Reason} -> {error, Reason}
  end.

conn_and_vsn(ClientPid, Topic, Partition) ->
  case brod_client:get_leader_connection(ClientPid, Topic, Partition) of
    {ok, Connection} ->
      case kpro:get_api_versions(Connection) of
        {ok, #{ produce := {_, Vsn}
              , fetch   := {_, _}
              }} -> {ok, {Connection, Vsn}};
        {error, Reason} -> {error, Reason}
      end;
    {error, Reason} -> {error, Reason}
  end.

-spec pid(client()) -> pid().
pid(Client) when is_atom(Client) -> whereis(Client);
pid(Client) when is_pid(Client) -> Client.

backoff(Attempt, BackOffStep) ->
  timer:sleep(trunc(math:pow(2, Attempt) * BackOffStep)).

persistent_call(Fun, MaxRetries, BackOffStep) ->
  persistent_call(Fun, 0, MaxRetries, BackOffStep).

persistent_call(Fun, Attempt, MaxRetries, BackOffStep) ->
  case Fun() of
    ok -> ok;
    {ok, R} -> {ok, R};
    {error, _} when Attempt + 1 < MaxRetries ->
      backoff(Attempt, BackOffStep),
      persistent_call(Fun, Attempt + 1, MaxRetries, BackOffStep);
    {error, Reason} -> {error, Reason}
  end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
