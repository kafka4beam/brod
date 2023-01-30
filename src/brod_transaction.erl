%%%
%%%   Copyright (c) 2014-2021 Klarna Bank AB (publ)
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

%% @doc A `brod_transaction' is a `gen_server' that orchestate a set of
%% producers to store messages within a transaction, it also supports
%% committing offsets in the same transaction.
%%
%% Simple produce sample:
%%
%%  {ok, Tx} = brod_transaction:new(Client, TxId, []),
%%  lists:foreach(fun(Partition) ->
%%                    Key = rand(), Value = rand(),
%%                    {ok, Ref} =
%%                    brod_transaction:produce(Tx,
%%                                             Topic,
%%                                             Partition,
%%                                             Key,
%%                                             Value),
%%                    {ok, _Offset} = brod_transaction:sync_produce_request(Ref)
%%                end, Partitions),
%%  brod_transaction:commit(Tx),
%%
%% handle callback of a group suuscriber using offset commit within a
%% transaction:
%%
%%  handle_message(Topic,
%%                 Partition,
%%                 #kafka_message{ offset = Offset
%%                               , key = Key
%%                               , value = Value},
%%                 #{ client := Client
%%                  , group_id := GroupId} =  State) ->
%%    {ok, Tx} = brod_transaction:new(Client),
%%    {ok, Ref1} = brod_transaction:produce(Tx, ?TOPIC_OUTPUT, Partition, Key, Value),
%%    ok = brod_transaction:txn_add_offsets(Tx, GroupId, #{{Topic, Partition} => Offset}),
%%    {ok, _} = brod_transaction:txn_sync_request(Ref1, ?TIMEOUT),
%%    ok = brod_transaction:commit(Tx)
%%
%%    {ok, ack_no_commit, State}.
%%

-module(brod_transaction).
-behaviour(gen_server).

% gen_server callbacks
-export([ init/1
        , handle_cast/2
        , handle_call/3
        , handle_info/2
        , terminate/2
        , code_change/3]).

% public API
-export([ produce/5
        , produce_no_ack/5
        , produce_cb/6
        , sync_produce_request/2
        , add_offsets/3
        , commit/1
        , abort/1
        , abort/2
        , stop/1
        , new/2
        , new/3
        , start_link/2
        , start_link/3]).

-export_type([ call_ref/0
             , client/0
             , client_id/0
             , config/0
             , group_id/0
             , key/0
             , offset/0
             , offsets_to_commit/0
             , partition/0
             , produce_ack_cb/0
             , topic/0
             , transaction/0
             , transactional_id/0
             , txn_ctx/0
             , value/0]).

-type call_ref() :: brod:call_ref().
-type client() :: client_id() | pid().
-type client_id() :: atom().
-type config() :: proplists:proplist().
-type group_id() :: kpro:group_id().
-type key() :: brod:key().
-type offset() :: kpro:offset().
-type offsets_to_commit() :: kpro:offsets_to_commit().
-type partition() :: kpro:partition().
-type produce_ack_cb() :: brod:produce_ack_cb().
-type topic() :: kpro:topic().
-type transaction() :: pid().
-type transactional_id() :: kpro:transactional_id().
-type txn_ctx() :: kpro:txn_ctx().
-type value() :: brod:value().


-record(state,
        { client_pid       :: client()
        , context          :: txn_ctx()
        , producers        :: map()
        , producers_config :: config()
        }).

-type state() :: #state{}.

init({Client, TxId, Config}) ->
  ClientPid = pid(Client),
  erlang:process_flag(trap_exit, true),
  {ok, CTX} = make_txn_context(ClientPid, TxId),
  {ok, #state{ client_pid = ClientPid
             , context = CTX
             , producers_config = Config
             , producers = #{}
             }}.

%%% @private
handle_call({add_offsets, ConsumerGroup, Offsets}, _From,
            #state{ client_pid = Client
                  , context = CTX
                  } = State) ->
  {ok, {{Host, Port}, _}} = brod_client:get_group_coordinator(Client, ConsumerGroup),
  {ok, Conn} = brod_client:get_connection(Client, Host, Port),

  %% before adding the offset we need to let kafka know we are going to commit
  %% the offsets.
  ok = kpro:txn_send_cg(CTX, ConsumerGroup),

  %% actually commit the offset
  Resp = kpro:txn_offset_commit(Conn, ConsumerGroup, CTX, Offsets),
  {reply, Resp, State};


%% @private
handle_call({get_producer, Topic, Partition}, _From, #state{} = OldState) ->
  State = may_add_producer(Topic, Partition, OldState),
  {reply, get_producer(Topic, Partition, State), State};

%% @private
handle_call(commit, _From, #state{context = CTX} = State) ->
  ok = kpro:txn_commit(CTX),
  {stop, normal, ok, stop_producers(State)};

%% @private
handle_call(terminate, _From, State) ->
  {stop, normal, ok, stop_producers(State)};

%% @private
handle_call(abort, _From,
            #state{context = CTX} = State) ->
  ok = kpro:txn_abort(CTX),
  {stop, normal, ok, stop_producers(State)};

%% @private
handle_call({abort, Timeout}, _From,
            #state{context = CTX} = State) ->
  ok = kpro:txn_abort(CTX, #{timeout => Timeout}),
  {stop, normal, ok, stop_producers(State)};

%% @private
handle_call(stop, _From, #state{} = State) ->
  {stop, normal, ok, stop_producers(State)};

%% @private
handle_call(Call, _From, #state{} = State) ->
  {reply, {error, {unsupported_call, Call}}, State}.

%% @private
handle_cast(_Cast, #state{} = State) ->
  {noreply, State}.

%% @private
handle_info(_Call, State) ->
  {noreply, State}.

%% @private
code_change(_OldVsn, #state{} = State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) -> ok.


%% @see start_link/2
-spec new(pid(), config()) -> {ok, transaction()}.
new(ClientPid, ProducerConfig) ->
  gen_server:start_link(?MODULE,
                        {ClientPid, make_transactional_id(), ProducerConfig},
                        []).

%% @see start_link/3
-spec new(pid(), transactional_id(), config()) -> {ok, transaction()}.
new(ClientPid, TxId, ProducerConfig) ->
  gen_server:start_link(?MODULE,
                        {ClientPid, TxId, ProducerConfig},
                        []).

%% @see start_link/3
%% it uses a random id for the transaction
-spec start_link(pid(), config()) -> {ok, pid()}.
start_link(ClientPid, ProducerConfig) ->
  gen_server:start_link(?MODULE,
                        {ClientPid, make_transactional_id(), ProducerConfig},
                        []).

%% @doc starts a new transaction, TxId will be the id of the transaction
%% ProducerConfig will be the configuration of the managed producers
%% @see producer:start_link/4 for documentation about this
-spec start_link(pid(), transactional_id(), config()) -> {ok, pid()}.
start_link(ClientPid, TxId, ProducerConfig) ->
  gen_server:start_link(?MODULE, {ClientPid, TxId, ProducerConfig}, []).

%% @doc produces the message (key and value) to the indicated topic-partition
%% asynchronously returning a reference to get the result.
%% @see brod_producer:produce/3
-spec produce(transaction(), topic(), partition(), key(), value()) ->
  {ok, brod:call_ref()} | {error, any()}.
produce(Transaction, Topic, Partition, Key, Value) ->
  {ok, ProducerPid} = gen_server:call(Transaction, {get_producer, Topic, Partition}),
  brod_producer:produce(ProducerPid, Key, Value).

%% @doc produces the message (key and value) to the indicated topic-partition
%% asynchronously. Without a way to know the result.
%% @see brod_producer:produce_no_ack/3
-spec produce_no_ack(transaction(), topic(), partition(), key(), value()) ->
  ok.
produce_no_ack(Transaction, Topic, Partition, Key, Value) ->
  {ok, ProducerPid} = gen_server:call(Transaction, {get_producer, Topic, Partition}),
  brod_producer:produce_no_ack(ProducerPid, Key, Value).

%% @doc produces the message (key and value) to the indicated topic-partition
%% asynchronously. On success it will call the function AckCb.
%% @see brod_producer:produce_cb/4
-spec produce_cb(transaction(), topic(), partition(), key(), value(),
                  undef | produce_ack_cb()) -> ok | {ok, call_ref()} | {error, any()}.
produce_cb(Transaction, Topic, Partition, Key, Value, AckCb) ->
  {ok, ProducerPid} = gen_server:call(Transaction, {get_producer, Topic, Partition}),
  brod_producer:produce_cb(ProducerPid, Key, Value, AckCb).

%% @see brod_producer:sync_produce_request/2
-spec sync_produce_request(call_ref(), timeout()) ->
  {ok, offset()} | {error, Reason}
    when Reason :: timeout | {producer_down, any()}.
sync_produce_request(CallRef, Timeout) ->
  brod_producer:sync_produce_request(CallRef, Timeout).

%% @doc adds the offset consumed by a group to the transaction.
-spec add_offsets(transaction(), group_id(), offsets_to_commit()) -> ok | {error, any()}.
add_offsets(Transaction, ConsumerGroup, Offsets) ->
  gen_server:call(Transaction, {add_offsets, ConsumerGroup, Offsets}).

%% @doc commits the transaction, after this, the gen_server will stop
-spec commit(transaction()) -> ok | {error, any()}.
commit(Transaction) ->
  gen_server:call(Transaction, commit).

%% @doc aborts the transaction, after this, the gen_server will stop
-spec abort(transaction()) -> ok | {error, any()}.
abort(Transaction) ->
  gen_server:call(Transaction, abort).

%% @doc aborts the transaction with a timeout, after this, the gen_server will
%% stop
-spec abort(transaction(), pos_integer()) -> ok | {error, any()}.
abort(Transaction, Timeout) ->
  gen_server:call(Transaction, {abort, Timeout}).

%% @doc stops the transaction.
-spec stop(transaction()) -> ok | {error, any()}.
stop(Transaction) ->
  gen_server:call(Transaction, terminate).

%% @private
make_transactional_id() ->
  iolist_to_binary([atom_to_list(?MODULE), "-txn-",
                    base64:encode(crypto:strong_rand_bytes(8))]).

%% @private
make_txn_context(Client, TxId)->
  {ok, {{Host, Port}, _}} = brod_client:get_transactional_coordinator(Client,
                                                                      TxId),
  {ok, Conn} = brod_client:get_connection(Client, Host, Port),

  kpro:txn_init_ctx(Conn, TxId).

%% @private
-spec may_add_producer(topic(), partition(), state()) -> state().
may_add_producer(Topic, Partition,
                 #state{ client_pid = ClientPid
                       , producers = OldProducers
                       , context = CTX
                       , producers_config = ProducersConfig
                       } = OldState) ->
  case maps:is_key({Topic, Partition}, OldProducers) of
    true -> OldState;
    _ ->
      {ok, ProducerPid} = brod_producer:start_link(ClientPid,
                                                   Topic,
                                                   Partition,
                                                   CTX,
                                                   ProducersConfig),

      %% before producing something in a topic partition, we have to
      %% announce it
      ok = kpro:txn_send_partitions(CTX, [{Topic, Partition}]),

      OldState#state{producers = OldProducers#{{Topic, Partition} => ProducerPid}}
  end.

-spec get_producer(topic(), partition(), state()) -> {ok, pid()}.
get_producer(Topic, Partition, #state{producers = Producers}) ->
  {ok, maps:get({Topic, Partition}, Producers)}.

-spec stop_producers(state()) -> state().
stop_producers(#state{producers = Producers} = OldState) ->
  lists:foreach(fun brod_producer:stop/1,
                maps:values(Producers)),
  OldState.

-spec pid(client()) -> pid().
pid(Client) when is_atom(Client) -> whereis(Client);
pid(Client) when is_pid(Client) -> Client.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
