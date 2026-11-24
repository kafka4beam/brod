%%%
%%%   Copyright (c) 2026 kafka4beam contributors
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

%% @private Leader epochs of the partitions consumed through a client (KIP-320).
%%
%% A table of this module is owned by a `brod_client' process,
%% and shared by its partition consumers and the group coordinators
%% which commit offsets for them:
%%
%% <ul>
%%  <li>A partition consumer records the leader epochs of the record batches
%%      it fetches, with the offset where each epoch starts.</li>
%%  <li>A group coordinator looks up the leader epoch of the last consumed
%%      record when it commits an offset (`committed_leader_epoch'),
%%      and records the leader epoch of the committed offsets it fetches.</li>
%%  <li>A partition consumer uses the leader epoch of the committed offset
%%      as the lowest leader epoch to send in fetch requests.</li>
%% </ul>
%%
%% With the committed leader epoch in fetch requests, a stale partition
%% leader, which has not learned about a newer leader, rejects the requests
%% with `unknown_leader_epoch', even if the stale leader itself was asked
%% for the partition metadata.
%%
%% As in the Java client, the epochs of fetched batches are only used to
%% commit offsets, not as the lowest epoch to fetch with.
-module(brod_leader_epochs).

-export([ new/0
        , committed/4
        , fetched/5
        , epoch_at/4
        , committed_epoch/3
        , forget/3
        ]).

-export_type([ table/0
             ]).

-type table() :: ets:tid().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type offset() :: brod:offset().
-type leader_epoch() :: kpro:leader_epoch().

%% A partition rarely changes leader. Keep only the latest epochs:
%% committed offsets are close to the fetched offsets.
-define(MAX_FETCHED_EPOCHS, 16).

-define(COMMITTED_KEY(Topic, Partition), {committed, Topic, Partition}).
-define(FETCHED_KEY(Topic, Partition), {fetched, Topic, Partition}).

%% @doc Create a new table, owned by the calling process.
-spec new() -> table().
new() ->
  ets:new(?MODULE, [set, public, {read_concurrency, true}]).

%% @doc Record the leader epoch of a committed offset.
%% `-1' (the offset was committed without a leader epoch)
%% forgets the epoch of an earlier committed offset.
-spec committed(table(), topic(), partition(), leader_epoch()) -> ok.
committed(Tab, Topic, Partition, LeaderEpoch) when LeaderEpoch < 0 ->
  true = ets:delete(Tab, ?COMMITTED_KEY(Topic, Partition)),
  ok;
committed(Tab, Topic, Partition, LeaderEpoch) ->
  true = ets:insert(Tab, {?COMMITTED_KEY(Topic, Partition), LeaderEpoch}),
  ok.

%% @doc Record the leader epoch of a fetched record batch,
%% given the offset of the first record in the batch.
%% Only one process should call this for a partition,
%% i.e. the partition consumer of the client.
-spec fetched(table(), topic(), partition(), offset(), leader_epoch()) -> ok.
fetched(_Tab, _Topic, _Partition, _Offset, LeaderEpoch) when LeaderEpoch < 0 ->
  ok;
fetched(Tab, Topic, Partition, Offset, LeaderEpoch) ->
  Epochs = lookup_fetched(Tab, Topic, Partition),
  case epoch_at(Epochs, Offset) =:= LeaderEpoch of
    true ->
      ok;
    false ->
      %% Leader epochs only grow with offsets. Records after this offset
      %% with a lower or the same epoch were truncated, or are fetched again:
      %% forget them. Keep those with a higher epoch, e.g. when fetching
      %% again from before the oldest recorded epoch.
      Earlier = [E || {StartOffset, _} = E <- Epochs, StartOffset < Offset],
      Later = [E || {StartOffset, Epoch} = E <- Epochs,
                    StartOffset > Offset, Epoch > LeaderEpoch],
      NewEpochs = last_n(Earlier ++ [{Offset, LeaderEpoch} | Later],
                         ?MAX_FETCHED_EPOCHS),
      true = ets:insert(Tab, {?FETCHED_KEY(Topic, Partition), NewEpochs}),
      ok
  end.

%% @doc Return the leader epoch of the fetched record at the given offset,
%% or `-1' if unknown.
-spec epoch_at(table(), topic(), partition(), offset()) -> leader_epoch().
epoch_at(Tab, Topic, Partition, Offset) ->
  epoch_at(lookup_fetched(Tab, Topic, Partition), Offset).

%% @doc Return the leader epoch of the committed offset,
%% or `-1' if unknown.
-spec committed_epoch(table(), topic(), partition()) -> leader_epoch().
committed_epoch(Tab, Topic, Partition) ->
  case ets:lookup(Tab, ?COMMITTED_KEY(Topic, Partition)) of
    [{_, Epoch}] -> Epoch;
    [] -> -1
  end.

%% @doc Forget the leader epochs of a partition,
%% e.g. after the topic was deleted.
-spec forget(table(), topic(), partition()) -> ok.
forget(Tab, Topic, Partition) ->
  true = ets:delete(Tab, ?COMMITTED_KEY(Topic, Partition)),
  true = ets:delete(Tab, ?FETCHED_KEY(Topic, Partition)),
  ok.

%%%_* Internal functions =======================================================

lookup_fetched(Tab, Topic, Partition) ->
  case ets:lookup(Tab, ?FETCHED_KEY(Topic, Partition)) of
    [{_, Epochs}] -> Epochs;
    [] -> []
  end.

%% Epochs are sorted by start offset.
epoch_at(Epochs, Offset) ->
  lists:foldl(
    fun({StartOffset, Epoch}, _Acc) when StartOffset =< Offset -> Epoch;
       (_, Acc) -> Acc
    end, -1, Epochs).

last_n(L, N) ->
  lists:nthtail(max(0, length(L) - N), L).

%%%_* Tests ====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(T, <<"t">>).

fetched_test() ->
  Tab = new(),
  ?assertEqual(-1, epoch_at(Tab, ?T, 0, 0)),
  ok = fetched(Tab, ?T, 0, 10, 5),
  ok = fetched(Tab, ?T, 0, 15, 5),
  ok = fetched(Tab, ?T, 0, 20, 7),
  ?assertEqual(-1, epoch_at(Tab, ?T, 0, 9)),
  ?assertEqual(5, epoch_at(Tab, ?T, 0, 10)),
  ?assertEqual(5, epoch_at(Tab, ?T, 0, 19)),
  ?assertEqual(7, epoch_at(Tab, ?T, 0, 20)),
  ?assertEqual(7, epoch_at(Tab, ?T, 0, 1000)),
  %% other partitions are not affected
  ?assertEqual(-1, epoch_at(Tab, ?T, 1, 20)),
  %% unknown epochs are ignored
  ok = fetched(Tab, ?T, 0, 30, -1),
  ?assertEqual(7, epoch_at(Tab, ?T, 0, 30)),
  ets:delete(Tab).

%% A batch with another epoch than recorded for its offset replaces
%% what was recorded from that offset on.
fetched_again_test() ->
  Tab = new(),
  ok = fetched(Tab, ?T, 0, 10, 5),
  ok = fetched(Tab, ?T, 0, 20, 7),
  ok = fetched(Tab, ?T, 0, 30, 8),
  %% Fetched again from an earlier offset: nothing changes
  ok = fetched(Tab, ?T, 0, 12, 5),
  ?assertEqual(8, epoch_at(Tab, ?T, 0, 30)),
  %% Log truncated (unclean leader election): offset 20 now has epoch 9
  ok = fetched(Tab, ?T, 0, 20, 9),
  ?assertEqual(5, epoch_at(Tab, ?T, 0, 19)),
  ?assertEqual(9, epoch_at(Tab, ?T, 0, 20)),
  ?assertEqual(9, epoch_at(Tab, ?T, 0, 30)),
  ets:delete(Tab).

%% Fetching again from before the oldest recorded epoch,
%% e.g. after a reset to the earliest offset, keeps the later epochs.
fetched_earlier_test() ->
  Tab = new(),
  ok = fetched(Tab, ?T, 0, 1000, 5),
  ok = fetched(Tab, ?T, 0, 2000, 6),
  ok = fetched(Tab, ?T, 0, 500, 5),
  ?assertEqual(-1, epoch_at(Tab, ?T, 0, 499)),
  ?assertEqual(5, epoch_at(Tab, ?T, 0, 500)),
  ?assertEqual(5, epoch_at(Tab, ?T, 0, 1999)),
  ?assertEqual(6, epoch_at(Tab, ?T, 0, 2500)),
  ets:delete(Tab).

fetched_max_test() ->
  Tab = new(),
  lists:foreach(fun(I) -> ok = fetched(Tab, ?T, 0, I * 10, I) end,
                lists:seq(1, ?MAX_FETCHED_EPOCHS + 4)),
  %% epochs 1-4 (offsets 10-49) are forgotten
  ?assertEqual(-1, epoch_at(Tab, ?T, 0, 49)),
  ?assertEqual(5, epoch_at(Tab, ?T, 0, 50)),
  ets:delete(Tab).

committed_test() ->
  Tab = new(),
  ok = committed(Tab, ?T, 0, -1),
  ?assertEqual(-1, committed_epoch(Tab, ?T, 0)),
  ok = committed(Tab, ?T, 0, 153),
  ?assertEqual(153, committed_epoch(Tab, ?T, 0)),
  ?assertEqual(-1, committed_epoch(Tab, ?T, 1)),
  %% the committed epoch is not the epoch of fetched records,
  %% and fetched records do not change the committed epoch
  ?assertEqual(-1, epoch_at(Tab, ?T, 0, 100)),
  ok = fetched(Tab, ?T, 0, 200, 154),
  ?assertEqual(153, committed_epoch(Tab, ?T, 0)),
  ok = forget(Tab, ?T, 0),
  ?assertEqual(-1, committed_epoch(Tab, ?T, 0)),
  ?assertEqual(-1, epoch_at(Tab, ?T, 0, 200)),
  %% an offset committed without an epoch replaces the committed epoch
  ok = committed(Tab, ?T, 0, 153),
  ok = committed(Tab, ?T, 0, -1),
  ?assertEqual(-1, committed_epoch(Tab, ?T, 0)),
  ets:delete(Tab).

-endif.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
