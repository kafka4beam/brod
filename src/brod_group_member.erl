%%%
%%%   Copyright (c) 2016-2021 Klarna Bank AB (publ)
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
%%% Implement `brod_group_member' behaviour callbacks to allow a
%%% process to act as a group member without having to deal with Kafka
%%% group protocol details. A typical workflow:
%%%
%%% 1. Spawn a group coordinator by calling
%%%    {@link brod_group_coordinator:start_link/6}.
%%%
%%% 2. Subscribe to partitions received in the assignments from
%%%    `assignments_received/4' callback.
%%%
%%% 3. Receive messages from the assigned partitions (delivered by
%%%    the partition workers (the pollers) implemented in `brod_consumer').
%%%
%%% 4. Unsubscribe from all previously subscribed partitions when
%%%    `assignments_revoked/1' is called.
%%%
%%% For group members that commit offsets to Kafka, do:
%%%
%%% 1. Call {@link brod_group_coordinator:ack/5}. to acknowledge successful
%%%    consumption of the messages. Group coordinator will commit the
%%%    acknowledged offsets at configured interval.
%%%
%%% 2. Call {@link brod_group_coordinator:commit_offsets/2}
%%%    to force an immediate offset commit if necessary.
%%%
%%% For group members that manage offsets locally, do:
%%%
%%% 1. Implement the `get_committed_offsets/2' callback.
%%%    This callback is evaluated every time when new assignments are received.
%%% @end
%%%=============================================================================

-module(brod_group_member).

-include("brod_int.hrl").

-optional_callbacks([assign_partitions/3,
                     user_data/1
                    ]).

%% Call the callback module to initialize assignments.
%% NOTE: This function is called only when `offset_commit_policy' is
%%       `consumer_managed' in group config.
%%       see brod_group_coordinator:start_link/6. for more group config details
%% NOTE: The committed offsets should be the offsets for successfully processed
%%       (acknowledged) messages, not the begin-offset to start fetching from.
-callback get_committed_offsets(pid(), [{brod:topic(), brod:partition()}]) ->
            {ok, [{{brod:topic(), brod:partition()}, brod:offset()}]}.

%% Called when the member is elected as the consumer group leader.
%% The first element in the group member list is ensured to be the leader.
%% NOTE: this function is called only when 'partition_assignment_strategy' is
%% 'callback_implemented' in group config.
%% see brod_group_coordinator:start_link/6. for more group config details.
-callback assign_partitions(pid(), [brod:group_member()],
                            [{brod:topic(), brod:partition()}]) ->
                                  [{brod:group_member_id(),
                                    [brod:partition_assignment()]}].

%% Called when assignments are received from group leader.
%% the member process should now call brod:subscribe/5
%% to start receiving message from kafka.
-callback assignments_received(pid(), brod:group_member_id(),
                               brod:group_generation_id(),
                               brod:received_assignments()) -> ok.

%% Called before group re-balancing, the member should call
%% brod:unsubscribe/3 to unsubscribe from all currently subscribed partitions.
-callback assignments_revoked(pid()) -> ok.

%% Called when making join request. This metadata is to let group leader know
%% more details about the member. e.g. its location and or capacity etc.
%% so that leader can make smarter decisions when assigning partitions to it.
-callback user_data(pid()) -> binary().

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
