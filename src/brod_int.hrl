%%%
%%%   Copyright (c) 2014-2016, Klarna AB
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

-ifndef(__BROD_INT_HRL).
-define(__BROD_INT_HRL, true).

-include("brod.hrl").
-include_lib("kpro/include/kpro.hrl").

-type topic()     :: kafka_topic().
-type partition() :: kafka_partition().
-type offset()    :: kafka_offset().
-type client_id() :: brod_client_id().

-type consumer_option() :: begin_offset
                         | min_bytes
                         | max_bytes
                         | max_wait_time
                         | sleep_timeout
                         | prefetch_count.

-type hostname()         :: string().
-type portnum()          :: pos_integer().
-type endpoint()         :: {hostname(), portnum()}.
-type cluster_id()       :: atom().
-type leader_id()        :: non_neg_integer().
-type corr_id()          :: 0..?MAX_CORR_ID.
-type kafka_kv()         :: {binary(), binary()}.
-type client_config()    :: brod_client_config().
-type producer_config()  :: brod_producer_config().
-type consumer_config()  :: brod_consumer_config().
-type consumer_options() :: [{consumer_option(), integer()}].
-type client()           :: client_id() | pid().
-type required_acks()    :: -1..1.

-record(socket, { pid     :: pid()
                , host    :: string()
                , port    :: integer()
                , node_id :: integer()
                }).

%%%_* metadata request ---------------------------------------------------------

-define(undef, undefined).

-define(DEFAULT_CLIENT_ID, brod).

-endif. % include brod_int.hrl

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
