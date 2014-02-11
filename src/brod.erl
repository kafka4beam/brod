%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).

%% API
-export([ start_producer/3
        , stop_producer/1
        , produce/3
        , produce/4
        , produce/5
        ]).

%%%_* Includes -----------------------------------------------------------------
-include_lib("brod/include/brod.hrl").

%%%_* Types --------------------------------------------------------------------

%%%_* API ----------------------------------------------------------------------
%% @doc Start a process which manages connections to kafka brokers and
%%      specialized in publishing messages.
%%      Hosts: list of "bootstrap" kafka nodes, {"hostname", 1234}
%%      RequiredAcks: how many kafka nodes must acknowledge a message
%%      before sending a response
%%      Timeout: maximum time in milliseconds the server can await the
%%      receipt of the number of acknowledgements in RequiredAcks
-spec start_producer([{string(), integer()}], integer(), integer()) ->
                        {ok, pid()} | {error, any()}.
start_producer(Hosts, RequiredAcks, Timeout) ->
  brod_producer:start_link(Hosts, RequiredAcks, Timeout).

%% @doc Stop producer process
-spec stop_producer(pid()) -> ok.
stop_producer(Pid) ->
  brod_producer:stop(Pid).

%% @equiv produce(Pid, Topic, 0, <<>>, Value)
-spec produce(pid(), binary(), binary()) -> ok | {error, any()}.
produce(Pid, Topic, Value) ->
  produce(Pid, Topic, 0, Value).

%% @equiv produce(Pid, Topic, Partition, <<>>, Value)
-spec produce(pid(), binary(), integer(), binary()) -> ok | {error, any()}.
produce(Pid, Topic, Partition, Value) ->
  produce(Pid, Topic, Partition, <<>>, Value).

%% @doc Send message to a broker.
%%      Internally it's a gen_server:call to brod_srv process. Returns
%%      when the message is handled by brod_srv.
-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 ok | {error, any()}.
produce(Pid, Topic, Partition, Key, Value) ->
  brod_producer:produce(Pid, Topic, Partition, Key, Value).

%%%_* Internal functions -------------------------------------------------------

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
