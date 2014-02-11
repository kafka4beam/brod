%%%=============================================================================
%%% @doc
%%% @copyright 2014 Klarna AB
%%% @end
%%%=============================================================================

-module(brod).

%% API
-export([ connect/3
        , close_connection/1
        , produce/3
        , produce/5
        ]).

%%%_* Includes -----------------------------------------------------------------
-include_lib("brod/include/brod.hrl").

%%%_* Types --------------------------------------------------------------------

%%%_* API ----------------------------------------------------------------------
%% @doc Open connection to kafka broker
%%      Hosts: list of "bootstrap" kafka nodes, {"hostname", 1234}
%%      RequiredAcks: how many kafka nodes must acknowledge a message
%%      before sending a response
%%      Timeout: maximum time in milliseconds the server can await the
%%      receipt of the number of acknowledgements in RequiredAcks
-spec connect([{string(), integer()}], integer(), integer()) ->
                 {ok, pid()} | {error, any()}.
connect(Hosts, RequiredAcks, Timeout) ->
  brod_srv:start_link(Hosts, RequiredAcks, Timeout).

%% @doc Close connection to kafka broker
-spec close_connection(pid()) -> ok.
close_connection(Pid) ->
  brod_srv:stop(Pid).

%% @equiv produce(Pid, Topic, 0, <<>>, Value)
-spec produce(pid(), binary(), binary()) -> ok | {error, any()}.
produce(Pid, Topic, Value) ->
  produce(Pid, Topic, 0, <<>>, Value).

%% @doc Send message to a broker.
%%      Internally it's a gen_server:call to brod_srv process. Returns
%%      when the message is handled by brod_srv.
-spec produce(pid(), binary(), integer(), binary(), binary()) ->
                 ok | {error, any()}.
produce(Pid, Topic, Partition, Key, Value) ->
  brod_srv:produce(Pid, Topic, Partition, Key, Value).

%%%_* Internal functions -------------------------------------------------------

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
