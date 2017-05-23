-module(brod_auth_backend).

-callback auth(Host :: string(), Sock :: gen_tcp:socket() | ssl:sslsocket(),
  Mod :: atom(), ClientId :: binary(), Timeout :: pos_integer(),
  SaslOpts :: term()) -> ok | {error, Reason :: term()}.

-export([auth/7]).

-spec auth(CallbackModule :: atom(), Host :: string(),
  Sock :: gen_tcp:socket()| ssl:sslsocket(), Mod :: atom(),
  ClientId :: binary(), Timeout :: pos_integer(),
  SaslOpts :: term()) -> ok | {error, Reason :: term()}.
auth(CallbackModule, Host, Sock, Mod, ClientId, Timeout, SaslOpts) ->
  CallbackModule:auth(Host, Sock, Mod, ClientId, Timeout, SaslOpts).