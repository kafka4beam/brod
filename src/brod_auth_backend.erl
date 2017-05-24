%%%
%%%   Copyright (c) 2017, Klarna AB
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

-module(brod_auth_backend).

-export([auth/7]).

-callback auth(Host :: string(), Sock :: gen_tcp:socket() | ssl:sslsocket(),
               Mod :: gen_tcp | ssl, ClientId :: binary(),
               Timeout :: pos_integer(), SaslOpts :: term()) ->
                 ok | {error, Reason :: term()}.

-spec auth(CallbackModule :: atom(), Host :: string(),
           Sock :: gen_tcp:socket() | ssl:sslsocket(),
           Mod :: gen_tcp | ssl, ClientId :: binary(),
           Timeout :: pos_integer(), SaslOpts :: term()) ->
            ok | {error, Reason :: term()}.
auth(CallbackModule, Host, Sock, Mod, ClientId, Timeout, SaslOpts) ->
  CallbackModule:auth(Host, Sock, Mod, ClientId, Timeout, SaslOpts).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
