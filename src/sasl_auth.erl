-module(sasl_auth).

-define(APPLICATION, begin {ok, A} = application:get_application(?MODULE), A end).

%% API
-export([init/0, kinit/2, sasl_client_init/0, sasl_client_new/3, sasl_listmech/0, sasl_client_start/0, sasl_client_step/1, sasl_errdetail/0]).

init() ->
    erlang:load_nif(filename:join(code:priv_dir(?APPLICATION), "brod"), 0).

kinit(_, _) -> exit(nif_library_not_loaded).

sasl_client_init() -> exit(nif_library_not_loaded).

sasl_client_new(_, _, _) -> exit(nif_library_not_loaded).

sasl_listmech() -> exit(nif_library_not_loaded).

sasl_client_start() -> exit(nif_library_not_loaded).

sasl_client_step(_) -> exit(nif_library_not_loaded).

sasl_errdetail() ->  exit(nif_library_not_loaded).
