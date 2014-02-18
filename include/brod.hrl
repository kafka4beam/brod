-ifndef(__BROD_HRL).
-define(__BROD_HRL, true).

-record(message, { offset     :: integer()
                 , crc        :: integer()
                 , magic_byte :: integer()
                 , attributes :: integer()
                 , key        :: binary()
                 , value      :: binary()
                 }).

%% delivered to subsriber by brod_consumer
-record(message_set, { topic          :: binary()
                     , partition      :: integer()
                     , high_wm_offset :: integer()
                     , messages       :: [#message{}]
                     }).

-endif. % include brod.hrl

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:

