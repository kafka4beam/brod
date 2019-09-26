-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-record(state, { is_async_ack
               , is_async_commit
               , is_assign_partitions
               , topic
               , partition
               }).

-define(MSG(Ref, Pid, Topic, Partition, Offset, Value),
        {Ref, Pid, Topic, Partition, Offset, Value}).

-define(CLIENT_ID, ?MODULE).
-define(TOPIC1, <<"brod-group-subscriber-1">>).
-define(TOPIC2, <<"brod-group-subscriber-2">>).
-define(TOPIC3, <<"brod-group-subscriber-3">>).
-define(TOPIC4, <<"brod-group-subscriber-4">>).
-define(GROUP_ID, list_to_binary(atom_to_list(?MODULE))).
-define(config(Name), proplists:get_value(Name, Config)).
