* 2.4.0 Added brod-cli
* 2.4.1 Fixed brod-cli typo fix
* 2.5.0
  - Pluggable SASL authentication backend (contributor: ElMaxo)
  - Brod-cli support extra ebin to code path
  - Fix group subscriber duplicated loopback messages (bug)
  - SASL-PLAIN username password in text file
  - Hide SASL-PLAIN password in an anonymous function in `brod_client` state
* 2.5.1
  - Fix ignored commit history when committed offset is 0 (bug)
* 3.0.0
  * New Features
    - New API `brod:connect_group_cordinator` to estabilish a sockt towards group coordinator broker.
    - New API `brod:fetch_committed_offsets` to fetch consumer group commited offsets.
    - New API `brod:list_groups`, `brod:list_all_groups` and `brod:describe_groups/3`.
    - Brod-cli new command `groups` to list / describe consumer groups.
    - Brod-cli new command `commits` to list comsuer group committed offsets.
  * Backward-incompatible changes
    - `brod:get_offsets` API replaced with `brod:resolve_offset`.
       Reason: `brod:get_offsets` and `brod_utils:fetch_offsets` are very confusing,
               because they look like fetching consumer group committed offsets.
               Also, the return value has been changed from a list of offsets to a single offset.
    - `brod:get_metadata` return value changed from `#kpro_MetadataResponse{}` record to `kpro:struct()`.
    - `brod_utils:fetch/4` is removed, use `make_fetch_fun/8` instead.
    - `--count` option removed from `brod-cli` `offset` command,
      The command now resolves only one (or maybe none) offset at a time.
    - All type specs defined in `brod.hrl` are moved to exported types from `brod.erl`
  * Backward-compatible changes
    - `#kafka_message{}` record is extended with new fields `ts_type` and `ts`.
    - `#kafka_message.crc` changed from signed to unsigned integer.
* 3.1.0
  * New Features
    - New message type option for topic and group subscribers that specifies whether to
      handle a Kafka `message` or `message_set`.
* 3.2.0
  * New Features
    - Support produce_request version 0 - 2
    - Support fetch_request version 0 - 3
    - Support offsets_request version 0 - 1
    - Support metadata_request version 0 - 2
    - Support message create time by allowing `Value` to be `[{Ts, Key, Value}]` in `brod:produce` and `brod:produce_sync` APIs
    - Bind all `#kafka_message{}` fields to variables so they can be used in brod-cli `fetch` command's `--fmt` expression
  * Bug Fixes
    - Remove sasl application from dependent app list
* 3.3.0 (not tagged yet)
  * New Features
    - Support offset_fetch_request version 1 - 2
    - Provide APIs to reset committed offsets
    - Add '--reset' option to brod-cli 'commits' command
  * Bug Fixes
    - Fixed brod-cli offset_fetch_request version when working with kafka 0.10.1.x or earlier

