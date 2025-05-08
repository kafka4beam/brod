# Changelog

- 4.4.1
  - Upgrade `kafka_protocol` from 4.2.3 to 4.2.5 for:
    - `crc32c` performance improvements on ARM.
  - Replace self messaging in `init` with `handle_continue`

- 4.4.0
  - Support `zstd` compression.
  - Upgrade `kafka_protocol` from 4.1.10 to 4.2.3 for:
    - logging improvements
    - `crc32c` performance improvements on x86

- 4.3.3
  - Catch `exit` exception when group coordinator calls group member (owner) process.
  - Add `unknown_topic_cache_ttl` option in client config.
  - Doc improvements.

- 4.3.2
  - Upgrade `kafka_protocol` from 4.1.9 to 4.1.10 for partition leader discover/connect timeout fix.

- 4.3.1
  - Fixed `brod_client:stop_consumer` so that it doesn't crash the client process if an unknown consumer is given as argument.
  - Previously, `brod_group_subscriber_v2` could leave `brod_consumer` processes lingering even after its shutdown.  Now, those processes are terminated.

- 4.3.0
  - Split brod-cli out to a separate project [kafka4beam/brod-cli](https://github.com/kafka4beam/brod-cli)

- 4.2.0
  - Optimize consumer fetch latency.
    Introduced the `share_leader_conn` consumer configuration option (default: `false`).
    This setting allows users to opt for the previous behavior if preferred (set to `true`).

- 4.1.1
  - Upgrade `kafka_protocol` from version 4.1.5 to 4.1.9.

- 4.1.0
  - For v2 subscribers, pass offset `ack_fun` as a callback state init parameter.
    This makes it possible for callback implementation to acknowledge offsets easier.
    [PR#591](https://github.com/kafka4beam/brod/pull/591)
  - Type spec fix. [PR#590](https://github.com/kafka4beam/brod/pull/590)
  - Fix `brod:fold/8` to avoid exception when a stable offset points to en empty batch.
    [PR#589](https://github.com/kafka4beam/brod/pull/589)

- 4.0.0
  - Remove `snappyer` from default dependency [PR#547](https://github.com/kafka4beam/brod/pull/547).
    Starting from `kafka_protocol-4.0`, it no longer requires compression libraries as rebar dependencies.
    Now `brod-4.0` has it removed too.
    For more compression options, see [kafka_protocol/README](https://github.com/kafka4beam/kafka_protocol/blob/master/README.md#compression-support)

- 3.19.1
  - Made brod-cli to work on OTP 26. [PR#582](https://github.com/kafka4beam/brod/pull/582)
    - `--ssl` option is now mandatory if TLS is to be used (previously it can be derived from `--cacertfile` option)
    - TLS version defaults to 1.2, added `--ssl-versions` to support explictly setting TLS 1.3
  - Support building on OTP 27 [PR#585](https://github.com/kafka4beam/brod/pull/585)
    Also fixed rebar.lock for dependency `kafka_protocol-4.1.5`
  - Guard against crashes in `brod_client:get_consumer/3` [PR#587](https://github.com/kafka4beam/brod/pull/587) (originally [PR#581](https://github.com/kafka4beam/brod/pull/581))

- 3.19.0
  - Forward unhandled messages in topic/group consumer processes to handle_info/2 callbacks
    in order to support arbitrary message passing [PR#580](https://github.com/kafka4beam/brod/pull/580)

- 3.18.0
  - Add transactional APIs. [PR#549](https://github.com/kafka4beam/brod/pull/549)
  - Fix unnecessary group coordinator restart due to `hb_timeout` exception. [PR#578](https://github.com/kafka4beam/brod/pull/578)
  - Changed supervisor3 progress log level from `info` to `debug`. [PR#572](https://github.com/kafka4beam/brod/pull/572)
  - Type spec fix. [PR#571](https://github.com/kafka4beam/brod/pull/571)
  - Remove unused macro. [PR#575](https://github.com/kafka4beam/brod/pull/575)

- 3.17.1
  - Upgrade `kafka_protocol` from 4.1.3 to 4.1.5
    - Allow space after `,` in comma-separated bootstrapping host:port list
    - Avoid `badmatch` exception when parsing SASL password file

- 3.17.0
  - Deleted `supervisor3` as dependency, module is now moved to brod.git named `brod_supervisor3`.
  - Upgrade `snappyer` version from 1.2.8 to 1.2.9.
  - Upgrade `kafka_protocol` from 4.1.2 to 4.1.3.
  - In rebar3.config move `coveralls` from `plugins` to `project_plugins`.

- 3.16.8
  - Upgrade `supervisor3` from 1.1.11 to 1.1.12 to log supervisor status
    at `debug` level instead of `notice` level.
  - Add `brod_group_subscriber_v2:get_workers` function to help
    monitor and check the health of a consumer group.

- 3.16.7
  - Upgrade `kafka_protocol` from 4.1.1 to 4.1.2 to allow handling
    multiply nested wrapped secrets.

- 3.16.6
  - Upgrade `kafka_protocol` from 4.1.0 to 4.1.1 to support defining
    SNI for bootstrap hosts without the need to set the `verify_peer`
    config.

- 3.16.5
  - Improve the documentation
  - Add `brod:get_partitions_count_safe/2`.
    It is ensured not to auto-create topics in Kafka even
    when Kafka has topic auto-creation configured.
  - Treat `offset = 0` commit record as `earliest`.
    This is an issue introduced in 3.16.4 which supported subscriber
    managed offset commits to be represented as `{begin_offset, Offset}`.
    The issue is: `0` is altered by v2 group worker to `-1`.
    Handled as offsets greater than 0, v2 group worker assums `brod_topic_subscriber`
    would apply `+1` to it, however `-1` is actually interpreted as `latest`.
    [#531](https://github.com/kafka4beam/brod/pull/531) fixes it
    by replacing `0` with `earliest`.

- 3.16.4
  - Allow special begin_offset from consumer managed commits.
  - Fix specs for `brod_group_subscriber_v2.get_committed_offset`
  - Update kafka_protocol from 4.0.3 to 4.1.0. kafka_protocol 4.1.0 support a
    different version of the auth plugin interface that also pass the
    handshake version to the plugin. This is used by the SASL GSSAPI Kerberos
    plugin brod_gssapi (<https://github.com/kafka4beam/brod_gssapi>) so that
    the correct handshake protocol is used. The change is backwards compatible
    as the old interface will be used if the plugin module does not have a
    function with the name and arity that is required by the new interface.
    See <https://github.com/kafka4beam/kafka_protocol/pull/102> for details.
- 3.16.3
  - Fix specs for `delete_topics`.
  - Ensure that partition worker process is alive before returning it.
  - Update kafka_protocol from 4.0.1 to 4.0.3 in rebar.lock file.
  - Make consumer 'isolation_level' configurable.
- 3.16.2
  - Update kafka_protocol from 4.0.1 to 4.0.3.
    Prior to this change the actual time spent in establishing a
    Kafka connection might be longer than desired due to the timeout
    being used in SSL upgrade (if enabled), then API version query.
    This has been fixed by turning the given timeout config
    into a deadline, and the sub-steps will try to meet the deadline.
    see more details here: <https://github.com/kafka4beam/kafka_protocol/pull/92>
  - Catch `timeout` and other `DOWN` reasons when making `gen_server` call to
    `brod_client`, `brod_consumer` and producer/consumer supervisor,
    and return as `Reason` in `{error, Reason}`.
    Previously only `noproc` reason is caught. (#492)
  - Propagate `connect_timeout` config to `kpro` API functions as `timeout` arg
    affected APIs: connect_group_coordinator, create_topics, delete_topics,
    resolve_offset, fetch, fold, fetch_committed_offsets (#458)
  - Fix bad field name in group describe request (#486)
- 3.16.1
  - Fix `brod` script in `brod-cli` in release.
  - Support `rebalance_timeout` consumer group option
- 3.16.0
  - Update to kafka-protocol v4.0.1
- 3.15.6
  - Eliminate long-lived anonymous function closures
- 3.15.5
  - Fix exponential growth of `brod_producer` buffer
- 3.15.4
  - Avoid start_link for temp connection usages
    affected APIs: fetch_committed_offsets, fetch, resolve_offset, create_topics, delete_topics
- 3.15.3
  - Try to commit acknowledged offsets when brod_group_subscriber_v2 terminates
  - Fix process leak, close connection after offset is fetched in brod_utils:fetch_committed_offsets/3
- 3.15.2
  - Producer: Do not format producer buffer in `gen_server` state
  - Consumer: Do not commit offsets when `unknown_member_id` error is received
  - Logging: Changed from `error_logger` to `logger` macros
  - Don't shut down brod_group_subscriber_v2 on previous generation worker termination
  - Fix brod_group_subscriber_v2 crash on shutdown
  - Use GitHub Actions instead of Travis CI
  - Added Elixir example
  - Rename DEFAULT_TIMEOUT macro in public brod.hrl to BROD_DEFAULT_TIMEOUT.
- 3.15.0 (and 3.15.1, for some reason tagged on the same commit)
  - Fix unknown types
  - Build on OTP-23
- 3.14.0
  NOTE: This release changes internal states of brod worker processes
  in a way that cannot by applied on a running system. brod application
  and all brod workers should be shot down for the time of the upgrade.

  - Introduced a new optional terminate/2 callback to
    brod_topic_subscriber and brod_group_subscriber_v2 behaviors

  - Introduced new API functions for starting topic subscribers:
    brod_topic_subscriber:start_link/1 and brod:start_link_topic_subscriber/1
    Old APIs
    - brod:start_link_topic_subscriber/5,
    - brod:start_link_topic_subscriber/6
    - brod:start_link_topic_subscriber/7
    - brod_topic_subscriber:start_link/6
    - brod_topic_subscriber:start_link/7
    - brod_topic_subscriber:start_link/8
      are deprecated and will be removed in the next major release

- 3.13.0
  - Update supervisor3 dependency to 1.1.11
  - brod_group_subscriber_v2 behavior handles worker crashes
  - Makefile fix for hex publishing
  - Reverse Changelog order
  - Fix typos and dialyzer warnings
- 3.12.0
  - Enable passing custom timeout to resolve_offset
- 3.11.0
  - Improve compatibility with EventHub
- 3.10.0
  - Stop supporting erlang.mk
  - Stop supporting rebar
  - Update kafka_protocol dependency to 2.3.6
  - Add new brod_group_coordinator:update_topics API
- 3.9.5
  - Bump kafka_protocol dependency to 2.3.3
- 3.9.4
  - Handle undefined fetcher in fold loop exception
- 3.9.3
  - Remove vsn-check dependency from default Makefile target
    This enables using brod with erlang.mk + hex
  - Bump kafka_protocol dependency to 2.3.2
- 3.9.2
  - Fix corrupted package published to hex
- 3.9.1
  - Receive pending acks after assignments_revoked is invoked
- 3.9.0
  - Updated kafka_protocol dependency to 2.3.1
  - (Experimental) Added group_subscriber_v2 behavior
  - Added API for topic deletion and creation
- 3.8.1
  - Handle the case when high_watermark < last_stable_offset in fetch resp
- 3.8.0
  - Bump to kafka_protocol 2.2.9 (allow `atom()` hostname)
  - Add `brod:fold/8`. This API spawns a process to fetch-ahead while folding the previously
    fetched batch. `brod-cli`'s `fetch` command is updated to call this `fold` API for better
    performance.
  - Add callbacks to allow `brod_client:stop_producer` and `brod_client:stop_consumer` to remove
    the stopped child references from the supervisor and clean up the client ets table to allow
    later restart.
  - Support scram SASL authentication in brod-cli
  - Made possible to start-link or supervise `brod_consumer` in user apps, instead of always
    under `brod_client`'s `brod_consumers_sup`
- 3.7.11
  - Fix a bug when dropping aborted transactions for compacted topics
- 3.7.10
  - Compare begin_offset with last stable offset before advancing to next offset in case empty
    batch is received. Prior to this version, fetch attempts on unstable messages (messages
    belong to open transactions (transactions which are neither committed nor aborted),
    may result in an empty message set, then `brod_consumer` or `brod_utils:fetch` jumps to
    the next offset (if it is less than high-watermark offset).
- 3.7.9
  - Fix brod-cli escript include apps
  - Fix brod-cli sub-record formatting crash
  - Upgrade to kafka_protocol 2.2.8 to discard replica_not_available error code in metadata response
  - Fix empty responses field in fetch response #323
- 3.7.8
  - Drop batches in aborted transactions (and all control batches)
    also improve offset fast-forwarding when empty batches are received
- 3.7.7
  - Fix `badrecord` race: message-set is delivered to `brod_group_subscriber` after
    unsubscribed from `brod_consumer`.
- 3.7.6
  - Fix produce message input type spec to make it backward compatible (bug introduced in 3.7.3)
- 3.7.5
  - Bump kafka_protocol version to 2.2.7
  - Fix empty assignment handling. In case a group member has no partition assigned,
    `member_assignment` data field in group sync response can either be `null` (kafka 0.10)
    or a struct having empty `topic_partitions` (kafka 0.11 or later). The later case
    was not handled properly in `brod` before this fix.
- 3.7.4
  - Add callback to make user_data in group join request
- 3.7.3
  - Bump kafka_protocol version to 2.2.3
  - Discard stale async-ack messages to group subscriber
- 3.7.2
  - Pr #298: Subscriber now automatically reconnects to Kafka on topic rebalances where
    the previous partition leader no longer holds the partition at all.
  - Pr #299: Fix topic subscriber returning partition offsets from callback module's init.
- 3.7.1
  - Fix brod_topic_subscriber and brod_group_subscriber re-subscribe behaviour
    to avoid fetching duplicated messages.
  - Add 'random' and 'hash' partitioner for produce APIs
  - Allow `brod_group_subscrber:assign_partitions/3` to return an updated cb_state
  - `brod_client:get_connection` is back
  - Make it possible to run tests on both mac and linux
  - Position group leader member at the head of members list when assigning partitions
- 3.7.0
  - Add `brod_group_subscriber:ack/5` and `brod_group_subscriber:commit/4` to let group subscribers
    commit offsets asynchronously
  - Pr #284: In compacted topics, Kafka may return a MessageSet which contains only
    messages before the desired offset. Just keep reading forward in this case.
  - Issue #285 `brod_consumer` no longer restart on `leader_not_availble` and `not_leader_for_partition`
    error codes received in fetch response. It resets connection and rediscover leader after delay.
- 3.6.2
  - Allow `brod_topic_subscriber` to explicitly start consuming from partition offset 0 (by passing in
    a committed offset of -1).
- 3.6.1
  - Make produce request version configurable as `produce_req_vsn` in `brod:producer_config()`
  - Upgrade `kafka_protocol` to `2.1.2` to support alpine/busybox build
- 3.6.0
  - Moved 3 modules to kafka_protocol:
    - `brod_sock` -> `kpro_connection`
    - `brod_auth_backed` -> `kpro_auth_backend`
    - `brod_kafka_requests` -> `kpro_sent_reqs`
  - `#kafka_message.key` and `#kafka_message.value` are now always `binary()`
    (they were of spec `undefined | binary()` prior to this version).
    i.e. empty bytes are now decoded as `<<>>` instead of `undefined`.
    This may cause dialyzer check failures.
  - `brod_client` no longer logs about metadata socket down, it had been confusing rather than being helpful
  - There is no more cool-down delay for metadata socket re-establishment
  - `brod_group_coordinator` default session timeout changed from 10 seconds to 30,
    and heartbeat interval changed from 2 seconds to 5.
  - Add `brod:produce_cb/4` and `brod:produce_cb/6` to support user defined callback as produce ack handler.
  - Add `brod:produce_no_ack/3` and `brod:produce_no_ack/5`.
  - `min_compression_batch_size` is removed from producer config.
  - Support magic v2 batch/message format (message headers).
  - Use rebar3 as primary build tool for travis-ci, erlang.mk is still supported.
  - Support `SCRAM-SHA-256` and `SCRAM-SHA-512` SASL authentication mechanisms.
- 3.5.2
  - Fix issue #263: Kafka 0.11 may send empty batch in fetch response when messages are deleted in
    compacted topics.
- 3.5.1
  - Add `extra_sock_opts` client socket options.
    It would be helpful for tuning the performance of tcp socket.
- 3.5.0
  - Add `*_offset` variants to `produce` APIs, returning the base offsets that were assigned by Kafka.
    Producers need to be restarted when upgrading to this version.
- 3.4.0
  - Add `prefetch_bytes` consumer config. `brod_consumer` should stop fetch-ahead only when
    both `prefetch_count` and `prefetch_bytes` limits are exceeded
- 3.3.5
  - Fix issue #252 -- Kafka 0.11 and 1.0 have more strict validations on compressed batch wrapper message.
    Changed kafka_protocol 1.1.2 has the wrapper timestamp and offsets fixed.
- 3.3.4
  - Fix issue #247 -- revert the handling of offset = -1 in offset_fetch_response, bug introduced in 3.3.0
    offset = -1 in offset_fetch_response is an indicator of 'no commit' should be ignored (not taken as 'latest')
- 3.3.3
  - Add a --no-api-vsn-query option for brod-cli to support kafka 0.9
  - Bump kafka_protocol to 1.1.1 to fix relative offsets issue
    so brod-cli can fetch compressed batches as expected,
    also brod_consumer can start picking fetch request version
  - Upgrade roundrobin group protocol to roundrobin_v2 to fix offset commit incompatiblility
    with kafka spec and monitoring tools etc. see <https://github.com/klarna/brod/issues/241> for details
- 3.3.2
  - Enhancements
    - Add sh script to wrap brod-cli escript for erts dir auto discover
    - Detailed log for connection estabilishment failures
  - Bug Fixes
    - Demonitor producer pid after sync_produce_request timeout
- 3.3.1
  - Fix brod-cli commits command redandunt socket usage.
- 3.3.0
  - New Features
    - Support offset_fetch_request version 1 - 2
    - Provide APIs to reset committed offsets
    - Support offset commit in brod-cli
    - Improved group coordinator logging: 1) stop showing member ID, 2) show callback process.
    - Cache queried version ranges per kafka host
    - Brod-cli 'offset' command by default resolves offsets for all partitions if '--partition' option is 'all' or missing
  - Enhancements
    - Brod rock with elvis with travis
    - Travis run dialyzer check
  - Bug Fixes
    - Fixed brod-cli offset_fetch_request version when working with kafka 0.10.1.x or earlier
    - Make group coordinator restart on heartbeat timeout
- 3.2.0
  - New Features
    - Support produce_request version 0 - 2
    - Support fetch_request version 0 - 3
    - Support offsets_request version 0 - 1
    - Support metadata_request version 0 - 2
    - Support message create time by allowing `Value` to be `[{Ts, Key, Value}]` in `brod:produce` and `brod:produce_sync` APIs
    - Bind all `#kafka_message{}` fields to variables so they can be used in brod-cli `fetch` command's `--fmt` expression
  - Bug Fixes
    - Remove sasl application from dependent app list
- 3.1.0
  - New Features
    - New message type option for topic and group subscribers that specifies whether to
      handle a Kafka `message` or `message_set`.
- 3.0.0
  - New Features
    - New API `brod:connect_group_cordinator` to establish a sockt towards group coordinator broker.
    - New API `brod:fetch_committed_offsets` to fetch consumer group committed offsets.
    - New API `brod:list_groups`, `brod:list_all_groups` and `brod:describe_groups/3`.
    - Brod-cli new command `groups` to list / describe consumer groups.
    - Brod-cli new command `commits` to list comsuer group committed offsets.
  - Backward-incompatible changes
    - `brod:get_offsets` API replaced with `brod:resolve_offset`.
      Reason: `brod:get_offsets` and `brod_utils:fetch_offsets` are very confusing,
      because they look like fetching consumer group committed offsets.
      Also, the return value has been changed from a list of offsets to a single offset.
    - `brod:get_metadata` return value changed from `#kpro_MetadataResponse{}` record to `kpro:struct()`.
    - `brod_utils:fetch/4` is removed, use `make_fetch_fun/8` instead.
    - `--count` option removed from `brod-cli` `offset` command,
      The command now resolves only one (or maybe none) offset at a time.
    - All type specs defined in `brod.hrl` are moved to exported types from `brod.erl`
  - Backward-compatible changes
    - `#kafka_message{}` record is extended with new fields `ts_type` and `ts`.
    - `#kafka_message.crc` changed from signed to unsigned integer.
- 2.5.1
  - Fix ignored commit history when committed offset is 0 (bug)
- 2.5.0
  - Pluggable SASL authentication backend (contributor: ElMaxo)
  - Brod-cli support extra ebin to code path
  - Fix group subscriber duplicated loopback messages (bug)
  - SASL-PLAIN username password in text file
  - Hide SASL-PLAIN password in an anonymous function in `brod_client` state
- 2.4.1 Fixed brod-cli typo fix
- 2.4.0 Added brod-cli
