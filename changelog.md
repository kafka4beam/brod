* 2.4.0 Brod-cli
* 2.4.1 Brod-cli typo fix
* 2.5.0
  - Pluggable SASL authentication backend (contributor: ElMaxo)
  - Brod-cli support extra ebin to code path
  - Fix group subscriber duplicated loopback messages (bug)
  - SASL-PLAIN username password in text file
  - Hide SASL-PLAIN password in an anonymous function in `brod_client` state
* 3.0.0
  * backward-incompatible changes
    - #kafka_message.crc changed from signed to unsigned integer.
    - `brod:get_metadata` return value changed from #kpro_MetadataResponse{} record to kpro:struct().
    - `brod:get_offsets` API replaced with `brod:resolve_offset`.
       Reason: The `brod:get_offsets` and `brod_utils:fetch_offsets` are very confusing,
               because they look like fetching consumer group committed offsets.
               Also, the return value has been changed from a list of offsets to a single offset.
    - --count option removed from brod-cli offset command
    - `brod_utils:fetch/4` is removed, use make_fetch_fun/8 instead
  * backward-compatible changes
    - `#kafka_message{}` record is extended with new fields `ts_type` and `ts`

