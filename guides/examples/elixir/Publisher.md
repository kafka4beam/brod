# Publisher Example

> #### Info {: .info}
>
> There is also a more complete example [here](https://github.com/kafka4beam/brod/tree/master/contrib/examples/elixir).

Ensure `:brod` is added to your deps on `mix.exs`

```elixir
defp deps do
    [
      {:brod, "~> 3.10.0"}
    ]
end
```

## Client Configuration

To use producers, you have to start a client first.

You can do that by adding the following configuration (e.g. into `config/dev.exs`):

```elixir
import Config

config :brod,
  clients: [
    kafka_client: [
      endpoints: [localhost: 9092],
      auto_start_producers: true,
      # The following :ssl and :sasl configs are not
      # required when running kafka locally unauthenticated
      ssl: true,
      sasl: {
        :plain,
        System.get_env("KAFKA_CLUSTER_API_KEY"),
        System.get_env("KAFKA_CLUSTER_API_SECRET")
      }
    ]
  ]
```

or by starting it dynamically with this snippet (you can also add SSL/SASL configuration if you want to):

```elixir
:brod.start_client([localhost: 9092], :kafka_client, auto_start_producers: true)
```

_Note:_ `kafka_client` can be any valid atom. And `:endpoints` accepts multiple host port tuples (e.g. `endpoints: [{"192.168.0.2", 9092}, {"192.168.0.3", 9092}, ...]`).

If you don't pass the `auto_start_producers: true` option, you also have to manually start producers before calling `:brod.produce_sync/5` (and other produce functions).
For example like this: `:brod.start_producer(:kafka_client, "my_topic", [])`.

See `:brod.start_client/3` for a list of all available options.

## Publisher

To send a message with brod we can use the `:brod.produce_sync/5` function

```elixir
defmodule BrodExample.Publisher do
  def publish(topic, partition, key, message) do
    :brod.produce_sync(:kafka_client, topic, :hash, key, message)
  end
end
```

There are also other ways (functions) how to produce messages, you can find them in the [overview](https://hexdocs.pm/brod/readme.html#producers) and in the `brod`
module documentation.

### Using partition key

When providing `:hash` as the _partition_ when calling `:brod.produce_sync/5` is equivalent to the following:

```elixir
{:ok, count} = :brod.get_partitions_count(:kafka_client, topic)
partition = rem(:erlang.phash2(key), count)
:brod.produce_sync(:kafka_client, topic, partition, key, message)
```

Internally brod will get the partition count, generate a hash for the key within the range of partitions,
and publish the message to the calculated hash. This is the same sticky routing that Kafka's [ProducerRecord](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html) implements:

> If no partition is specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is present a partition will be assigned in a round-robin fashion.
