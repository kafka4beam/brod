# Publisher Example

Ensure `:brod` is added to your deps on `mix.exs`

```elixir
defp deps do
    [
      {:brod, "~> 3.10.0"}
    ]
end
```

## Client Configuration

Adding the following configuration (e.g. `config/dev.exs`)  

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

_Note:_ `kafka_client` can be any valid atom. And `:endpoints` accepts multiple host port tuples (e.g. `endpoints: [{"192.168.0.2", 9092}, {"192.168.0.3", 9092}, ...]`).   

## Publisher

To send a message with brod we can use the `:brod.produce_sync/5` function

```elixir
defmodule BrodExample.Publisher do
  def publish(topic, partition, key, message) do
    :brod.produce_sync(:kafka_client, topic, :hash, key, message)
  end
end
```

### Using partition key

When providing `:hash` as the _partition_ when calling `:brod.produce_sync/5` is equivalent to the following: 

```elixir
{:ok, count} = :brod.get_partitions_count(:kafka_client, topic)
:brod.produce_sync(:kafka_client, topic, :erlang.phash2(key, count), key, message)
```

Internally brod will get the partition count, generate a hash for the key within the range of partitions,
and publish the message to the calculated hash. This is the same sticky routing that Kafka's [ProducerRecord](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html) implements:

>If no partition is specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is present a partition will be assigned in a round-robin fashion.