# Consumer Example

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

Both examples require a brod client with name `:kafka_client` to be already started.
You can do that either statically by specifying it in the configuration (see an
[example](https://github.com/kafka4beam/brod/blob/master/contrib/examples/elixir/config/dev.exs))
or dynamically
(e.g. by calling `:brod.start_client([{"localhost", 9092}], :kafka_client)`).

## Group Subscriber

Either the `brod_group_subscriber_v2` or `brod_group_subscriber` behaviours can be used
to consume messages. The key difference is that the v2 subscriber runs a worker for each
partition in a separate Erlang process, allowing parallel message processing.

Here is an example of callback module that implements the `brod_group_subscriber_v2` behaviour to consume messages.

```elixir
defmodule BrodSample.GroupSubscriberV2 do
  @behaviour :brod_group_subscriber_v2

  def child_spec(_arg) do
    config = %{
      client: :kafka_client,
      group_id: "consumer_group_name",
      topics: ["streaming.events"],
      cb_module: __MODULE__,
      consumer_config: [{:begin_offset, :earliest}],
      init_data: [],
      message_type: :message_set,
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 5,
        rejoin_delay_seconds: 60,
        reconnect_cool_down_seconds: 60
      ]
    }

    %{
      id: __MODULE__,
      start: {:brod_group_subscriber_v2, :start_link, [config]},
      type: :worker,
      restart: :temporary,
      shutdown: 5000
    }
  end

  @impl :brod_group_subscriber_v2
  def init(_group_id, _init_data), do: {:ok, []}

  @impl :brod_group_subscriber_v2
  def handle_message(message, _state) do
    IO.inspect(message, label: "message")
    {:ok, :commit, []}
  end
end
```

The example module implements `child_spec/1` so that our consumer can be started by a Supervisor. The restart policy is set to `:temporary`
because, in this case, if a message can not be processed, then there is no point in restarting. This might not always
be the case.

See `:brod_group_subscriber_v2.start_link/1` for details on the configuration options.

See docs for more details about the required or optional callbacks.

## Partition Subscriber

A more low-level approach can be used when you want a more fine-grained control or when you have only a single partition.

```elixir
defmodule BrodSample.PartitionSubscriber do
  use GenServer

  import Record, only: [defrecord: 2, extract: 2]

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message_set, extract(:kafka_message_set, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_fetch_error, extract(:kafka_fetch_error, from_lib: "brod/include/brod.hrl")

  defmodule State do
    @enforce_keys [:consumer_pid]
    defstruct consumer_pid: nil
  end

  defmodule KafkaMessage do
    @enforce_keys [:offset, :key, :value, :ts]
    defstruct offset: nil, key: nil, value: nil, ts: nil
  end

  def start_link(topic, partition) do
    GenServer.start_link(__MODULE__, {topic, partition})
  end

  @impl true
  def init({topic, partition}) do
    # start the consumer(s)
    # if you have more than one partition, do it somewhere else once for all paritions
    # (e.g. in the parent process)
    :ok = :brod.start_consumer(:kafka_client, topic, begin_offset: :latest)

    {:ok, consumer_pid} = :brod.subscribe(:kafka_client, self(), topic, partition, [])
    # you may also want to handle error when subscribing
    # and to monitor the consumer pid (and resubscribe when the consumer crashes)

    {:ok, %State{consumer_pid: consumer_pid}}
  end

  @impl true
  def handle_info(
        {consumer_pid, kafka_message_set(messages: msgs)},
        %State{consumer_pid: consumer_pid} = state
      ) do
    for msg <- msgs do
      msg = kafka_message_to_struct(msg)

      # process the message...
      IO.inspect(msg)

      # and then acknowledge it
      :brod.consume_ack(consumer_pid, msg.offset)
    end

    {:noreply, state}
  end

  def handle_info({pid, kafka_fetch_error()} = error, %State{consumer_pid: pid} = state) do
    # you may want to handle the error differently
    {:stop, error, state}
  end

  defp kafka_message_to_struct(kafka_message(offset: offset, key: key, value: value, ts: ts)) do
    %KafkaMessage{
      offset: offset,
      key: key,
      value: value,
      ts: DateTime.from_unix!(ts, :millisecond)
    }
  end
end
```