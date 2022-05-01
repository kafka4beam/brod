# Consumer Example

Ensure `:brod` is added to your deps on `mix.exs`

```elixir
defp deps do
    [
      {:brod, "~> 3.10.0"}
    ]
end
```

## Consumer

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
      start: {brod_group_subscriber_v2, :start_link, [config]},
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

See `brod_group_subscriber_v2:start_link/1` for details on the configuration options.

See docs for more details about the required or optional callbacks.
