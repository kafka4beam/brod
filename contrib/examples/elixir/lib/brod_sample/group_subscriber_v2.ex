defmodule BrodSample.GroupSubscriberV2 do
  @behaviour :brod_group_subscriber_v2
  require Logger

  def start() do
    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      rejoin_delay_seconds: 2,
      reconnect_cool_down_seconds: 10
    ]

    config = %{
      client: :kafka_client,
      group_id: "from_zero",
      topics: ["sample"],
      cb_module: __MODULE__,
      group_config: group_config,
      consumer_config: [begin_offset: :earliest]
    }

    :brod.start_link_group_subscriber_v2(config)
  end

  def init(_arg, _arg2) do
    IO.inspect("init/2")
    {:ok, []}
  end

  def handle_message(message, state) do
    IO.inspect(message, label: "message")
    {_kafka_message_set, _content, partition, _unkow, _set} = message
    IO.inspect(partition, label: "Partition")
    IO.inspect(state, label: "state")
    IO.inspect(self(), label: "pid")
    IO.puts("Waiting")
    # :timer.sleep(10000)
    IO.puts("acking")

    case partition do
      # 1 -> {:error}
      _ -> {:ok, :commit, []}
    end

    # {:ok, :commit, []}
    # {:ok, :no_ack, []}
  end

  def terminate() do
    IO.inspect("DOWN")
  end

  # assign_partitions/3, get_committed_offset/3.
end
