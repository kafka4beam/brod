defmodule BrodSample.GroupSubscriberV2 do
  @behaviour :brod_group_subscriber_v2
  require Logger

  def child_spec(_arg) do
    %{
      id: BrodSample.GroupSubscriberV2,
      start: {BrodSample.GroupSubscriberV2, :start, []}
    }
  end

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

  @impl true
  def init(_arg, _arg2) do
    {:ok, []}
  end

  @impl true
  def handle_message(message, _state) do
    {_kafka_message_set, topic, partition, offset, messages} = message

    Enum.each(messages, fn msg ->
      Logger.info(
        "topic: #{topic}, partition: #{partition}, offset: #{offset}, len_messages: #{inspect(msg)}"
      )
    end)

    case partition do
      # 1 -> {:error}
      _ -> {:ok, :commit, []}
    end
  end
end
