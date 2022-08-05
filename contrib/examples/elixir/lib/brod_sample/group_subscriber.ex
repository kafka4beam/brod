defmodule BrodSample.GroupSubscriber do
  require Logger
  require Record
  import Record, only: [defrecord: 2, extract: 2]
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  def child_spec(_arg) do
    %{
      id: BrodSample.GroupSubscriber,
      start: {BrodSample.GroupSubscriber, :start, []}
    }
  end

  def start() do
    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      rejoin_delay_seconds: 2,
      reconnect_cool_down_seconds: 10
    ]

    {:ok, _subscriber} =
      :brod.start_link_group_subscriber(
        :kafka_client,
        "consumer-group-name",
        ["sample"],
        group_config,
        _consumer_config = [begin_offset: :earliest],
        _callback_module = __MODULE__,
        _callback_init_args = []
      )
  end

  def init(_group_id, _callback_init_args) do
    {:ok, []}
  end

  def handle_message(
        _topic,
        _partition,
        {:kafka_message, _offset, _key, body, _op, _timestamp, []} = _message,
        state
      ) do
    Logger.info("Message #{body}")
    Logger.info("Message #{inspect(state)}")

    case body do
      "error_bodyy" -> :error
      _ -> {:ok, :ack, state}
    end
  end
end
