defmodule BrodSample.Publisher do
  def publish(topic, partition_key, message) do
    {:ok, count} = :brod.get_partitions_count(:kafka_client, topic)

    :brod.produce_sync(
      :kafka_client,
      topic,
      :erlang.phash2(partition_key, count),
      partition_key,
      message
    )
  end

  def publish_sample_messages(topic, n) do
    0..n
    |> Task.async_stream(fn n ->
      :brod.produce(
        :kafka_client,
        topic,
        n,
        "oi",
        "Message number #{n}"
      )
    end)
    |> Enum.to_list()
  end
end
