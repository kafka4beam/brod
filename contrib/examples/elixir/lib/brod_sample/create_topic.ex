defmodule BrodSample.TopicManagement do
  def create(name) do
    topic_config = [
      %{
        name: name,
        num_partitions: 6,
        replication_factor: 1,
        assignments: [],
        configs: []
      }
    ]

    :brod.create_topics(
      [{"localhost", 9092}],
      topic_config,
      %{timeout: 1_000}
    )
  end

  def delete(name) do
    :brod.delete_topics(
      [{"localhost", 9092}],
      [name],
      10_000
    )
  end
end
