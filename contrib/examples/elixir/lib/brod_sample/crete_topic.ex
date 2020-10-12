defmodule BrodSample.CreateTopic do
  def create() do
    topic_config = [
      %{
        config_entries: [],
        num_partitions: 6,
        replica_assignment: [],
        replication_factor: 1,
        topic: "test_topic"
      }
    ]

    kafka_url = [
      {System.get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost"), 9092}
    ]

    :brod.create_topics(
      # ["localhost": 19092],
      kafka_url,
      topic_config,
      %{timeout: 1_000}
    )

    # :brod.create_topics(
    #   ["localhost": 19092],
    #   ["test_create_topic"],
    #   %{timeout: 1_000}
    # )
  end

  @topics ["some.topic", "reviews.attributions", "reviews.listings", "reviews.snapshots"]
  def delete() do
    :brod.delete_topics(
      [localhost: 9092],
      @topics,
      10_000
    )
  end
end
