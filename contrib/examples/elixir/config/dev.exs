import Config

config :brod,
  clients: [
    # This client will only be used by the publisher.
    kafka_client: [
      endpoints: [{"localhost", 9092}],
      # This will auto-start the producers with default configs
      auto_start_producers: true
    ],

    # Creating a client for each group_subscriber because it can not be shared.
    cg_v1_client: [
      endpoints: [{"localhost", 9092}],
      # This will auto-start the producers with default configs
      auto_start_producers: true
    ],

    # Creating a client for each group_subscriber because it can not be shared.
    cg_v2_client: [
      endpoints: [{"localhost", 9092}],
      # This will auto-start the producers with default configs
      auto_start_producers: true
    ]
  ]
