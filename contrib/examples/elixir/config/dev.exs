import Config

config :brod,
  clients: [
    # You can choose the name of the client
    kafka_client: [
      endpoints: [{"localhost", 9092}],
      # This will auto-start the producers with default configs
      auto_start_producers: true
    ]
  ]
