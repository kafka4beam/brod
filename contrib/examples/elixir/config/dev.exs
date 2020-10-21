import Config

config :brod,
  clients: [
    # You can choose the name of the client
    kafka_client: [
      endpoints: ["kafka-default.dev.podium-dev.com": 19092],
      # This will auto-start the producers with default configs
      auto_start_producers: true
    ]
  ]
