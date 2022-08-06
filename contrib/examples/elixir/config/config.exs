import Config

import_config "#{Mix.env()}.exs"

config :logger, :console,
 format: "[$level] $metadata $message\n",
 metadata: [:pid, :mfa]
