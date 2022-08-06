import Config

import_config "#{Mix.env()}.exs"

config :logger, :console,
 format: "[$level] $message $metadata\n",
 metadata: [:error_code, :file, :pid]
