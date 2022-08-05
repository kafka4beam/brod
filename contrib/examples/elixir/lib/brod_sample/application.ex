defmodule BrodSample.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      BrodSample.GroupSubscriberV2
    ]
    opts = [strategy: :one_for_one, name: BrodSample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
