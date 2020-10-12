defmodule BrodSample.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    BrodSample.GroupSubscriber.start()

    # {:ok, pid} = BrodSample.GroupSubscriberV2.start()

    children = [
      # pid
      # Starts a worker by calling: BrodSample.Worker.start_link(arg)
      # {BrodSample.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: BrodSample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
