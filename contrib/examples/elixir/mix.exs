defmodule BrodSample.MixProject do
  use Mix.Project

  def project do
    [
      app: :brod_sample,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {BrodSample.Application, []}
    ]
  end

  defp deps do
    [
      {:brod, "~> 3.10.0"},
      {:jason, "~> 1.2.1 "}
    ]
  end
end
