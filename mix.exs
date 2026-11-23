defmodule EventStoreLite.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/alexkuang/eventstore-lite"

  def project do
    [
      app: :eventstore_lite,
      version: @version,
      name: "EventStoreLite",
      source_url: @source_url,
      homepage_url: @source_url,
      docs: [
        main: "readme",
        source_ref: "v#{@version}",
        extras: ["README.md", "PARITY.md", "THIRD_PARTY.md", "MANUAL_TESTING.md", "LICENSE"]
      ],
      elixir: "~> 1.16",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      consolidate_protocols: Mix.env() not in [:test, :jsonb, :text_ids, :migration],
      test_ignore_filters: [&String.starts_with?(&1, "test/support/"), &String.starts_with?(&1, "test/manual/")],
      description: "SQLite-backed event store for Commanded",
      package: [
        licenses: ["Apache-2.0"],
        links: %{"GitHub" => @source_url},
        files: ["lib", "mix.exs", "README.md", "PARITY.md", "THIRD_PARTY.md", "MANUAL_TESTING.md", "LICENSE*"]
      ]
    ]
  end

  def application, do: [extra_applications: [:crypto, :eex, :logger, :ssl], mod: {EventStoreLite.Application, []}]

  defp elixirc_paths(env) when env in [:test, :jsonb, :text_ids, :migration],
    do: ["lib", "test/support", "test/subscriptions/support"]

  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:fsm, "~> 0.3"},
      {:gen_stage, "~> 1.2"},
      {:exqlite, "~> 0.37"},
      {:jason, "~> 1.4", optional: true},
      {:db_connection, "~> 2.8"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.12", only: [:dev, :test], runtime: false},
      {:commanded, "~> 1.4", optional: true}
    ]
  end
end
