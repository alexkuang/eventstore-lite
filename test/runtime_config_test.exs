defmodule EventStoreLite.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.Config
  alias EventStoreLite.Wait

  describe "event store runtime config" do
    setup do
      start_supervised!(TestEventStoreLite)
      start_supervised!({TestEventStoreLite, name: :test_event_store, schema: "example"})
      :ok
    end

    test "should get event store config by module" do
      expected_config =
        :eventstore_lite
        |> Application.get_env(TestEventStoreLite)
        |> Keyword.put_new(:conn, TestEventStoreLite.SQLite)
        |> Keyword.put_new(:schema, "public")
        |> Keyword.put_new(:subscription_hibernate_after, 15_000)
        |> Keyword.put_new(:subscription_retry_interval, 1000)
        |> with_defaults()

      for {key, value} <- Config.lookup(TestEventStoreLite) do
        assert Keyword.get(expected_config, key) == value
      end
    end

    test "should get event store config by name" do
      expected_config =
        :eventstore_lite
        |> Application.get_env(TestEventStoreLite)
        |> Keyword.put(:conn, Module.concat([:test_event_store, SQLite]))
        |> Keyword.put(:schema, "example")
        |> Keyword.put(:name, :test_event_store)
        |> with_defaults()

      for {key, value} <- Config.lookup(:test_event_store) do
        assert Keyword.get(expected_config, key) == value
      end
    end

    test "should remove config when application stopped" do
      :ok = stop_supervised(TestEventStoreLite)

      Wait.until(fn ->
        assert_raise(
          RuntimeError,
          "could not lookup TestEventStoreLite because it was not started or it does not exist",
          fn -> Config.lookup(TestEventStoreLite) end
        )
      end)

      assert EventStoreLite.all_instances() == [{TestEventStoreLite, name: :test_event_store}]
    end

    test "should list all running instances" do
      all_instances = Enum.sort(EventStoreLite.all_instances())

      assert all_instances == [
               {TestEventStoreLite, name: TestEventStoreLite},
               {TestEventStoreLite, name: :test_event_store}
             ]
    end

    # EventStoreLite has no compile-time configuration
    defmodule RuntimeConfiguredEventStoreLite do
      @moduledoc false
      use EventStoreLite, otp_app: :eventstore_lite
    end

    test "should allow an event store to be entirely configured at runtime" do
      config = [
        database: "tmp/runtime_config.sqlite3",
        serializer: EventStoreLite.JsonSerializer
      ]

      assert {:ok, _pid} = start_supervised({RuntimeConfiguredEventStoreLite, config})
    end
  end

  defp with_defaults(config) do
    Keyword.merge(
      [
        column_data_type: "bytea",
        correlation_id_type: "uuid",
        causation_id_type: "uuid",
        enable_hard_deletes: false,
        otp_app: :eventstore_lite,
        pool: DBConnection.ConnectionPool,
        subscription_hibernate_after: 15_000,
        subscription_retry_interval: 1_000,
        timeout: 15_000
      ],
      config
    )
  end
end
