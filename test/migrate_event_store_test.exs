defmodule EventStoreLite.MigrateEventStoreLiteTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias EventStoreLite.Storage.Database
  alias EventStoreLite.Storage.Initializer

  @moduletag :migration

  setup_all do
    config = TestEventStoreLite.config()

    [config: config]
  end

  describe "migrate event store" do
    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"
    test "should prevent migrations from running concurrently", %{config: config} do
      restore_pre_migration_event_store(config)

      log =
        [
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end)
        ]
        |> Enum.map(&Task.await/1)
        |> Enum.sort()

      # One migration should succeed, two should be prevented due to already running
      assert Enum.at(log, 0) =~ "EventStoreLite database migration already in progress."
      assert Enum.at(log, 1) =~ "EventStoreLite database migration already in progress."
      assert Enum.at(log, 2) =~ "The EventStoreLite database has been migrated."

      assert length(log) == 3
    end
  end

  describe "migrated event store schema" do
    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"
    test "should be identical to new event store schema", %{config: config} do
      restore_pre_migration_event_store(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      # Dump migrated database
      {migrated_schema, 0} = Database.dump(config, [])

      create_new_database(config)

      # Dump newly created database
      {new_schema, 0} = Database.dump(config, [])

      migrated_normalized = split_and_normalize(migrated_schema)
      new_normalized = split_and_normalize(new_schema)

      [migrated_normalized, new_normalized]
      |> Enum.zip()
      |> Enum.each(fn {migrated, new} ->
        assert migrated == new
      end)

      assert length(migrated_normalized) == length(new_normalized)
    end
  end

  test "initialization and migration are idempotent and preserve stored events", %{config: config} do
    conn = start_supervised!({EventStoreLite.SQLite, config})
    Initializer.run!(conn, config)
    Initializer.reset!(conn, config)
    start_supervised!(TestEventStoreLite)
    events = EventStoreLite.EventFactory.create_events(3)
    :ok = TestEventStoreLite.append_to_stream("migration-preserves-events", 0, events)
    {:ok, before_events} = TestEventStoreLite.read_stream_forward("migration-preserves-events")
    stop_supervised!(TestEventStoreLite)
    migrate_eventstore(config, quiet: true, is_mix: false)
    Initializer.run!(conn, config)
    start_supervised!(TestEventStoreLite)
    assert {:ok, ^before_events} = TestEventStoreLite.read_stream_forward("migration-preserves-events")
  end

  defp split_and_normalize(enumerable) do
    enumerable
    |> Enum.join()
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.map(
      &Regex.replace(
        ~r/[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]{0,6}\+[0-9]{2}/,
        &1,
        "NOW"
      )
    )
    # Ignore comments and pg_dump version-specific directives with random tokens
    |> Enum.reject(fn
      "0\t17\t0\tNOW" -> true
      "1\t1\t0\tNOW" -> true
      "1\t2\t0\tNOW" -> true
      "1\t3\t0\tNOW" -> true
      line -> String.starts_with?(line, ["--", "\\restrict ", "\\unrestrict "])
    end)
  end

  defp create_new_database(config) do
    :ok = Database.drop(config)
    :ok = Database.create(config)

    {:ok, conn} = EventStoreLite.SQLite.start_link(config)

    Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
  end

  defp restore_pre_migration_event_store(config) do
    Database.drop(config)
    :ok = Database.create(config)

    {_, 0} = Database.restore(config, "test/fixture/eventstore.dump")
  end

  defp migrate_eventstore(config, opts) do
    EventStoreLite.Tasks.Migrate.exec(config, opts)
  end
end
