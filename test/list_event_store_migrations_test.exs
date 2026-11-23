defmodule EventStoreLite.ListEventStoreLiteMigrationsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias EventStoreLite.Storage.Database
  alias EventStoreLite.Tasks.Migrations

  @moduletag :migration

  setup_all do
    config = TestEventStoreLite.config()

    [config: config]
  end

  describe "event store with pending migrations" do
    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"
    test "should list completed and pending migrations", %{config: config} do
      restore_pre_migration_event_store(config)

      expected = """

      EventStoreLite: TestEventStoreLite

        migration     state           migrated_at
      -------------------------------------------------------------
        0.17.0    \tcompleted\t2020-04-28 22:15:48.922963Z
        1.1.0     \tpending
        1.2.0     \tpending
        1.3.0     \tpending
        1.3.2     \tpending

      """

      assert expected == list_migrations(config, eventstore: TestEventStoreLite)
    end

    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

    test "exits with status 1 when running via mix", %{config: config} do
      restore_pre_migration_event_store(config)

      assert {:shutdown, 1} ==
               catch_exit(
                 Mix.Task.run("event_store_lite.migrations", ["--eventstore", "TestEventStoreLite", "--quiet"])
               )
    end
  end

  describe "recently created event store" do
    test "lists migrations as completed", %{config: config} do
      create_new_database(config)

      output =
        capture_io(fn ->
          assert [] = Migrations.exec(config, eventstore: TestEventStoreLite)
        end)

      refute output =~ "\tpending"
    end

    test "exits normally when running via mix", %{config: config} do
      create_new_database(config)

      # Task should exit normally
      Mix.Task.run("event_store_lite.migrations", ["--eventstore", "TestEventStoreLite", "--quiet"])
    end
  end

  defp create_new_database(config) do
    :ok = Database.drop(config)
    :ok = Database.create(config)

    {:ok, conn} = EventStoreLite.SQLite.start_link(config)

    EventStoreLite.Storage.Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
  end

  defp restore_pre_migration_event_store(config) do
    Database.drop(config)
    :ok = Database.create(config)

    {_, 0} = Database.restore(config, "test/fixture/eventstore.dump")
  end

  defp list_migrations(config, opts) do
    capture_io(fn -> Migrations.exec(config, opts) end)
  end
end
