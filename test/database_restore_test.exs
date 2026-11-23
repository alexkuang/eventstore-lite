defmodule EventStoreLite.DatabaseRestoreTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.EventData
  alias EventStoreLite.Storage.Database
  alias EventStoreLite.TermSerializer

  @moduletag :tmp_dir
  @stream "restore-history"

  setup %{tmp_dir: dir} do
    writer = Path.join(dir, "writer.sqlite3")
    database = Path.join(dir, "target.sqlite3")
    backup = Path.join(dir, "backup.sqlite3")
    config = [database: database, serializer: TermSerializer]
    writer_config = Keyword.replace!(config, :database, writer)

    start_supervised!({TestEventStoreLite, writer_config})
    append_event!(0)
    assert {:ok, backup_events} = TestEventStoreLite.read_stream_forward(@stream)
    assert {"", 0} = Database.dump(writer_config, backup)
    stop_supervised!(TestEventStoreLite)

    start_supervised!({TestEventStoreLite, writer_config})
    Enum.each(1..17, &append_event!/1)
    assert {:ok, acknowledged} = TestEventStoreLite.read_stream_forward(@stream)

    # Capture the quiescent writer's disk state before close can checkpoint its WAL.
    for suffix <- ["", "-wal", "-shm"], do: File.cp!(writer <> suffix, database <> suffix)
    stop_supervised!(TestEventStoreLite)

    [config: config, database: database, backup: backup, backup_events: backup_events, acknowledged: acknowledged]
  end

  test "a missing backup preserves acknowledged history not yet checkpointed", context do
    %{config: config, database: database, tmp_dir: dir, backup_events: backup_events} = context

    main_only = Path.join(dir, "main-only.sqlite3")
    File.cp!(database, main_only)
    start_supervised!({TestEventStoreLite, Keyword.replace!(config, :database, main_only)})
    assert {:ok, ^backup_events} = TestEventStoreLite.read_stream_forward(@stream)
    stop_supervised!(TestEventStoreLite)

    assert_failed_restore_preserves_history(context, Path.join(dir, "missing.sqlite3"))
  end

  test "a truncated SQLite backup preserves acknowledged history", %{backup: backup, tmp_dir: dir} = context do
    truncated = Path.join(dir, "truncated.sqlite3")
    File.write!(truncated, binary_part(File.read!(backup), 0, 4_096))
    assert_failed_restore_preserves_history(context, truncated)
  end

  test "the destination cannot be its own backup source", %{database: database} = context do
    assert_failed_restore_preserves_history(context, database)
  end

  test "a failed replacement returns the original WAL", context do
    %{config: config, database: database, backup: backup, acknowledged: acknowledged} = context
    original = database <> ".original"
    wal = File.read!(database <> "-wal")
    File.rename!(database, original)
    File.mkdir!(database)

    assert {_reason, 1} = Database.restore(config, backup)
    assert File.read!(database <> "-wal") == wal

    File.rmdir!(database)
    File.rename!(original, database)
    start_supervised!({TestEventStoreLite, config})
    assert {:ok, ^acknowledged} = TestEventStoreLite.read_stream_forward(@stream)
  end

  test "a valid backup replaces the database without replaying the old WAL", context do
    %{config: config, database: database, backup: backup, backup_events: backup_events} = context

    assert {"", 0} = Database.restore(config, backup)
    refute File.exists?(database <> "-wal")
    refute File.exists?(database <> "-shm")

    start_supervised!({TestEventStoreLite, config})
    assert {:ok, ^backup_events} = TestEventStoreLite.read_stream_forward(@stream)
    append_event!(1)
  end

  defp assert_failed_restore_preserves_history(context, source) do
    %{config: config, database: database, acknowledged: acknowledged} = context
    files = for suffix <- ["", "-wal", "-shm"], do: {database <> suffix, File.read!(database <> suffix)}

    assert {_reason, 1} = Database.restore(config, source)
    for {path, bytes} <- files, do: assert(File.read(path) == {:ok, bytes})

    start_supervised!({TestEventStoreLite, config})
    assert {:ok, ^acknowledged} = TestEventStoreLite.read_stream_forward(@stream)
  end

  defp append_event!(version) do
    event = %EventData{event_type: "RestorableEvent", data: version}
    assert :ok = TestEventStoreLite.append_to_stream(@stream, version, [event])
  end
end
