defmodule EventStoreLite.Storage.Database do
  @moduledoc "SQLite database lifecycle and standalone backup operations."
  alias EventStoreLite.SQLite
  alias Exqlite.Sqlite3

  def create(config) do
    path = Keyword.fetch!(config, :database)

    if path != ":memory:" and File.exists?(path) do
      {:error, :already_up}
    else
      with {:ok, pid} <- SQLite.start_link(Keyword.delete(config, :name)) do
        GenServer.stop(pid)
        :ok
      end
    end
  end

  def drop(config) do
    path = Keyword.fetch!(config, :database)

    case File.rm(path) do
      :ok ->
        File.rm(path <> "-wal")
        File.rm(path <> "-shm")
        :ok

      {:error, :enoent} ->
        {:error, :already_down}

      error ->
        error
    end
  end

  def execute(config, script) do
    with {:ok, pid} <- SQLite.start_link(Keyword.delete(config, :name)) do
      try do
        SQLite.execute_script(pid, script, config)
      after
        GenServer.stop(pid)
      end
    end
  end

  @doc "Creates a consistent SQLite backup at a file path or into a collectable."
  def dump(config, target) do
    with {:ok, pid} <- SQLite.start_link(Keyword.delete(config, :name)) do
      try do
        temporary = Path.join(System.tmp_dir!(), "eventstore-lite-#{System.unique_integer([:positive])}.sqlite3")

        try do
          SQLite.query!(pid, "VACUUM INTO ?1", [temporary])
          data = File.read!(temporary)
          if is_binary(target), do: File.write!(target, data), else: _ = Enum.into([data], target)
          {"", 0}
        after
          File.rm(temporary)
        end
      after
        GenServer.stop(pid)
      end
    end
  end

  @doc "Restores a separate, standalone SQLite backup. All connections to the destination must be closed."
  def restore(config, source) do
    database = Keyword.fetch!(config, :database)
    parent = Path.dirname(database)
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
    staging = Path.join(parent, ".eventstore-lite-restore-#{suffix}")

    with true <- Path.expand(source) != Path.expand(database),
         :ok <- File.mkdir_p(parent),
         :ok <- File.mkdir(staging),
         :ok <- stage_restore(source, database, staging) do
      {"", 0}
    else
      false -> {"backup must be separate from the destination", 1}
      {:error, reason} -> {inspect(reason), 1}
    end
  end

  defp stage_restore(source, database, staging) do
    backup = Path.join(staging, "backup.sqlite3")
    parked_wal = Path.join(staging, "previous-wal")

    try do
      with :ok <- File.cp(source, backup),
           :ok <- validate_backup(backup) do
        replace_database(database, backup, parked_wal)
      end
    after
      for suffix <- ["", "-wal", "-shm"], do: File.rm(backup <> suffix)
      # An interrupted replacement or a failed rollback can leave the only copy of the old WAL here.
      File.rmdir(staging)
    end
  end

  defp validate_backup(backup) do
    with {:ok, "SQLite format 3\0"} <- File.open(backup, [:read, :binary], &IO.binread(&1, 16)),
         {:ok, db} <- Sqlite3.open(backup, mode: :readonly) do
      try do
        check_backup(db)
      after
        Sqlite3.close(db)
      end
    else
      {:ok, _} -> {:error, :invalid_sqlite_backup}
      {:error, _} = error -> error
    end
  end

  defp check_backup(db) do
    with {:ok, statement} <- Sqlite3.prepare(db, "PRAGMA quick_check(1)") do
      try do
        case Sqlite3.fetch_all(db, statement) do
          {:ok, [["ok"]]} -> :ok
          {:ok, rows} -> {:error, {:invalid_sqlite_backup, rows}}
          {:error, _} = error -> error
        end
      after
        Sqlite3.release(db, statement)
      end
    end
  end

  defp replace_database(database, backup, parked_wal) do
    wal = database <> "-wal"

    # SQLite can rebuild the SHM file, but the WAL may contain the only copy of acknowledged events.
    with :ok <- ignore_missing(File.rm(database <> "-shm")),
         :ok <- ignore_missing(File.rename(wal, parked_wal)) do
      case File.rename(backup, database) do
        :ok ->
          File.rm(parked_wal)
          :ok

        {:error, reason} ->
          case ignore_missing(File.rename(parked_wal, wal)) do
            :ok -> {:error, reason}
            {:error, rollback_error} -> {:error, {:wal_recovery_required, parked_wal, reason, rollback_error}}
          end
      end
    end
  end

  defp ignore_missing({:error, :enoent}), do: :ok
  defp ignore_missing(result), do: result
end
