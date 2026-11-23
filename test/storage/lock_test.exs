defmodule EventStoreLite.Storage.LockTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.ProcessHelper
  alias EventStoreLite.Storage.Lock
  alias EventStoreLite.Wait

  test "acquire exclusive subscription lock", %{sqlite_config: sqlite_config, schema: schema} do
    conn = start_supervised!({EventStoreLite.SQLite, sqlite_config})

    assert :ok = Lock.try_acquire_exclusive_lock(conn, random_key(), schema: schema)
  end

  test "acquire and release lock by connection", %{
    sqlite_config: sqlite_config,
    schema: schema
  } do
    {:ok, conn1} = EventStoreLite.SQLite.start_link(sqlite_config)
    {:ok, conn2} = EventStoreLite.SQLite.start_link(sqlite_config)

    key = random_key()

    # conn1 acquire lock
    assert :ok = Lock.try_acquire_exclusive_lock(conn1, key, schema: schema)

    # conn2 cannot acquire lock
    assert {:error, :lock_already_taken} =
             Lock.try_acquire_exclusive_lock(conn2, key, schema: schema)

    # conn1 can acquire same lock multiple times
    assert :ok = Lock.try_acquire_exclusive_lock(conn1, key, schema: schema)

    # shutdown conn1 process should release its locks
    ProcessHelper.shutdown(conn1)

    # conn2 can now acquire lock
    Wait.until(fn ->
      assert :ok = Lock.try_acquire_exclusive_lock(conn2, key, schema: schema)
    end)

    ProcessHelper.shutdown(conn2)
  end

  def random_key, do: :rand.uniform(2_147_483_647)
end
