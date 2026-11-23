defmodule EventStoreLite.Storage.Lock do
  @moduledoc false
  def try_acquire_exclusive_lock(conn, key, opts), do: EventStoreLite.SQLite.lock(conn, key, opts[:schema])
  def unlock(conn, key, opts), do: EventStoreLite.SQLite.unlock(conn, key, opts[:schema])
end
