defmodule EventStoreLite.StorageInitializer do
  @moduledoc false
  alias EventStoreLite.Storage.Initializer

  def reset_storage! do
    config = TestEventStoreLite.config()

    with {:ok, conn} <- EventStoreLite.SQLite.start_link(config) do
      try do
        Initializer.run!(conn, config)
        Initializer.reset!(conn, config)
      after
        :ok = GenServer.stop(conn)
      end

      :ok
    end
  end
end
