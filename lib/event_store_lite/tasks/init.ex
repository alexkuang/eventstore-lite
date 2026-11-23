defmodule EventStoreLite.Tasks.Init do
  @moduledoc "Initializes the SQLite event store schema."
  import EventStoreLite.Tasks.Output

  alias EventStoreLite.SQLite
  alias EventStoreLite.Storage.Initializer

  def exec(config, opts \\ []) do
    {:ok, conn} = SQLite.start_link(config)

    try do
      Initializer.run!(conn, config)
      write_info("The EventStoreLite database has been initialized.", opts)
      :ok
    after
      GenServer.stop(conn)
    end
  end
end
