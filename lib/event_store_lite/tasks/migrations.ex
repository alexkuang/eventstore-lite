defmodule EventStoreLite.Tasks.Migrations do
  @moduledoc "Lists SQLite schema migrations."
  import EventStoreLite.Tasks.Output

  @doc false
  def available_migrations, do: []

  def exec(_config, opts \\ []) do
    write_info("EventStoreLite initial SQLite schema is current.", opts)
    []
  end
end
