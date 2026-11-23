defmodule EventStoreLite.Tasks.Migrate do
  @moduledoc "Initializes the store schema."
  defdelegate exec(config, opts \\ []), to: EventStoreLite.Tasks.Init
end
