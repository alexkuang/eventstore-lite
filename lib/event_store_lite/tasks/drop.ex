defmodule EventStoreLite.Tasks.Drop do
  @moduledoc """
  Task to drop the EventStoreLite database.
  """

  import EventStoreLite.Tasks.Output

  alias EventStoreLite.Storage.Database
  alias EventStoreLite.Storage.Schema

  @doc """
  Runs task

  ## Parameters

    - config: the parsed EventStoreLite config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)

    case schema do
      "public" ->
        drop_database(config, opts)

      _schema ->
        drop_schema(config, opts)
    end
  end

  defp drop_database(config, opts) do
    case Database.drop(config) do
      :ok ->
        write_info("The EventStoreLite database has been dropped.", opts)

      {:error, :already_down} ->
        write_info("The EventStoreLite database has already been dropped.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStoreLite database couldn't be dropped, reason given: #{inspect(term)}.",
          opts
        )
    end
  end

  defp drop_schema(config, opts) do
    case Schema.drop(config) do
      :ok ->
        write_info("The EventStoreLite schema has been dropped.", opts)

      {:error, :already_down} ->
        write_info("The EventStoreLite schema has already been dropped.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStoreLite schema couldn't be dropped, reason given: #{inspect(term)}.",
          opts
        )
    end
  end
end
