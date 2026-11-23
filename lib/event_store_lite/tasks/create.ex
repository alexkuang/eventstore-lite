defmodule EventStoreLite.Tasks.Create do
  @moduledoc """
  Task to create the EventStoreLite
  """

  import EventStoreLite.Tasks.Output

  alias EventStoreLite.Storage.Database
  alias EventStoreLite.Storage.Schema

  @doc """
  Runs database and schema create task.

  ## Parameters

    - config: the parsed EventStoreLite config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    case Database.create(config) do
      :ok ->
        write_info("The EventStoreLite database has been created.", opts)

      {:error, :already_up} ->
        write_info("The EventStoreLite database already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStoreLite database couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end

    case Schema.create(config) do
      :ok ->
        write_info("The EventStoreLite schema has been created.", opts)

      {:error, :already_up} ->
        write_info("The EventStoreLite schema already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStoreLite schema couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end
  end
end
