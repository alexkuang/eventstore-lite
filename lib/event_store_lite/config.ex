defmodule EventStoreLite.Config do
  @moduledoc """
  Provides access to the EventStoreLite configuration.
  """

  alias EventStoreLite.Config.Store

  @doc """
  Get the event store configuration for the environment.
  """
  def get(event_store, otp_app) do
    Application.get_env(otp_app, event_store, [])
  end

  @doc false
  def get_pool, do: DBConnection.ConnectionPool

  @doc """
  Get the event store configuration for the environment.
  """
  def parsed(event_store, otp_app) do
    event_store |> get(otp_app) |> parse()
  end

  @doc """
  Normalizes the event store configuration.
  """
  defdelegate parse(config), to: EventStoreLite.Config.Parser

  @doc false
  defdelegate all, to: Store

  @doc false
  defdelegate associate(name, pid, event_store, config), to: Store

  @doc false
  defdelegate lookup(name), to: Store, as: :get

  @doc false
  defdelegate lookup(name, setting), to: Store, as: :get

  @doc """
  Selects the payload mode (`"bytea"` or `"jsonb"`).
  Use `"jsonb"` with `JsonbSerializer`.
  """
  def column_data_type(event_store, config) do
    case Keyword.get(config, :column_data_type, "bytea") do
      valid when valid in ["bytea", "jsonb"] ->
        valid

      invalid ->
        raise ArgumentError,
              inspect(event_store) <>
                ~s( `:column_data_type` expects either "bytea" or "jsonb" but got: ) <>
                inspect(invalid)
    end
  end

  @doc """
  Get the column type used to store the correlation identifier.

  Supported types are:

    - "uuid" - Decodes 16-byte identifiers as UUID strings.
    - "text" - Allows any string value as an identifier.
  """
  def correlation_id_type(event_store, config) do
    case Keyword.get(config, :correlation_id_type, "uuid") do
      valid when valid in ["uuid", "text"] ->
        valid

      invalid ->
        raise ArgumentError,
              inspect(event_store) <>
                ~s( `:correlation_id_type` expects either "uuid" or "text" but got: ) <>
                inspect(invalid)
    end
  end

  @doc """
  Get the column type used to store the causation identifier.

  Supported types are:

    - "uuid" - Decodes 16-byte identifiers as UUID strings.
    - "text" - Allows any string value as an identifier.
  """
  def causation_id_type(event_store, config) do
    case Keyword.get(config, :causation_id_type, "uuid") do
      valid when valid in ["uuid", "text"] ->
        valid

      invalid ->
        raise ArgumentError,
              inspect(event_store) <>
                ~s( `:causation_id_type` expects either "uuid" or "text" but got: ) <>
                inspect(invalid)
    end
  end

  @doc "Returns configuration for a SQLite connection."
  def default_sqlite_opts(config), do: config

  @doc "Returns configuration for a named SQLite connection."
  def sqlite_opts(config, name), do: Keyword.put(config, :name, name)
end
