defmodule EventStoreLite.Storage.Schema do
  @moduledoc false
  alias EventStoreLite.SQLite
  alias EventStoreLite.Storage.Initializer

  def create(config) do
    with_connection(config, fn conn ->
      case SQLite.query(conn, "SELECT 1 FROM streams LIMIT 1", [], config) do
        {:ok, _} ->
          {:error, :already_up}

        {:error, error} when is_binary(error) ->
          if String.contains?(error, "no such table") do
            initialize(conn, config)
          else
            {:error, error}
          end

        {:error, _} = failure ->
          failure
      end
    end)
  end

  def drop(config) do
    with_connection(config, fn conn ->
      case SQLite.query(conn, "SELECT 1 FROM streams LIMIT 1", [], config) do
        {:error, error} when is_binary(error) ->
          if String.contains?(error, "no such table"), do: {:error, :already_down}, else: {:error, error}

        {:error, _} = failure ->
          failure

        {:ok, _} ->
          drop_tables(conn, config)
      end
    end)
  end

  defp initialize(conn, config) do
    case Initializer.run!(conn, config) do
      {:ok, :ok} -> :ok
      {:error, _} = failure -> failure
    end
  end

  defp drop_tables(conn, config) do
    case SQLite.transaction(conn, fn tx ->
           SQLite.query!(tx, "UPDATE store_settings SET hard_delete=1", [], config)

           for table <-
                 ~w(stream_notifications stream_events events streams subscriptions snapshots schema_migrations store_settings) do
             SQLite.query!(tx, "DROP TABLE #{table}", [], config)
           end

           :ok
         end) do
      {:ok, :ok} -> :ok
      {:error, _} = failure -> failure
    end
  end

  defp with_connection(config, fun) do
    with {:ok, conn} <- SQLite.start_link(Keyword.delete(config, :name)) do
      try do
        fun.(conn)
      after
        GenServer.stop(conn)
      end
    end
  end
end
