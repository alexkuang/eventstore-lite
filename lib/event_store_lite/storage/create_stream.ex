defmodule EventStoreLite.Storage.CreateStream do
  @moduledoc false
  alias EventStoreLite.SQLite

  def execute(conn, uuid, opts) do
    case SQLite.query(conn, "INSERT INTO streams(stream_uuid) VALUES(?1) RETURNING stream_id", [uuid], opts) do
      {:ok, %{rows: [[id]]}} ->
        {:ok, id}

      {:error, reason} ->
        if String.contains?(inspect(reason), "UNIQUE"), do: {:error, :stream_exists}, else: {:error, reason}
    end
  end
end
