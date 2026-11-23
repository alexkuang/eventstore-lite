defmodule EventStoreLite.Storage.DeleteStream do
  @moduledoc false
  alias EventStoreLite.SQLite

  def soft_delete(conn, id, opts) do
    case SQLite.query(
           conn,
           "UPDATE streams SET deleted_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE stream_id=?1",
           [id],
           opts
         ) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _} -> {:error, :stream_not_found}
      error -> error
    end
  end

  def hard_delete(conn, id, opts) do
    case SQLite.transaction(
           conn,
           fn tx ->
             case SQLite.query(tx, "SELECT stream_id FROM streams WHERE stream_id=?1", [id], opts) do
               {:ok, %{rows: [[^id]]}} ->
                 SQLite.query!(tx, "UPDATE store_settings SET hard_delete=1", [], opts)
                 SQLite.query!(tx, "CREATE TEMP TABLE IF NOT EXISTS deleted_event_ids (event_id TEXT PRIMARY KEY)")
                 SQLite.query!(tx, "DELETE FROM deleted_event_ids")

                 SQLite.query!(
                   tx,
                   "INSERT INTO deleted_event_ids SELECT event_id FROM stream_events WHERE stream_id=?1",
                   [id],
                   opts
                 )

                 SQLite.query!(
                   tx,
                   "DELETE FROM stream_events WHERE event_id IN (SELECT event_id FROM deleted_event_ids)",
                   [],
                   opts
                 )

                 SQLite.query!(
                   tx,
                   "DELETE FROM events WHERE event_id IN (SELECT event_id FROM deleted_event_ids)",
                   [],
                   opts
                 )

                 SQLite.query!(tx, "DELETE FROM streams WHERE stream_id=?1", [id], opts)
                 SQLite.query!(tx, "UPDATE store_settings SET hard_delete=0", [], opts)
                 :ok

               {:ok, _} ->
                 {:error, :stream_not_found}

               error ->
                 error
             end
           end,
           opts
         ) do
      {:ok, value} -> value
      error -> error
    end
  end
end
