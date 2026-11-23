defmodule EventStoreLite.Sql.Reset do
  @moduledoc false
  def statements(config) do
    schema = Keyword.get(config, :schema)

    sequence_names =
      Enum.map_join(~w(streams subscriptions stream_notifications), ",", fn table ->
        name = EventStoreLite.SQLite.namespace(table, schema)
        "'#{name}'"
      end)

    [
      "UPDATE store_settings SET hard_delete=1",
      "DELETE FROM stream_events",
      "DELETE FROM stream_notifications",
      "DELETE FROM events",
      "DELETE FROM streams",
      "DELETE FROM snapshots",
      "DELETE FROM subscriptions",
      "DELETE FROM sqlite_sequence WHERE name IN (#{sequence_names})",
      "INSERT INTO streams(stream_id,stream_uuid) VALUES(0,'$all')",
      "UPDATE store_settings SET hard_delete=0"
    ]
  end
end
