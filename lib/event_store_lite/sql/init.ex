defmodule EventStoreLite.Sql.Init do
  @moduledoc false
  def statements(_) do
    [
      "CREATE TABLE IF NOT EXISTS streams (stream_id INTEGER PRIMARY KEY AUTOINCREMENT, stream_uuid TEXT NOT NULL UNIQUE, stream_version INTEGER NOT NULL DEFAULT 0 CHECK(stream_version>=0), created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), deleted_at TEXT)",
      "INSERT OR IGNORE INTO streams(stream_id,stream_uuid) VALUES(0,'$all')",
      "CREATE TABLE IF NOT EXISTS events (event_id TEXT PRIMARY KEY NOT NULL, event_type TEXT NOT NULL, causation_id TEXT, correlation_id TEXT, data BLOB NOT NULL, metadata BLOB, created_at TEXT NOT NULL)",
      "CREATE TABLE IF NOT EXISTS stream_events (event_id TEXT NOT NULL REFERENCES events(event_id), stream_id INTEGER NOT NULL REFERENCES streams(stream_id), stream_version INTEGER NOT NULL CHECK(stream_version>0), original_stream_id INTEGER NOT NULL REFERENCES streams(stream_id), original_stream_version INTEGER NOT NULL, PRIMARY KEY(stream_id,event_id), UNIQUE(stream_id,stream_version))",
      "CREATE TABLE IF NOT EXISTS stream_notifications (notification_id INTEGER PRIMARY KEY AUTOINCREMENT, stream_id INTEGER NOT NULL, stream_uuid TEXT NOT NULL, from_stream_version INTEGER NOT NULL, to_stream_version INTEGER NOT NULL)",
      "CREATE INDEX IF NOT EXISTS ix_stream_events_event_id ON stream_events(event_id)",
      "CREATE TABLE IF NOT EXISTS subscriptions (subscription_id INTEGER PRIMARY KEY AUTOINCREMENT, stream_uuid TEXT NOT NULL, subscription_name TEXT NOT NULL, last_seen INTEGER, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), UNIQUE(stream_uuid,subscription_name))",
      "CREATE TABLE IF NOT EXISTS snapshots (source_uuid TEXT PRIMARY KEY NOT NULL, source_version INTEGER NOT NULL, source_type TEXT NOT NULL, data BLOB NOT NULL, metadata BLOB, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))",
      "CREATE TABLE IF NOT EXISTS schema_migrations (major_version INTEGER, minor_version INTEGER, patch_version INTEGER, migrated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))",
      "INSERT INTO schema_migrations(major_version,minor_version,patch_version) SELECT 1,4,10 WHERE NOT EXISTS(SELECT 1 FROM schema_migrations)",
      "CREATE TABLE IF NOT EXISTS store_settings (hard_delete INTEGER NOT NULL DEFAULT 0)",
      "INSERT INTO store_settings(hard_delete) SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM store_settings)"
    ] ++ protections()
  end

  defp protections do
    for {table, operation} <- [
          {"events", "UPDATE"},
          {"stream_events", "UPDATE"},
          {"events", "DELETE"},
          {"stream_events", "DELETE"},
          {"streams", "DELETE"}
        ] do
      permission = if operation == "DELETE", do: " WHEN (SELECT hard_delete FROM store_settings)=0", else: ""

      "CREATE TRIGGER IF NOT EXISTS protect_#{table}_#{String.downcase(operation)} BEFORE #{operation} ON #{table}#{permission} BEGIN SELECT RAISE(ABORT, 'Cannot #{String.downcase(operation)} #{table}'); END"
    end
  end
end
