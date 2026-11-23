defmodule EventStoreLite.StorageCase do
  @moduledoc false
  use ExUnit.CaseTemplate

  alias EventStoreLite.Config
  alias EventStoreLite.Serializer
  alias EventStoreLite.SQLite
  alias EventStoreLite.Storage

  setup_all do
    config = Config.parsed(TestEventStoreLite, :eventstore_lite)
    sqlite_config = Config.default_sqlite_opts(config)
    conn = start_supervised!({SQLite, sqlite_config})
    Storage.Initializer.run!(conn, config)

    [
      conn: conn,
      config: config,
      base_config: config,
      schema: "public",
      event_store: TestEventStoreLite,
      sqlite_config: sqlite_config,
      serializer: Serializer.serializer(TestEventStoreLite, config),
      correlation_id_type: Keyword.get(config, :correlation_id_type, "uuid"),
      causation_id_type: Keyword.get(config, :causation_id_type, "uuid")
    ]
  end

  setup %{conn: conn, base_config: config} do
    Storage.Initializer.reset!(conn, config)

    # Named/schema stores persist in the same files between runs. Reset them
    # before starting listeners so earlier tests cannot leak checkpoints/events.
    for store <- Application.fetch_env!(:eventstore_lite, :event_stores), store != TestEventStoreLite do
      other_config = store.config()
      {:ok, other_conn} = SQLite.start_link(Keyword.delete(other_config, :name))

      try do
        Storage.Initializer.run!(other_conn, other_config)
        Storage.Initializer.reset!(other_conn, other_config)
      after
        GenServer.stop(other_conn)
      end
    end

    start_supervised!(TestEventStoreLite)
    :ok
  end
end
