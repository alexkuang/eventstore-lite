defmodule EventStoreLite.ConfigTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.Config

  test "parse keys" do
    config = [
      username: "postgres",
      hostname: "localhost",
      database: "eventstore_test",
      password: "postgres",
      schema: "example",
      timeout: 120_000,
      pool: Config.get_pool()
    ]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      pool: Config.get_pool(),
      timeout: 120_000,
      schema: "example",
      password: "postgres",
      database: "eventstore_test",
      hostname: "localhost",
      username: "postgres"
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse socket" do
    config = [
      username: "postgres",
      socket: "/path/to/socket",
      database: "eventstore_test",
      password: "postgres",
      timeout: 120_000,
      pool: Config.get_pool()
    ]

    assert_parsed_config(config,
      pool: Config.get_pool(),
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      timeout: 120_000,
      password: "postgres",
      database: "eventstore_test",
      socket: "/path/to/socket",
      username: "postgres"
    )
  end

  test "parse `:shared_connection_pool`" do
    config = [
      username: "postgres",
      database: "eventstore_test",
      password: "postgres",
      shared_connection_pool: :shared_pool
    ]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      timeout: 15_000,
      pool: Config.get_pool(),
      shared_connection_pool: :shared_pool,
      password: "postgres",
      database: "eventstore_test",
      username: "postgres"
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse socket_dir" do
    config = [
      username: "postgres",
      socket_dir: "/path/to/socket_dir",
      database: "eventstore_test",
      password: "postgres",
      timeout: 120_000,
      pool: Config.get_pool()
    ]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      pool: Config.get_pool(),
      timeout: 120_000,
      password: "postgres",
      database: "eventstore_test",
      socket_dir: "/path/to/socket_dir",
      username: "postgres"
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse url" do
    config = [url: "postgres://username:password@localhost/database"]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      timeout: 15_000,
      pool: Config.get_pool(),
      username: "username",
      password: "password",
      database: "database",
      hostname: "localhost"
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse url with encoded hash in password" do
    config = [url: "postgres://username:password%23with_hash@localhost/database"]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      timeout: 15_000,
      pool: Config.get_pool(),
      username: "username",
      password: "password#with_hash",
      database: "database",
      hostname: "localhost"
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse session_mode_url" do
    config = [session_mode_url: "postgres://username:password@localhost/database"]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      timeout: 15_000,
      pool: DBConnection.ConnectionPool,
      session_mode_pool: [
        username: "username",
        password: "password",
        database: "database",
        hostname: "localhost"
      ]
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "passes parent config options to session mode pool config" do
    config = [
      ssl: true,
      ssl_opts: [cacertfile: ~c"/etc/ssl/certs/db.ca-certificate.crt", verify: :verify_peer],
      prepare: :unnamed,
      session_mode_url: "postgres://username:password@localhost/database"
    ]

    session_mode_pool_config =
      config |> Config.parse() |> Config.postgrex_notifications_opts(:name) |> Enum.sort()

    assert session_mode_pool_config == [
             auto_reconnect: true,
             backoff_type: :exp,
             database: "database",
             hostname: "localhost",
             name: :name,
             password: "password",
             pool: DBConnection.ConnectionPool,
             pool_size: 1,
             prepare: :unnamed,
             ssl: true,
             ssl_opts: [
               cacertfile: ~c"/etc/ssl/certs/db.ca-certificate.crt",
               verify: :verify_peer
             ],
             sync_connect: false,
             timeout: 15_000,
             username: "username"
           ]
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse url with query parameters" do
    config = [
      url: "postgres://username:password@localhost/database?ssl=true&pool_size=5&timeout=120000"
    ]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      pool: Config.get_pool(),
      username: "username",
      password: "password",
      database: "database",
      hostname: "localhost",
      timeout: 120_000,
      pool_size: 5,
      ssl: true
    )
  end

  @tag :postgres_only
  @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

  test "parse database url from environment variable" do
    set_envs(%{"ES_URL" => "postgres://username:password@localhost/database"})

    config = [url: {:system, "ES_URL"}]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      timeout: 15_000,
      pool: Config.get_pool(),
      username: "username",
      password: "password",
      database: "database",
      hostname: "localhost"
    )
  end

  test "fetch database settings from environment variables" do
    set_envs(%{
      "ES_USERNAME" => "username",
      "ES_PASSWORD" => "password",
      "ES_DATABASE" => "database",
      "ES_HOSTNAME" => "hostname",
      "ES_PORT" => "5432",
      "ES_TIMEOUT" => "120000"
    })

    config = [
      username: {:system, "ES_USERNAME"},
      password: {:system, "ES_PASSWORD"},
      database: {:system, "ES_DATABASE"},
      hostname: {:system, "ES_HOSTNAME"},
      port: {:system, "ES_PORT"},
      timeout: {:system, "ES_TIMEOUT"}
    ]

    assert_parsed_config(config,
      enable_hard_deletes: false,
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      schema: "public",
      pool: Config.get_pool(),
      timeout: 120_000,
      port: 5432,
      hostname: "hostname",
      database: "database",
      password: "password",
      username: "username"
    )
  end

  test "support default value when env variable not set" do
    config = [username: {:system, "ES_USERNAME", "default_username"}]

    assert_parsed_config(config,
      pool: Config.get_pool(),
      username: "default_username",
      column_data_type: "bytea",
      correlation_id_type: "uuid",
      causation_id_type: "uuid",
      enable_hard_deletes: false,
      schema: "public",
      timeout: 15_000
    )
  end

  test "parse SQLite database URL" do
    config = Config.parse(url: "sqlite:///tmp/event%20store.sqlite3")
    assert config[:database] == "/tmp/event store.sqlite3"
    assert config[:timeout] == 15_000
  end

  test "parse SQLite database path from environment" do
    set_envs(%{"ES_SQLITE_DATABASE" => "/tmp/eventstore.sqlite3"})
    config = Config.parse(database: {:system, "ES_SQLITE_DATABASE"})
    assert config[:database] == "/tmp/eventstore.sqlite3"
  end

  describe "setting application parameters" do
    setup do
      sqlite_config = TestEventStoreLite.config()
      conn = start_supervised!({EventStoreLite.SQLite, sqlite_config})

      [conn: conn]
    end

    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

    test "successfully connects with the application name setup", %{conn: conn} do
      start_supervised!({TestEventStoreLite, name: :app_name_test, parameters: [application_name: "event_store_test"]})

      result =
        EventStoreLite.SQLite.query!(
          conn,
          """
          SELECT application_name
          FROM pg_stat_activity
          WHERE application_name = 'event_store_test'
          """,
          []
        )

      assert result.num_rows > 1
    end

    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"

    test "default connections do not have application name setup", %{conn: conn} do
      result =
        EventStoreLite.SQLite.query!(
          conn,
          """
          SELECT application_name
          FROM pg_stat_activity
          WHERE application_name = 'event_store_test'
          """,
          []
        )

      assert result.num_rows == 0
    end
  end

  defp assert_parsed_config(config, expected) do
    parsed = Config.parse(config)

    assert Keyword.equal?(parsed, expected)
  end

  defp set_envs(envs) do
    for {key, value} <- envs do
      System.put_env(key, value)
    end

    on_exit(fn ->
      for {key, _value} <- envs do
        System.delete_env(key)
      end
    end)
  end
end
