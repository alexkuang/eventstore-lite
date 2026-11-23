defmodule EventStoreLite.Supervisor do
  @moduledoc false

  use Supervisor

  alias EventStoreLite.AdvisoryLocks
  alias EventStoreLite.Config
  alias EventStoreLite.MonitoredServer
  alias EventStoreLite.Notifications
  alias EventStoreLite.PubSub
  alias EventStoreLite.Serializer
  alias EventStoreLite.Subscriptions

  @doc """
  Starts the event store supervisor.
  """
  def start_link(event_store, otp_app, name, opts) do
    Supervisor.start_link(
      __MODULE__,
      {event_store, otp_app, name, opts},
      name: name
    )
  end

  @doc """
  Retrieves the runtime configuration.
  """
  def runtime_config(event_store, otp_app, opts) do
    config =
      otp_app
      |> Application.get_env(event_store, [])
      |> Keyword.merge(opts)
      |> Keyword.put(:otp_app, otp_app)

    ## Supervisor callbacks
    case event_store_init(event_store, config) do
      {:ok, config} ->
        config = Config.parse(config)

        {:ok, config}

      :ignore ->
        :ignore
    end
  end

  @doc false
  def init({event_store, otp_app, name, opts}) do
    case runtime_config(event_store, otp_app, opts) do
      {:ok, config} ->
        config = validate_config!(event_store, name, config)

        conn = Keyword.fetch!(config, :conn)
        schema = Keyword.fetch!(config, :schema)

        advisory_locks_name = Module.concat([name, AdvisoryLocks])
        subscriptions_name = Module.concat([name, Subscriptions.Supervisor])
        subscriptions_registry_name = Module.concat([name, Subscriptions.Registry])

        sqlite_config = Config.sqlite_opts(config, conn)

        children =
          [
            Supervisor.child_spec(
              {MonitoredServer,
               mfa: {EventStoreLite.SQLite, :start_link, [sqlite_config]},
               name: Module.concat([name, :SQLite, MonitoredServer]),
               backoff_min: 0},
              id: Module.concat([conn, MonitoredServer])
            ),
            %{
              id: :initialize_storage,
              start: {__MODULE__, :initialize_storage, [conn, config]},
              restart: :temporary
            },
            {AdvisoryLocks,
             conn: conn,
             monitored_server: Module.concat([name, :SQLite, MonitoredServer]),
             database: Keyword.fetch!(config, :database),
             schema: schema,
             name: advisory_locks_name}
          ] ++
            PubSub.child_spec(name) ++
            [
              {Subscriptions.Supervisor, name: subscriptions_name},
              Supervisor.child_spec({Registry, keys: :unique, name: subscriptions_registry_name},
                id: subscriptions_registry_name
              ),
              {Notifications.Supervisor, {name, config}}
            ]

        :ok = Config.associate(name, self(), event_store, config)

        Supervisor.init(children, strategy: :one_for_all)

      :ignore ->
        :ignore
    end
  end

  @doc false
  def initialize_storage(conn, config) do
    # Initialize each schema synchronously, including shared connections, before
    # notification listeners establish their baseline.
    case EventStoreLite.Storage.Initializer.run!(conn, config) do
      {:ok, :ok} -> :ignore
      {:error, _} = failure -> failure
    end
  end

  defp event_store_init(event_store, config) do
    if Code.ensure_loaded?(event_store) and function_exported?(event_store, :init, 1) do
      event_store.init(config)
    else
      {:ok, config}
    end
  end

  defp validate_config!(event_store, name, config) do
    conn = sqlite_conn(name, config)
    column_data_type = Config.column_data_type(event_store, config)
    correlation_id_type = Config.correlation_id_type(event_store, config)
    causation_id_type = Config.causation_id_type(event_store, config)
    serializer = Serializer.serializer(event_store, config)
    subscription_retry_interval = Subscriptions.retry_interval(event_store, config)
    subscription_hibernate_after = Subscriptions.hibernate_after(event_store, config)

    Keyword.merge(config,
      conn: conn,
      column_data_type: column_data_type,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type,
      serializer: serializer,
      subscription_retry_interval: subscription_retry_interval,
      subscription_hibernate_after: subscription_hibernate_after
    )
  end

  # Get the name of the main SQLite database connection.
  #
  # By default each event store instance will start its own connection.
  #
  # The `:shared_connection_pool` config option can be used to share the same
  # database connection between multiple event store instances when they
  # connect to the same physical database. This will reduce the total number of
  # connections.
  defp sqlite_conn(name, config) do
    case Keyword.get(config, :shared_connection_pool) do
      nil ->
        Module.concat([name, :SQLite])

      shared_connection_pool when is_atom(shared_connection_pool) ->
        Module.concat([shared_connection_pool, :SQLite])

      invalid ->
        raise ArgumentError,
              "Invalid `:shared_connection_pool` specified, expected an atom but got: " <>
                inspect(invalid)
    end
  end
end
