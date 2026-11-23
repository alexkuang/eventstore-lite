defmodule EventStoreLite.Notifications.Supervisor do
  @moduledoc false

  # Supervises the individual `GenStage` stages used to listen to, read, and
  # publish all events appended to storage.

  use Supervisor

  alias EventStoreLite.Notifications.Listener
  alias EventStoreLite.Notifications.Publisher
  alias EventStoreLite.Subscriptions

  def child_spec({event_store, _config} = init_arg) do
    %{id: Module.concat(event_store, __MODULE__), start: {__MODULE__, :start_link, [init_arg]}}
  end

  def start_link({_event_store, _config} = init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl Supervisor
  def init({event_store, config}) do
    conn = Keyword.fetch!(config, :conn)
    schema = Keyword.fetch!(config, :schema)
    serializer = Keyword.fetch!(config, :serializer)
    query_timeout = Keyword.fetch!(config, :timeout)
    correlation_id_type = Keyword.get(config, :correlation_id_type, "uuid")
    causation_id_type = Keyword.get(config, :causation_id_type, "uuid")

    listener_name = Module.concat([event_store, Listener])
    publisher_name = Module.concat([event_store, Publisher])
    hibernate_after = Subscriptions.hibernate_after(event_store, config)

    Supervisor.init(
      [
        {Listener,
         conn: conn,
         poll_interval: Keyword.get(config, :notification_poll_interval, 5),
         query_timeout: query_timeout,
         schema: schema,
         name: listener_name,
         hibernate_after: hibernate_after},
        {Publisher,
         conn: conn,
         query_timeout: query_timeout,
         event_store: event_store,
         schema: schema,
         serializer: serializer,
         correlation_id_type: correlation_id_type,
         causation_id_type: causation_id_type,
         subscribe_to: listener_name,
         name: publisher_name,
         hibernate_after: hibernate_after}
      ],
      strategy: :one_for_all
    )
  end
end
