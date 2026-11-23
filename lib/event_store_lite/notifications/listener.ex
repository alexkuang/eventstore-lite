defmodule EventStoreLite.Notifications.Listener do
  @moduledoc """
  Produces notifications by polling committed SQLite notification ranges.

  The durable outbox preserves append boundaries and observes other connections.
  Establishes the initial baseline before store startup returns.
  """

  use GenStage

  alias EventStoreLite.Notifications.Listener
  alias EventStoreLite.Notifications.Notification
  alias EventStoreLite.SQLite

  defstruct [:conn, :query_timeout, :schema, :poll_interval, cursor: 0, demand: 0, queue: :queue.new()]

  def start_link(opts) do
    state = %Listener{
      conn: Keyword.fetch!(opts, :conn),
      query_timeout: Keyword.fetch!(opts, :query_timeout),
      schema: Keyword.fetch!(opts, :schema),
      poll_interval: Keyword.get(opts, :poll_interval, 5)
    }

    GenStage.start_link(__MODULE__, state, Keyword.take(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after]))
  end

  @impl true
  def init(state) do
    case SQLite.query(state.conn, "SELECT COALESCE(MAX(notification_id), 0) FROM stream_notifications", [],
           schema: state.schema,
           timeout: state.query_timeout
         ) do
      {:ok, %{rows: [[cursor]]}} ->
        schedule_poll(state)
        {:producer, %{state | cursor: cursor}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    state =
      case read_notifications(state) do
        {:ok, %{rows: rows}} -> enqueue_notifications(rows, state)
        {:error, _reason} -> state
      end

    schedule_poll(state)
    dispatch_events([], state)
  end

  @impl true
  def handle_demand(incoming_demand, state) do
    dispatch_events([], %{state | demand: state.demand + incoming_demand})
  end

  defp read_notifications(state) do
    # Keep the producer queue bounded while the publisher has no demand.
    if :queue.is_empty(state.queue) do
      SQLite.query(
        state.conn,
        "SELECT notification_id, stream_id, stream_uuid, from_stream_version, to_stream_version " <>
          "FROM stream_notifications WHERE notification_id > ? ORDER BY notification_id LIMIT 1000",
        [state.cursor],
        schema: state.schema,
        timeout: state.query_timeout
      )
    else
      {:ok, %{rows: []}}
    end
  end

  defp schedule_poll(state), do: Process.send_after(self(), :poll, state.poll_interval)

  defp enqueue_notifications(rows, state) do
    Enum.reduce(rows, state, fn [cursor, id, uuid, first, last], state ->
      notification = %Notification{
        stream_id: id,
        stream_uuid: uuid,
        from_stream_version: first,
        to_stream_version: last
      }

      %{state | cursor: cursor, queue: :queue.in(notification, state.queue)}
    end)
  end

  defp dispatch_events(events, %{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, state) do
    case :queue.out(state.queue) do
      {{:value, event}, queue} ->
        dispatch_events([event | events], %{state | demand: state.demand - 1, queue: queue})

      {:empty, _queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end
end
