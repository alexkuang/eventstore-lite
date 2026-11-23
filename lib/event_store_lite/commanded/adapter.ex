if Code.ensure_loaded?(Commanded.EventStore.Adapter) do
  defmodule EventStoreLite.Commanded.Adapter do
    @moduledoc """
    EventStoreLite adapter for Commanded.

    See the [Commanded setup](readme.html#commanded) for configuration instructions.
    """

    @behaviour Commanded.EventStore.Adapter

    alias Commanded.EventStore.Adapter
    alias Commanded.EventStore.RecordedEvent
    alias EventStoreLite.Commanded.Mapper

    @all_stream "$all"

    @impl Adapter
    def child_spec(application, config) do
      {event_store, config} = Keyword.pop(config, :event_store)

      verify_event_store!(application, event_store)

      name = Keyword.get(config, :name, event_store)

      # Rename `prefix` config to `schema`
      config =
        case Keyword.pop(config, :prefix) do
          {nil, config} -> config
          {prefix, config} -> Keyword.put(config, :schema, prefix)
        end

      child_spec = [{event_store, config}]
      adapter_meta = %{event_store: event_store, name: name}

      {:ok, child_spec, adapter_meta}
    end

    @impl Adapter
    def append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts \\ []) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      events = Enum.map(events, &Mapper.to_event_data/1)

      opts = Keyword.put(opts, :name, name)
      event_store.append_to_stream(stream_uuid, expected_version, events, opts)
    end

    @impl Adapter
    def stream_forward(adapter_meta, stream_uuid, start_version \\ 0, read_batch_size \\ 1_000) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      opts = [name: name, read_batch_size: read_batch_size]

      case event_store.stream_forward(stream_uuid, start_version, opts) do
        {:error, error} -> {:error, error}
        stream -> Stream.map(stream, &Mapper.from_recorded_event/1)
      end
    end

    @impl Adapter
    def subscribe(adapter_meta, :all), do: subscribe(adapter_meta, @all_stream)

    @impl Adapter
    def subscribe(adapter_meta, stream_uuid) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      event_store.subscribe(stream_uuid, name: name, mapper: &Mapper.from_recorded_event/1)
    end

    @impl Adapter
    def subscribe_to(adapter_meta, :all, subscription_name, subscriber, start_from, opts) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      event_store.subscribe_to_all_streams(
        subscription_name,
        subscriber,
        subscription_options(name, start_from, opts)
      )
    end

    @impl Adapter
    def subscribe_to(adapter_meta, stream_uuid, subscription_name, subscriber, start_from, opts) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      event_store.subscribe_to_stream(
        stream_uuid,
        subscription_name,
        subscriber,
        subscription_options(name, start_from, opts)
      )
    end

    @impl Adapter
    def ack_event(adapter_meta, subscription, %RecordedEvent{} = event) do
      %RecordedEvent{event_number: event_number} = event

      {event_store, _name} = extract_adapter_meta(adapter_meta)

      event_store.ack(subscription, event_number)
    end

    @impl Adapter
    def unsubscribe(_adapter_meta, subscription) do
      EventStoreLite.Subscriptions.Subscription.unsubscribe(subscription)
    end

    @impl Adapter
    def delete_subscription(adapter_meta, :all, subscription_name) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      event_store.delete_subscription(@all_stream, subscription_name, name: name)
    end

    @impl Adapter
    def delete_subscription(adapter_meta, stream_uuid, subscription_name) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      event_store.delete_subscription(stream_uuid, subscription_name, name: name)
    end

    @impl Adapter
    def read_snapshot(adapter_meta, source_uuid) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      with {:ok, snapshot_data} <- event_store.read_snapshot(source_uuid, name: name) do
        snapshot = Mapper.from_snapshot_data(snapshot_data)

        {:ok, snapshot}
      end
    end

    @impl Adapter
    def record_snapshot(adapter_meta, %Commanded.EventStore.SnapshotData{} = snapshot) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      snapshot
      |> Mapper.to_snapshot_data()
      |> event_store.record_snapshot(name: name)
    end

    @impl Adapter
    def delete_snapshot(adapter_meta, source_uuid) do
      {event_store, name} = extract_adapter_meta(adapter_meta)

      event_store.delete_snapshot(source_uuid, name: name)
    end

    defp subscription_options(name, start_from, opts) do
      opts
      |> Keyword.merge(
        name: name,
        start_from: start_from,
        mapper: &Mapper.from_recorded_event/1
      )
      |> Keyword.update(:partition_by, nil, fn
        nil ->
          nil

        partition_by when is_function(partition_by, 1) ->
          fn %EventStoreLite.RecordedEvent{} = event ->
            event |> Mapper.from_recorded_event() |> partition_by.()
          end
      end)
    end

    defp extract_adapter_meta(adapter_meta) do
      event_store = Map.fetch!(adapter_meta, :event_store)
      name = Map.fetch!(adapter_meta, :name)

      {event_store, name}
    end

    defp verify_event_store!(application, event_store) do
      if !event_store do
        raise ArgumentError,
              "missing :event_store option for event store adapter in application " <>
                inspect(application)
      end

      if !implements?(event_store, EventStoreLite) do
        raise ArgumentError,
              "module " <>
                inspect(event_store) <>
                " is not an EventStoreLite, " <>
                "ensure you pass an event store module to the :event_store config in application " <>
                inspect(application)
      end
    end

    # Returns `true` if module implements behaviour.
    defp implements?(module, behaviour) do
      behaviours = Keyword.take(module.__info__(:attributes), [:behaviour])

      [behaviour] in Keyword.values(behaviours)
    end
  end
end
