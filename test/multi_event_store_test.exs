defmodule EventStoreLite.MultiEventStoreLiteTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.Config
  alias EventStoreLite.EventData
  alias EventStoreLite.EventFactory
  alias EventStoreLite.RecordedEvent
  alias EventStoreLite.Snapshots.SnapshotData
  alias EventStoreLite.Storage
  alias EventStoreLite.UUID

  setup do
    reset_event_store!(SecondEventStoreLite)
    start_supervised!({SecondEventStoreLite, name: SecondEventStoreLite})

    :ok
  end

  describe "append to multiple event stores" do
    test "should append events to single store" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      :ok = TestEventStoreLite.append_to_stream(stream_uuid, 0, events)

      assert_read_stream_events(TestEventStoreLite, stream_uuid, events)
      assert_read_all_stream_events(TestEventStoreLite, events)

      assert {:error, :stream_not_found} = SecondEventStoreLite.read_stream_forward(stream_uuid)
    end

    test "should append events to multiple stores" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      :ok = TestEventStoreLite.append_to_stream(stream_uuid, 0, events)
      :ok = SecondEventStoreLite.append_to_stream(stream_uuid, 0, events)

      assert_read_stream_events(TestEventStoreLite, stream_uuid, events)
      assert_read_stream_events(SecondEventStoreLite, stream_uuid, events)

      assert_read_all_stream_events(TestEventStoreLite, events)
      assert_read_all_stream_events(SecondEventStoreLite, events)
    end
  end

  describe "transient subscriptions" do
    test "should only notify subscriber from single event store" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      :ok = TestEventStoreLite.subscribe(stream_uuid)

      :ok = TestEventStoreLite.append_to_stream(stream_uuid, 0, events)
      :ok = SecondEventStoreLite.append_to_stream(stream_uuid, 0, events)

      assert_receive_events(stream_uuid, events)
      refute_receive {:events, _events}
    end

    test "should notify subscribers from multiple stores" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      :ok = TestEventStoreLite.subscribe(stream_uuid)
      :ok = SecondEventStoreLite.subscribe(stream_uuid)

      :ok = TestEventStoreLite.append_to_stream(stream_uuid, 0, events)
      :ok = SecondEventStoreLite.append_to_stream(stream_uuid, 0, events)

      assert_receive_events(stream_uuid, events)
      assert_receive_events(stream_uuid, events)
      refute_receive {:events, _events}
    end
  end

  describe "persistent subscription" do
    test "should notify subscribers from multiple stores" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      {:ok, subscription1} =
        TestEventStoreLite.subscribe_to_all_streams("subscription", self(), buffer_size: 3)

      {:ok, subscription2} =
        SecondEventStoreLite.subscribe_to_all_streams("subscription", self(), buffer_size: 3)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}

      :ok = TestEventStoreLite.append_to_stream(stream_uuid, 0, events)
      :ok = SecondEventStoreLite.append_to_stream(stream_uuid, 0, events)

      assert_receive_events(stream_uuid, events)
      assert_receive_events(stream_uuid, events)
      refute_receive {:events, _events}
    end
  end

  defmodule ExampleData do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:data]
  end

  describe "snapshots" do
    test "should record and read snapshot from multiple stores" do
      source_uuid = UUID.uuid4()

      assert {:error, :snapshot_not_found} == TestEventStoreLite.read_snapshot(source_uuid)
      snapshot1 = record_snapshot(TestEventStoreLite, source_uuid)

      assert {:error, :snapshot_not_found} == SecondEventStoreLite.read_snapshot(source_uuid)
      snapshot2 = record_snapshot(SecondEventStoreLite, source_uuid)

      assert_snapshot(TestEventStoreLite, snapshot1)
      assert_snapshot(SecondEventStoreLite, snapshot2)
    end
  end

  defp assert_snapshot(event_store, snapshot) do
    {:ok, read_snapshot} = event_store.read_snapshot(snapshot.source_uuid)

    assert snapshot.source_uuid == read_snapshot.source_uuid
    assert snapshot.source_version == read_snapshot.source_version
    assert snapshot.source_type == read_snapshot.source_type
    assert snapshot.data == read_snapshot.data
  end

  defp assert_read_all_stream_events(event_store, expected_events) do
    {:ok, recorded_events} = event_store.read_all_streams_forward()

    assert length(expected_events) == length(recorded_events)

    expected_events
    |> Enum.zip(recorded_events)
    |> Enum.with_index()
    |> Enum.each(fn {{expected_event, recorded_event}, index} ->
      assert_recorded_event(recorded_event.stream_uuid, index + 1, expected_event, recorded_event)
    end)
  end

  defp assert_read_stream_events(event_store, stream_uuid, expected_events) do
    {:ok, recorded_events} = event_store.read_stream_forward(stream_uuid)

    assert_recorded_events(stream_uuid, expected_events, recorded_events)
  end

  defp assert_receive_events(expected_stream_uuid, expected_events) do
    assert_receive {:events, received_events}

    assert_recorded_events(expected_stream_uuid, expected_events, received_events)
  end

  defp assert_recorded_events(expected_stream_uuid, expected_events, recorded_events) do
    assert length(expected_events) == length(recorded_events)

    expected_events
    |> Enum.zip(recorded_events)
    |> Enum.with_index()
    |> Enum.each(fn {{expected_event, recorded_event}, index} ->
      assert_recorded_event(expected_stream_uuid, index + 1, expected_event, recorded_event)
    end)
  end

  defp assert_recorded_event(
         expected_stream_uuid,
         expected_stream_version,
         %EventData{} = expected_event,
         %RecordedEvent{} = recorded_event
       ) do
    assert_is_uuid(recorded_event.event_id)
    assert_is_uuid(recorded_event.causation_id)
    assert_is_uuid(recorded_event.correlation_id)
    assert recorded_event.stream_uuid == expected_stream_uuid
    assert recorded_event.stream_version == expected_stream_version
    assert recorded_event.event_type == expected_event.event_type
    assert recorded_event.data == expected_event.data
    assert recorded_event.metadata == expected_event.metadata
    assert %DateTime{} = recorded_event.created_at
  end

  defp assert_is_uuid(uuid) do
    assert uuid |> UUID.string_to_binary!() |> is_binary()
  end

  # defp pluck(enumerable, field), do: Enum.map(enumerable, &Map.get(&1, field))
  defp record_snapshot(event_store, source_uuid) do
    snapshot = %SnapshotData{
      source_uuid: source_uuid,
      source_version: 1,
      source_type: Atom.to_string(ExampleData),
      data: %ExampleData{data: "some data"}
    }

    :ok = event_store.record_snapshot(snapshot)

    snapshot
  end

  defp reset_event_store!(event_store) do
    config = event_store.config()
    sqlite_config = Config.default_sqlite_opts(config)

    {:ok, conn} = EventStoreLite.SQLite.start_link(sqlite_config)

    try do
      Storage.Initializer.run!(conn, config)
      Storage.Initializer.reset!(conn, config)
    after
      GenServer.stop(conn)
    end
  end
end
