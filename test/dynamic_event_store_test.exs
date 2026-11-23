defmodule EventStoreLite.DynamicEventStoreLiteTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.UUID

  describe "dynamic event store" do
    setup do
      start_supervised!({TestEventStoreLite, name: :eventstore_lite1, schema: "public"})
      start_supervised!({TestEventStoreLite, name: :eventstore_lite2, schema: "public"})
      start_supervised!({TestEventStoreLite, name: :schema_eventstore, schema: "example"})
      :ok
    end

    test "should list all running instances" do
      all_instances = Enum.sort(EventStoreLite.all_instances())

      assert all_instances == [
               {TestEventStoreLite, name: TestEventStoreLite},
               {TestEventStoreLite, name: :eventstore_lite1},
               {TestEventStoreLite, name: :eventstore_lite2},
               {TestEventStoreLite, name: :schema_eventstore}
             ]

      stop_supervised!(:eventstore_lite1)
      stop_supervised!(:eventstore_lite2)

      all_instances = Enum.sort(EventStoreLite.all_instances())

      assert all_instances == [
               {TestEventStoreLite, name: TestEventStoreLite},
               {TestEventStoreLite, name: :schema_eventstore}
             ]
    end

    test "should ensure name is an atom" do
      assert_raise ArgumentError, "expected `:name` to be an atom, got: \"invalid\"", fn ->
        TestEventStoreLite.start_link(name: "invalid")
      end
    end

    test "should append and read events" do
      stream_uuid = UUID.uuid4()

      {:ok, events} = append_events_to_stream(:eventstore_lite1, stream_uuid, 3)

      assert_recorded_events(:eventstore_lite1, stream_uuid, events)
      assert_recorded_events(:eventstore_lite2, stream_uuid, events)

      assert TestEventStoreLite.stream_forward(stream_uuid, 0, name: :schema_eventstore) ==
               {:error, :stream_not_found}
    end

    test "should support subscriptions to named event stores" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription1} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "test1", self(), name: :eventstore_lite1)

      {:ok, subscription2} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "test2", self(), name: :eventstore_lite2)

      {:ok, subscription3} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "test3", self(), name: :schema_eventstore)

      assert [subscription1, subscription2, subscription3] |> Enum.uniq() |> length() == 3

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}
      assert_receive {:subscribed, ^subscription3}

      {:ok, events} = append_events_to_stream(:eventstore_lite1, stream_uuid, 1)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      refute_receive {:events, _received_events}
    end

    test "should unsubscribe and delete subscription from named event store" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription1} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "test1", self(), name: :eventstore_lite1)

      {:ok, subscription2} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "test2", self(), name: :eventstore_lite2)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}

      {:ok, events} = append_events_to_stream(:eventstore_lite1, stream_uuid, 1)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      refute_receive {:events, _received_events}

      assert :ok =
               TestEventStoreLite.unsubscribe_from_stream(stream_uuid, "test1", name: :eventstore_lite1)

      assert :ok = TestEventStoreLite.delete_subscription(stream_uuid, "test1", name: :eventstore_lite1)

      # Recreate subscription from `:origin` should receive all events again
      {:ok, subscription1} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "test1", self(), name: :eventstore_lite1)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:events, received_events}
      assert_events(events, received_events)

      refute_receive {:events, _received_events}
    end
  end

  defp append_events_to_stream(event_store_name, stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version + 1)

    :ok =
      TestEventStoreLite.append_to_stream(stream_uuid, expected_version, events, name: event_store_name)

    {:ok, events}
  end

  defp assert_recorded_events(event_store_name, stream_uuid, expected_events) do
    actual_events =
      stream_uuid |> TestEventStoreLite.stream_forward(0, name: event_store_name) |> Enum.to_list()

    assert_events(expected_events, actual_events)
  end

  defp assert_events(expected_events, actual_events) do
    assert length(expected_events) == length(actual_events)

    for {expected, actual} <- Enum.zip(expected_events, actual_events) do
      assert_event(expected, actual)
    end
  end

  defp assert_event(expected_event, actual_event) do
    assert expected_event.correlation_id == actual_event.correlation_id
    assert expected_event.causation_id == actual_event.causation_id
    assert expected_event.event_type == actual_event.event_type
    assert expected_event.data == actual_event.data
    assert expected_event.metadata == actual_event.metadata
  end
end
