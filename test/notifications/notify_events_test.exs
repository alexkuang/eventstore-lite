defmodule EventStoreLite.Notifications.NotifyEventsTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.PubSub
  alias EventStoreLite.Storage.Initializer

  setup do
    config = SchemaEventStoreLite.config()
    conn = start_supervised!({EventStoreLite.SQLite, config}, id: :schema_notification_conn)
    Initializer.run!(conn, config)
    Initializer.reset!(conn, config)
    start_supervised!(SchemaEventStoreLite)
    :ok = PubSub.subscribe(TestEventStoreLite, "example-stream")
    :ok = PubSub.subscribe(TestEventStoreLite, "$all")
    :ok
  end

  test "should notify events when appended" do
    for {count, version} <- [{3, 0}, {2, 3}, {1, 5}] do
      events = EventFactory.create_events(count, version)
      :ok = TestEventStoreLite.append_to_stream("example-stream", version, events)

      # Both the stream and all-stream feeds must publish the committed range.
      for _ <- 1..2 do
        assert_receive {:events, received}
        assert Enum.map(received, & &1.stream_version) == Enum.to_list((version + 1)..(version + count))
      end

      refute_receive {:events, _}
    end
  end

  test "should not notify events appended to another schema" do
    events = EventFactory.create_events(3)
    :ok = SchemaEventStoreLite.append_to_stream("example-stream", 0, events)
    refute_receive {:events, _}
  end
end
