defmodule EventStoreLite.SharedConnectionPoolTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.MonitoredServer
  alias EventStoreLite.MonitoredServer.State, as: MonitoredServerState
  alias EventStoreLite.Tasks.Create
  alias EventStoreLite.Tasks.Drop
  alias EventStoreLite.Tasks.Init
  alias EventStoreLite.UUID
  alias EventStoreLite.Wait

  describe "connection pool sharing" do
    setup do
      for schema <- ["schema1", "schema2"] do
        config = Keyword.put(TestEventStoreLite.config(), :schema, schema)

        Create.exec(config, quiet: true)
        Init.exec(config, quiet: true)
      end

      start_supervised!(
        {TestEventStoreLite, name: :eventstore_lite1, shared_connection_pool: :shared_pool, schema: "schema1"}
      )

      start_supervised!(
        {TestEventStoreLite, name: :eventstore_lite2, shared_connection_pool: :shared_pool, schema: "schema2"}
      )

      start_supervised!({TestEventStoreLite, name: :eventstore_lite3, schema: "schema3"})

      on_exit(fn ->
        for schema <- ["schema1", "schema2", "schema3"] do
          config = Keyword.put(TestEventStoreLite.config(), :schema, schema)

          Drop.exec(config, quiet: true)
        end
      end)
    end

    test "should only start one SQLite connection pool" do
      # Event stores sharing a connection pool should use the same `EventStoreLite.SQLite` conn
      conn = Process.whereis(Module.concat([:shared_pool, SQLite]))
      assert is_pid(conn)
      assert_sqlite_connection(conn)

      pid1 = Process.whereis(Module.concat([:eventstore_lite1, SQLite, MonitoredServer]))

      assert is_pid(pid1)
      assert %MonitoredServerState{pid: ^conn} = :sys.get_state(pid1)

      pid2 = Process.whereis(Module.concat([:eventstore_lite2, SQLite, MonitoredServer]))

      assert is_pid(pid2)
      assert %MonitoredServerState{pid: ^conn} = :sys.get_state(pid2)
    end

    test "start a separate SQLite connection for non-shared connection pools" do
      shared_conn = Process.whereis(Module.concat([:shared_pool, SQLite]))

      # An event store started without specifying a connection pool should start its own pool
      conn = Process.whereis(Module.concat([:eventstore_lite3, SQLite]))

      assert is_pid(conn)
      assert_sqlite_connection(conn)
      assert shared_conn != conn
    end

    test "stopping event store with shared connection pool should not stop connection pool" do
      conn = Process.whereis(Module.concat([:shared_pool, SQLite]))
      assert is_pid(conn)

      ref = Process.monitor(conn)

      stop_supervised!(:eventstore_lite1)

      refute_receive {:DOWN, ^ref, :process, _object, _reason}

      # Ensure existing EventStoreLite.SQLite connection can still be used
      assert {:ok, _events} = append_events_to_stream(:eventstore_lite2, UUID.uuid4(), 1)

      stop_supervised!(:eventstore_lite2)

      # Stopping the last event store using the shared pool should stop the EventStoreLite.SQLite connection
      assert_receive {:DOWN, ^ref, :process, _object, _reason}
    end

    @tag :manual
    @tag :postgres_only
    @tag skip: "PostgreSQL-only mechanics; see PARITY.md"
    test "ensure database connections are shared between instances" do
      # Starting another event store instance using shared pool should only
      # increase connection count by one (used for advisory locks).
      assert_connection_count_diff(1, fn ->
        start_supervised!({TestEventStoreLite, shared_connection_pool: :shared_pool, name: :eventstore_lite4})
      end)

      assert_connection_count_diff(1, fn ->
        start_supervised!({TestEventStoreLite, shared_connection_pool: :shared_pool, name: :eventstore_lite5})
      end)

      assert_connection_count_diff(3, fn ->
        start_supervised!(
          {TestEventStoreLite, shared_connection_pool: :another_pool, name: :eventstore_lite6, pool_size: 1}
        )
      end)

      # Start another event store instance with its own connection pool of size 10
      assert_connection_count_diff(12, fn ->
        start_supervised!({TestEventStoreLite, name: :eventstore_lite7, pool_size: 10})
      end)

      assert_connection_count_diff(-12, fn -> stop_supervised!(:eventstore_lite7) end)
      assert_connection_count_diff(-3, fn -> stop_supervised!(:eventstore_lite6) end)
      assert_connection_count_diff(-1, fn -> stop_supervised!(:eventstore_lite5) end)
      assert_connection_count_diff(-1, fn -> stop_supervised!(:eventstore_lite4) end)
    end

    test "append and read events" do
      stream_uuid = UUID.uuid4()

      {:ok, events} = append_events_to_stream(:eventstore_lite1, stream_uuid, 3)

      assert_recorded_events(:eventstore_lite1, stream_uuid, events)
      refute_stream_exists(:eventstore_lite2, stream_uuid)
    end

    test "subscribe to stream" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription1} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "subscriber1", self(), name: :eventstore_lite1)

      {:ok, subscription2} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "subscriber2", self(), name: :eventstore_lite2)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}

      {:ok, _events} = append_events_to_stream(:eventstore_lite1, stream_uuid, 3)

      assert_receive {:events, _events}
      refute_receive {:events, _events}
    end

    test "subscription to stream continues to receive events after event store with shared pool is stopped" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription} =
        TestEventStoreLite.subscribe_to_stream(stream_uuid, "subscriber2", self(), name: :eventstore_lite2)

      assert_receive {:subscribed, ^subscription}

      {:ok, _events} = append_events_to_stream(:eventstore_lite2, stream_uuid, 1)

      assert_receive {:events, received_events}

      :ok = TestEventStoreLite.ack(subscription, received_events)

      refute_received {:events, _events}

      # Stop eventstore1
      stop_supervised!(:eventstore_lite1)

      # Append new events to stream should be received via eventstore2 subscription
      {:ok, _events} = append_events_to_stream(:eventstore_lite2, stream_uuid, 1, 1)

      assert_receive {:events, received_events}

      :ok = TestEventStoreLite.ack(subscription, received_events)

      refute_received {:events, _events}
    end
  end

  # Check that this is a `EventStoreLite.SQLite` process by executing a database query.
  defp assert_sqlite_connection(conn) do
    assert {:ok, %{rows: [[1]]}} = EventStoreLite.SQLite.query(conn, "SELECT 1;", [])
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

  defp refute_stream_exists(event_store_name, stream_uuid) do
    assert {:error, :stream_not_found} ==
             TestEventStoreLite.stream_forward(stream_uuid, 0, name: event_store_name)
  end

  defp assert_connection_count_diff(expected_diff, fun) when is_function(fun, 0) do
    conn = Process.whereis(Module.concat([:shared_pool, SQLite]))

    count_before = count_connections(conn)

    fun.()

    Wait.until(fn ->
      assert count_connections(conn) == count_before + expected_diff
    end)
  end

  defp count_connections(conn) do
    %{rows: [[count]]} =
      EventStoreLite.SQLite.query!(
        conn,
        "SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid();",
        []
      )

    count
  end
end
