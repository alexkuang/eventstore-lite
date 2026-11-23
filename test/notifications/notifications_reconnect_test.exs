defmodule EventStoreLite.Notifications.NotificationsReconnectTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.ProcessHelper
  alias EventStoreLite.PubSub
  alias EventStoreLite.SQLite
  alias EventStoreLite.Wait

  describe "notifications reconnect" do
    test "resume after disconnect" do
      :ok = PubSub.subscribe(TestEventStoreLite, "example-stream")
      name = Module.concat(TestEventStoreLite, EventStoreLite.Notifications.Listener)
      old_pid = Process.whereis(name)
      assert is_pid(old_pid)
      ProcessHelper.shutdown(old_pid)

      Wait.until(fn ->
        pid = Process.whereis(name)
        assert is_pid(pid) and pid != old_pid
      end)

      :ok = append_events(3)
      assert_receive {:events, events}
      assert length(events) == 3
      refute_receive {:events, _}
    end

    test "publisher handle SQLite connection down when reading events" do
      :ok = PubSub.subscribe(TestEventStoreLite, "example-stream")
      :ok = append_events(3)
      assert_receive {:events, events}
      assert length(events) == 3

      name = TestEventStoreLite.SQLite
      old_pid = Process.whereis(name)
      assert is_pid(old_pid)
      ProcessHelper.shutdown(old_pid)

      Wait.until(fn ->
        pid = Process.whereis(name)
        assert is_pid(pid) and pid != old_pid
        assert {:ok, %{rows: [[1]]}} = SQLite.query(name, "SELECT 1", [])
      end)

      :ok = append_events(3, 3)
      assert_receive {:events, events}
      assert Enum.map(events, & &1.stream_version) == [4, 5, 6]
    end
  end

  defp append_events(count, expected_version \\ 0) do
    TestEventStoreLite.append_to_stream(
      "example-stream",
      expected_version,
      EventFactory.create_events(count, expected_version)
    )
  end
end
