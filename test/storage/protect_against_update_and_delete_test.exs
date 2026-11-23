defmodule EventStoreLite.Storage.ProtectAgainstUpdateAndDeleteTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.SQLite

  setup do
    :ok = TestEventStoreLite.append_to_stream("protected", 0, EventFactory.create_events(1))
    :ok
  end

  describe "event store table protection" do
    test "cannot delete from `events`, `streams`, and `stream_events` tables", %{conn: conn} do
      for table <- ["events", "streams", "stream_events"] do
        assert {:error, message} = SQLite.query(conn, "DELETE FROM #{table};", [])
        assert message == "Cannot delete #{table}"
      end
    end

    test "cannot update `events` table", %{conn: conn} do
      assert {:error, "Cannot update events"} =
               SQLite.query(conn, "UPDATE events SET created_at = CURRENT_TIMESTAMP;", [])
    end

    test "cannot update `stream_events` table", %{conn: conn} do
      assert {:error, "Cannot update stream_events"} = SQLite.query(conn, "UPDATE stream_events SET stream_id = 1;", [])
    end
  end

  describe "event store enable hard deletes" do
    test "can delete from `events`, `streams`, and `stream_events` tables" do
      # SQLite has no session GUC. Exercise the supported hard-delete API instead.
      stop_supervised!(TestEventStoreLite)
      start_supervised!({TestEventStoreLite, enable_hard_deletes: true})
      assert :ok = TestEventStoreLite.delete_stream("protected", 1, :hard)
      assert {:error, :stream_not_found} = TestEventStoreLite.read_stream_forward("protected")
      conn = TestEventStoreLite.SQLite
      assert {:ok, %{rows: [[0]]}} = SQLite.query(conn, "SELECT COUNT(*) FROM events", [])
      assert {:ok, %{rows: [[0]]}} = SQLite.query(conn, "SELECT COUNT(*) FROM stream_events", [])
      assert {:ok, %{rows: [[1]]}} = SQLite.query(conn, "SELECT COUNT(*) FROM streams", [])
    end
  end
end
