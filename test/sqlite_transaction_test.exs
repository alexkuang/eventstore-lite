defmodule EventStoreLite.SQLiteTransactionTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.Config
  alias EventStoreLite.EventFactory
  alias EventStoreLite.SQLite

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    start_supervised!({TestEventStoreLite, database: Path.join(tmp_dir, "transactions.sqlite3")})
    conn = Keyword.fetch!(Config.lookup(TestEventStoreLite), :conn)
    SQLite.query!(conn, "CREATE TABLE business_entries (id INTEGER PRIMARY KEY)")
    %{conn: conn}
  end

  test "a rescued nested constraint error rolls back the append and business write", %{conn: conn} do
    failed_events = EventFactory.create_events(1)
    committed_events = EventFactory.create_events(1)

    failed_operation = fn inner ->
      assert :ok = TestEventStoreLite.append_to_stream("failed", 0, failed_events, conn: inner)
      insert_entry(inner, 2)
      insert_entry(inner, 1)
    end

    assert {:ok, :ok} =
             SQLite.transaction(conn, fn outer ->
               insert_entry(outer, 1)

               assert_raise RuntimeError, ~r/UNIQUE constraint failed: business_entries.id/, fn ->
                 SQLite.transaction(outer, failed_operation)
               end

               insert_entry(outer, 3)
               TestEventStoreLite.append_to_stream("committed", 0, committed_events, conn: outer)
             end)

    assert %{rows: [[1], [3]]} = SQLite.query!(conn, "SELECT id FROM business_entries ORDER BY id")
    assert {:error, :stream_not_found} = TestEventStoreLite.read_stream_forward("failed")
    assert {:ok, [%{stream_uuid: "committed", event_number: 1}]} = TestEventStoreLite.read_all_streams_forward()
  end

  test "caught throws and exits roll back nested writes and retain their reasons", %{conn: conn} do
    throwing = fn inner ->
      insert_entry(inner, 1)
      throw({:unexpected, :throw})
    end

    exiting = fn inner ->
      insert_entry(inner, 2)
      exit({:unexpected, :exit})
    end

    assert {:ok, :continued} =
             SQLite.transaction(conn, fn outer ->
               assert catch_throw(SQLite.transaction(outer, throwing)) == {:unexpected, :throw}
               assert catch_exit(SQLite.transaction(outer, exiting)) == {:unexpected, :exit}
               :continued
             end)

    assert %{rows: []} = SQLite.query!(conn, "SELECT id FROM business_entries")
  end

  test "explicit rollback and error returns undo nested writes while success retains them", %{conn: conn} do
    assert {:ok, :ok} =
             SQLite.transaction(conn, fn outer ->
               assert {:error, :explicit} =
                        SQLite.transaction(outer, fn inner ->
                          insert_entry(inner, 1)
                          SQLite.rollback(inner, :explicit)
                        end)

               assert {:error, :returned} =
                        SQLite.transaction(outer, fn inner ->
                          insert_entry(inner, 2)
                          {:error, :returned}
                        end)

               assert {:ok, :saved} =
                        SQLite.transaction(outer, fn inner ->
                          insert_entry(inner, 3)
                          :saved
                        end)

               :ok
             end)

    assert %{rows: [[3]]} = SQLite.query!(conn, "SELECT id FROM business_entries")
  end

  test "preserves the constraint exception when SQLite already rolled back the savepoint", %{conn: conn} do
    assert {:error, %RuntimeError{message: message}} =
             SQLite.transaction(conn, fn outer ->
               insert_entry(outer, 1)

               SQLite.transaction(outer, fn inner ->
                 SQLite.query!(inner, "INSERT OR ROLLBACK INTO business_entries VALUES (1)")
               end)
             end)

    assert message =~ "UNIQUE constraint failed: business_entries.id"
    assert %{rows: []} = SQLite.query!(conn, "SELECT id FROM business_entries")
  end

  defp insert_entry(conn, id), do: SQLite.query!(conn, "INSERT INTO business_entries VALUES (?)", [id])
end
