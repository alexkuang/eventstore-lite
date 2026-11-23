defmodule EventStoreLite.SQLiteNamespaceTest do
  use ExUnit.Case, async: true

  alias EventStoreLite.SQLite
  alias EventStoreLite.Storage
  alias EventStoreLite.Storage.Initializer

  setup do
    conn = start_supervised!({SQLite, database: ":memory:", schema: "tenant"})
    assert {:ok, :ok} = Initializer.run!(conn, schema: "tenant")
    %{conn: conn}
  end

  test "single-quoted literals and comments are not identifiers", %{conn: conn} do
    sql =
      "SELECT 'streams', 'events', 'public.events', 'tenant.streams', 'it''s events' -- streams public.events\n/* events tenant.streams */"

    assert SQLite.namespace(sql, "tenant") == sql

    assert {:ok, %{rows: [["streams", "events", "public.events", "tenant.streams", "it's events"]]}} =
             SQLite.query(conn, sql)
  end

  test "qualified and quoted identifiers resolve without touching literal data", %{conn: conn} do
    for table <- [
          "streams",
          "\"streams\"",
          "`streams`",
          "[streams]",
          ~s("tenant" . "streams"),
          "tenant./* events */streams",
          "public.streams"
        ] do
      assert {:ok, %{rows: [["$all", "tenant.streams"]]}} =
               SQLite.query(conn, "SELECT stream_uuid, 'tenant.streams' FROM #{table}")
    end

    sql = "-- events\nINSERT INTO streams(stream_uuid) VALUES ('events'); /* streams */"
    assert :ok = SQLite.execute_script(conn, sql)

    assert {:ok, %{rows: [["events"]]}} =
             SQLite.query(conn, "SELECT stream_uuid FROM streams WHERE stream_uuid='events'")
  end

  test "reset restarts only the selected schema's sequences", %{conn: conn} do
    assert {:ok, :ok} = Initializer.run!(conn, schema: "other")
    assert {:ok, 1} = Storage.create_stream(conn, "one", schema: "tenant")
    assert {:ok, 2} = Storage.create_stream(conn, "two", schema: "tenant")
    assert {:ok, 1} = Storage.create_stream(conn, "other", schema: "other")
    assert {:ok, %{subscription_id: 1}} = Storage.subscribe_to_stream(conn, "$all", "one", 0, schema: "tenant")
    assert {:ok, %{subscription_id: 2}} = Storage.subscribe_to_stream(conn, "$all", "two", 0, schema: "tenant")

    assert {:ok, _} =
             SQLite.query(
               conn,
               "INSERT INTO stream_notifications(stream_id,stream_uuid,from_stream_version,to_stream_version) VALUES(0,'$all',1,1)",
               [],
               schema: "tenant"
             )

    assert {:ok, :ok} = Initializer.reset!(conn, schema: "tenant")

    assert {:ok, 1} = Storage.create_stream(conn, "after-reset", schema: "tenant")
    assert {:ok, 2} = Storage.create_stream(conn, "other-after-reset", schema: "other")
    assert {:ok, %{subscription_id: 1}} = Storage.subscribe_to_stream(conn, "$all", "after-reset", 0, schema: "tenant")

    assert {:ok, %{rows: [[1]]}} =
             SQLite.query(
               conn,
               "INSERT INTO stream_notifications(stream_id,stream_uuid,from_stream_version,to_stream_version) VALUES(0,'$all',1,1) RETURNING notification_id",
               [],
               schema: "tenant"
             )
  end
end
