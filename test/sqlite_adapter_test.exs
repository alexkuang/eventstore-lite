defmodule EventStoreLite.SQLiteAdapterTest.Store do
  @moduledoc false
  use EventStoreLite, otp_app: :eventstore_lite
end

defmodule EventStoreLite.SQLiteAdapterTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.EventData
  alias EventStoreLite.SQLiteAdapterTest.Store
  alias EventStoreLite.Tasks.Init

  setup do
    path = Path.join(System.tmp_dir!(), "eventstore-lite-regression-#{System.unique_integer([:positive])}.sqlite3")
    config = [database: path, serializer: EventStoreLite.TermSerializer]
    :ok = Init.exec(EventStoreLite.Config.parse(config), quiet: true)
    start_supervised!({Store, config})
    on_exit(fn -> for suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix) end)
    %{path: path, config: config}
  end

  test "payload envelopes cannot corrupt identifiers or arbitrary binary payloads" do
    payloads = [
      <<0, 255, 131>>,
      "ESLTERMnot-an-erlang-term",
      <<"ESLTERM", :erlang.term_to_binary(%{value: 1})::binary>>,
      %{nested: [1, 2]}
    ]

    events = Enum.map(payloads, &%EventData{event_type: "ESLTERM-event", data: &1, metadata: %{source: "test"}})
    :ok = Store.append_to_stream("ESLTERM-stream", 0, events)
    assert {:ok, recorded} = Store.read_stream_forward("ESLTERM-stream")
    assert Enum.map(recorded, & &1.data) == payloads
    assert Enum.all?(recorded, &(&1.event_type == "ESLTERM-event"))
  end

  test "independent SQLite connections atomically enforce optimistic concurrency", %{config: config} do
    start_supervised!({Store, Keyword.put(config, :name, :sqlite_second_writer)}, id: :second_writer)

    results =
      1..16
      |> Task.async_stream(
        fn index ->
          name = if rem(index, 2) == 0, do: Store, else: :sqlite_second_writer
          Store.append_to_stream("contended", 0, [%EventData{event_type: "test", data: index}], name: name)
        end,
        max_concurrency: 16
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.count(results, &(&1 == {:error, :wrong_expected_version})) == 15
  end

  test "case-distinct schemas isolate append, read, and hard delete", %{config: config} do
    config = Keyword.merge(config, enable_hard_deletes: true, shared_connection_pool: :case_sensitive_pool)

    for {name, schema} <- [lower: "acme", mixed: "Acme", upper: "ACME"] do
      start_supervised!({Store, Keyword.merge(config, name: name, schema: schema)})
      assert :ok = Store.append_to_stream("same-stream", 0, [%EventData{event_type: "test", data: schema}], name: name)
    end

    assert {:ok, [%{data: "Acme"}]} = Store.read_stream_forward("same-stream", 0, 10, name: :mixed)
    assert :ok = Store.delete_stream("same-stream", 1, :hard, name: :mixed)
    assert {:error, :stream_not_found} = Store.read_stream_forward("same-stream", 0, 10, name: :mixed)
    assert {:ok, [%{data: "acme"}]} = Store.read_stream_forward("same-stream", 0, 10, name: :lower)
    assert {:ok, [%{data: "ACME"}]} = Store.read_stream_forward("same-stream", 0, 10, name: :upper)
  end

  test "invalid schema names are rejected", %{config: config} do
    for schema <- ["esl$41636d65", "ESL$41636D65"] do
      assert {:error,
              {{:shutdown, {:failed_to_start_child, :initialize_storage, %ArgumentError{message: "invalid schema"}}}, _}} =
               start_supervised({Store, Keyword.merge(config, name: :invalid, schema: schema)})
    end
  end

  test "file-backed streams survive closing and reopening the store", %{config: config} do
    :ok = Store.append_to_stream("durable", 0, [%EventData{event_type: "test", data: %{value: 42}}])
    assert {:ok, before_events} = Store.read_stream_forward("durable")
    stop_supervised!(Store)
    start_supervised!({Store, config})
    assert {:ok, ^before_events} = Store.read_stream_forward("durable")
  end
end
