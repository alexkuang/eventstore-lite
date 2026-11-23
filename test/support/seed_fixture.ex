defmodule EventStoreLite.SeedFixture do
  @moduledoc false
  alias EventStoreLite.EventData
  alias EventStoreLite.Snapshots.SnapshotData
  alias EventStoreLite.UUID

  def run(opts \\ []) do
    append_events(opts)
    link_events(opts)
    record_snapshots(opts)
    subscribe_to_streams()
  end

  defp append_events(opts) do
    stream_count = Keyword.get(opts, :stream_count, 10)
    event_count = Keyword.get(opts, :event_count, 10)

    for stream_index <- 1..stream_count do
      events =
        for event_number <- 1..event_count do
          %EventData{
            correlation_id: UUID.uuid4(),
            causation_id: UUID.uuid4(),
            event_type: "Elixir.Event",
            data: struct(Event, data: event_number),
            metadata: %{"user" => "user@example.com", "timestamp" => DateTime.utc_now()}
          }
        end

      :ok = TestEventStoreLite.append_to_stream("stream-#{stream_index}", 0, events)
    end
  end

  defp link_events(opts) do
    stream_count = Keyword.get(opts, :stream_count, 10)

    for stream_index <- 1..stream_count do
      {:ok, events} = TestEventStoreLite.read_stream_forward("stream-#{stream_index}", 0, 1)

      :ok = TestEventStoreLite.link_to_stream("linked-stream", stream_index - 1, events)
    end
  end

  defp record_snapshots(opts) do
    snapshot_count = Keyword.get(opts, :snapshot_count, 10)

    for i <- 1..snapshot_count do
      snapshot = %SnapshotData{
        source_uuid: "snapshot-#{i}",
        source_version: 1,
        source_type: "Elixir.Snapshot",
        data: struct(Snapshot, data: i)
      }

      :ok = TestEventStoreLite.record_snapshot(snapshot)
    end
  end

  defp subscribe_to_streams do
    for {stream, name, start_from} <- [
          {"$all", "subscription-all-origin", :origin},
          {"$all", "subscription-all-current", :current},
          {"stream-1", "subscription-stream-1-origin", :origin},
          {"stream-1", "subscription-stream-1-current", :current}
        ] do
      {:ok, subscription} = TestEventStoreLite.subscribe_to_stream(stream, name, self(), start_from: start_from)

      receive do
        {:subscribed, ^subscription} -> :ok
      after
        5_000 -> raise "fixture subscription did not become durable: #{name}"
      end
    end

    :ok
  end
end
