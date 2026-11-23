if Code.ensure_loaded?(Commanded.EventStore.Adapter) do
  defmodule EventStoreLite.Commanded.Mapper do
    @moduledoc false

    alias Commanded.EventStore.EventData
    alias Commanded.EventStore.SnapshotData

    def to_event_data(%EventData{} = event_data) do
      %EventData{
        causation_id: causation_id,
        correlation_id: correlation_id,
        event_type: event_type,
        data: data,
        metadata: metadata
      } = event_data

      %EventStoreLite.EventData{
        causation_id: causation_id,
        correlation_id: correlation_id,
        event_type: event_type,
        data: data,
        metadata: metadata
      }
    end

    def from_recorded_event(%EventStoreLite.RecordedEvent{} = event) do
      %EventStoreLite.RecordedEvent{
        event_id: event_id,
        event_number: event_number,
        stream_uuid: stream_uuid,
        stream_version: stream_version,
        correlation_id: correlation_id,
        causation_id: causation_id,
        event_type: event_type,
        data: data,
        metadata: metadata,
        created_at: created_at
      } = event

      %Commanded.EventStore.RecordedEvent{
        event_id: event_id,
        event_number: event_number,
        stream_id: stream_uuid,
        stream_version: stream_version,
        correlation_id: correlation_id,
        causation_id: causation_id,
        event_type: event_type,
        data: data,
        metadata: metadata,
        created_at: created_at
      }
    end

    def to_snapshot_data(%SnapshotData{} = snapshot) do
      %SnapshotData{
        source_uuid: source_uuid,
        source_version: source_version,
        source_type: source_type,
        data: data,
        metadata: metadata,
        created_at: created_at
      } = snapshot

      %EventStoreLite.Snapshots.SnapshotData{
        source_uuid: source_uuid,
        source_version: source_version,
        source_type: source_type,
        data: data,
        metadata: metadata,
        created_at: created_at
      }
    end

    def from_snapshot_data(%EventStoreLite.Snapshots.SnapshotData{} = snapshot) do
      %EventStoreLite.Snapshots.SnapshotData{
        source_uuid: source_uuid,
        source_version: source_version,
        source_type: source_type,
        data: data,
        metadata: metadata,
        created_at: created_at
      } = snapshot

      %SnapshotData{
        source_uuid: source_uuid,
        source_version: source_version,
        source_type: source_type,
        data: data,
        metadata: metadata,
        created_at: created_at
      }
    end
  end
end
