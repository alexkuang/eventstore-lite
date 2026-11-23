defmodule EventStoreLite.Storage.Reader do
  @moduledoc false

  alias EventStoreLite.RecordedEvent
  alias EventStoreLite.Sql.Statements
  alias EventStoreLite.Storage.Reader
  alias EventStoreLite.UUID

  require Logger

  @doc """
  Read events from a single stream forwards from the given starting version.
  """
  def read_forward(conn, stream_id, start_version, count, opts) do
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    case Reader.Query.read_events_forward(conn, stream_id, start_version, count, opts) do
      {:ok, []} = reply -> reply
      {:ok, rows} -> map_rows_to_event_data(rows, correlation_id_type, causation_id_type)
      {:error, reason} -> failed_to_read(stream_id, reason)
    end
  end

  @doc """
  Read events from a single stream backwards from the given starting version.
  """
  def read_backward(conn, stream_id, start_version, count, opts) do
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    case Reader.Query.read_events_backward(conn, stream_id, start_version, count, opts) do
      {:ok, []} = reply -> reply
      {:ok, rows} -> map_rows_to_event_data(rows, correlation_id_type, causation_id_type)
      {:error, reason} -> failed_to_read(stream_id, reason)
    end
  end

  defp map_rows_to_event_data(rows, correlation_id_type, causation_id_type) do
    {:ok, Reader.EventAdapter.to_event_data(rows, correlation_id_type, causation_id_type)}
  end

  defp failed_to_read(stream_id, reason) do
    Logger.warning("Failed to read events from stream id #{stream_id} due to: #{inspect(reason)}")

    {:error, reason}
  end

  defmodule EventAdapter do
    @moduledoc false

    @doc """
    Map event data from the database to `RecordedEvent` struct
    """
    def to_event_data(rows, correlation_id_type, causation_id_type),
      do: Enum.map(rows, &to_event_data_from_row(&1, correlation_id_type, causation_id_type))

    def to_event_data_from_row(
          [
            event_number,
            event_id,
            stream_uuid,
            stream_version,
            event_type,
            correlation_id,
            causation_id,
            data,
            metadata,
            created_at
          ],
          correlation_id_type,
          causation_id_type
        ) do
      %RecordedEvent{
        event_number: event_number,
        event_id: from_uuid(event_id),
        stream_uuid: stream_uuid,
        stream_version: stream_version,
        event_type: event_type,
        correlation_id: decode_id(correlation_id, correlation_id_type),
        causation_id: decode_id(causation_id, causation_id_type),
        data: data,
        metadata: metadata,
        created_at: from_timestamp(created_at)
      }
    end

    defp from_uuid(nil), do: nil
    defp from_uuid(uuid), do: if(byte_size(uuid) == 16, do: UUID.binary_to_string!(uuid), else: uuid)

    defp decode_id(nil, _type), do: nil
    defp decode_id(value, "uuid"), do: if(byte_size(value) == 16, do: UUID.binary_to_string!(value), else: value)
    defp decode_id(value, "text"), do: value

    defp from_timestamp(%DateTime{} = timestamp), do: timestamp

    defp from_timestamp(%NaiveDateTime{} = timestamp) do
      DateTime.from_naive(timestamp, "Etc/UTC")
    end

    defp from_timestamp(value), do: EventStoreLite.SQLite.timestamp(value)
  end

  defmodule Query do
    @moduledoc false

    def read_events_forward(conn, stream_id, start_version, count, opts) do
      schema = Keyword.get(opts, :schema)

      query = Statements.query_stream_events_forward(schema)

      do_query(conn, query, [stream_id, start_version, count], opts)
    end

    def read_events_backward(conn, stream_id, start_version, count, opts) do
      schema = Keyword.get(opts, :schema)

      query = Statements.query_stream_events_backward(schema)

      do_query(conn, query, [stream_id, start_version, count], opts)
    end

    defp do_query(conn, query, params, opts) do
      case EventStoreLite.SQLite.query(conn, query, params, opts) do
        {:ok, %{num_rows: 0}} ->
          {:ok, []}

        {:ok, %{rows: rows}} ->
          {:ok, rows}

        {:error, error} = reply ->
          Logger.warning("Failed to read events from stream due to: " <> inspect(error))

          reply
      end
    end
  end
end
