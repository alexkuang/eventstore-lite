defmodule EventStoreLite.Storage.Appender do
  @moduledoc false
  alias EventStoreLite.SQLite
  alias EventStoreLite.Storage

  def append(_conn, _id, [], _opts), do: :ok

  def append(conn, id, [first | _] = events, opts) do
    transaction(
      conn,
      fn tx ->
        with :ok <- unique_events(tx, events, opts),
             {:ok, stream} <- Storage.stream_info(tx, first.stream_uuid, opts),
             {:ok, id, version} <- prepare(tx, id, stream, first, opts) do
          insert_events(tx, id, version, events, opts)
        end
      end,
      opts
    )
  end

  defp unique_events(tx, events, opts) do
    ids = Enum.map(events, & &1.event_id)

    if MapSet.size(MapSet.new(ids)) == length(ids) do
      ids
      |> Enum.chunk_every(500)
      |> Enum.reduce_while(:ok, fn batch, :ok ->
        placeholders = Enum.map_join(1..length(batch), ",", &"?#{&1}")

        case SQLite.query(tx, "SELECT event_id FROM events WHERE event_id IN (#{placeholders}) LIMIT 1", batch, opts) do
          {:ok, %{rows: []}} -> {:cont, :ok}
          {:ok, _} -> {:halt, {:error, :duplicate_event}}
          error -> {:halt, error}
        end
      end)
    else
      {:error, :duplicate_event}
    end
  end

  defp prepare(tx, nil, %{stream_id: nil}, first, opts) do
    case SQLite.query(
           tx,
           "INSERT INTO streams(stream_uuid,created_at) VALUES(?1,?2) RETURNING stream_id",
           [first.stream_uuid, opts[:created_at_override] || DateTime.utc_now()],
           opts
         ) do
      {:ok, %{rows: [[id]]}} -> {:ok, id, 0}
      error -> error
    end
  end

  defp prepare(_, nil, %{stream_id: id, stream_version: version, status: :created}, _, opts) do
    if opts[:expected_version] == :any_version, do: {:ok, id, version}, else: {:error, :duplicate_stream_uuid}
  end

  defp prepare(_, nil, _, _, _), do: {:error, :wrong_expected_version}

  defp prepare(_, id, %{stream_id: id, stream_version: version, status: :created}, first, opts) do
    if opts[:expected_version] == :any_version or first.stream_version == version + 1,
      do: {:ok, id, version},
      else: {:error, :wrong_expected_version}
  end

  defp prepare(_, _, _, _, _), do: {:error, :not_found}

  defp insert_events(tx, id, version, events, opts) do
    {:ok, %{rows: [[all_version]]}} = SQLite.query(tx, "SELECT stream_version FROM streams WHERE stream_id=0", [], opts)

    events
    |> Enum.with_index(1)
    |> Enum.reduce_while(:ok, fn {event, index}, :ok ->
      params = [
        event.event_id,
        event.event_type,
        event.causation_id,
        event.correlation_id,
        SQLite.payload(event.data),
        SQLite.payload(event.metadata),
        event.created_at || DateTime.utc_now()
      ]

      with {:ok, _} <-
             SQLite.query(
               tx,
               "INSERT INTO events(event_id,event_type,causation_id,correlation_id,data,metadata,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",
               params,
               opts
             ),
           {:ok, _} <-
             SQLite.query(
               tx,
               "INSERT INTO stream_events(event_id,stream_id,stream_version,original_stream_id,original_stream_version) VALUES(?1,?2,?3,?2,?3),(?1,0,?4,?2,?3)",
               [event.event_id, id, version + index, all_version + index],
               opts
             ) do
        {:cont, :ok}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      :ok ->
        SQLite.query!(
          tx,
          "UPDATE streams SET stream_version=stream_version+?1 WHERE stream_id IN (0,?2)",
          [length(events), id],
          opts
        )

        notify(tx, id, version + 1, version + length(events), opts)
        notify(tx, 0, all_version + 1, all_version + length(events), opts)
        :ok

      error ->
        error
    end
  end

  def link(conn, id, ids, opts \\ []) do
    transaction(
      conn,
      fn tx ->
        case SQLite.query(
               tx,
               "SELECT stream_version FROM streams WHERE stream_id=?1 AND deleted_at IS NULL",
               [id],
               opts
             ) do
          {:ok, %{rows: [[version]]}} ->
            result =
              ids
              |> Enum.with_index(1)
              |> Enum.reduce_while(:ok, fn {event_id, index}, :ok ->
                # All links must share the version allocation transaction.
                # credo:disable-for-next-line Credo.Check.Refactor.Nesting
                case SQLite.query(
                       tx,
                       "INSERT INTO stream_events(event_id,stream_id,stream_version,original_stream_id,original_stream_version) SELECT event_id,?1,?2,original_stream_id,original_stream_version FROM stream_events WHERE stream_id=0 AND event_id=?3",
                       [id, version + index, event_id],
                       opts
                     ) do
                  {:ok, %{num_rows: 1}} -> {:cont, :ok}
                  {:ok, _} -> {:halt, {:error, :not_found}}
                  error -> {:halt, error}
                end
              end)

            with :ok <- result do
              SQLite.query!(
                tx,
                "UPDATE streams SET stream_version=stream_version+?1 WHERE stream_id=?2",
                [length(ids), id],
                opts
              )

              notify(tx, id, version + 1, version + length(ids), opts)
              :ok
            end

          {:ok, _} ->
            {:error, :not_found}

          error ->
            error
        end
      end,
      opts
    )
  end

  defp notify(_tx, _id, first, last, _opts) when first > last, do: :ok

  defp notify(tx, id, first, last, opts) do
    SQLite.query!(
      tx,
      "INSERT INTO stream_notifications(stream_id,stream_uuid,from_stream_version,to_stream_version) SELECT stream_id,stream_uuid,?1,?2 FROM streams WHERE stream_id=?3",
      [first, last, id],
      opts
    )
  end

  defp transaction(conn, fun, opts) do
    case SQLite.transaction(conn, fun, opts) do
      {:ok, value} ->
        value

      {:error, reason} ->
        message = inspect(reason)

        cond do
          String.contains?(message, "UNIQUE") ->
            {:error, :duplicate_event}

          String.contains?(message, "no such table") ->
            {:error, :undefined_table}

          String.contains?(message, "FOREIGN KEY") ->
            {:error, :not_found}

          true ->
            {:error, reason}
        end
    end
  end
end
