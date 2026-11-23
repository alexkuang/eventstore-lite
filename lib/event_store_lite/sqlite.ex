defmodule EventStoreLite.SQLite do
  @moduledoc "Owns a SQLite connection and serializes complete write transactions."
  use GenServer

  alias Exqlite.Sqlite3

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  def query(conn, sql, params \\ [], opts \\ [])
  def query({__MODULE__, db}, sql, params, opts), do: execute(db, sql, params, opts)

  def query(conn, sql, params, opts), do: call(conn, {:query, sql, params, opts}, opts)

  def execute_script(conn, sql, opts \\ []), do: call(conn, {:script, sql, opts}, opts)

  def query!(conn, sql, params \\ [], opts \\ []) do
    case query(conn, sql, params, opts) do
      {:ok, result} -> result
      {:error, reason} -> raise "SQLite query failed: #{inspect(reason)}"
    end
  end

  def transaction(conn, fun, opts \\ [])

  def transaction({__MODULE__, db} = conn, fun, _opts) do
    savepoint = "nested_#{System.unique_integer([:positive])}"
    :ok = Sqlite3.execute(db, "SAVEPOINT #{savepoint}")

    result =
      try do
        fun.(conn)
      catch
        {:sqlite_rollback, reason} ->
          {:error, reason}

        kind, reason ->
          rollback_savepoint(db, savepoint)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end

    case result do
      {:error, _} = error ->
        rollback_savepoint(db, savepoint)
        error

      value ->
        Sqlite3.execute(db, "RELEASE #{savepoint}")
        {:ok, value}
    end
  end

  def transaction(conn, fun, opts), do: call(conn, {:transaction, fun}, opts)

  defp rollback_savepoint(db, savepoint) do
    # SQLite can roll back the entire transaction on a constraint error. Preserve the original failure if that happens.
    Sqlite3.execute(db, "ROLLBACK TO #{savepoint}; RELEASE #{savepoint}")
  end

  defp call(conn, message, opts) do
    if Keyword.get(opts, :timeout) == 0 do
      {:error, DBConnection.ConnectionError.exception("query timeout")}
    else
      GenServer.call(conn, message, Keyword.get(opts, :timeout, 15_000))
    end
  end

  def rollback(_conn, reason), do: throw({:sqlite_rollback, reason})

  @impl true
  def init(opts) do
    path = Keyword.fetch!(opts, :database)
    Process.put(:identity, {Path.expand(path), Keyword.get(opts, :schema, "public")})
    if path != ":memory:", do: File.mkdir_p!(Path.dirname(path))

    with {:ok, db} <- Sqlite3.open(path),
         :ok <- Sqlite3.execute(db, "PRAGMA foreign_keys=ON; PRAGMA busy_timeout=15000; PRAGMA journal_mode=WAL;") do
      {:ok, db}
    end
  end

  def lock(conn, key, schema), do: GenServer.call(conn, {:lock, key, schema})
  def unlock(conn, key, schema), do: GenServer.call(conn, {:unlock, key, schema})
  @impl true
  def handle_call({:lock, key, schema}, _from, db) do
    {path, default_schema} = Process.get(:identity)
    name = {EventStoreLite.AdvisoryLocks, {path, schema || default_schema}, key}

    owned = Process.get({:lock, name})

    result =
      case :global.whereis_name(name) do
        pid when is_pid(pid) and pid == owned ->
          :ok

        :undefined ->
          owner = self()

          holder =
            spawn(fn ->
              ref = Process.monitor(owner)

              receive do
                :stop -> :ok
                {:DOWN, ^ref, :process, ^owner, _} -> :ok
              end
            end)

          case :global.register_name(name, holder) do
            :yes ->
              Process.put({:lock, name}, holder)
              :ok

            :no ->
              send(holder, :stop)
              {:error, :lock_already_taken}
          end

        _ ->
          {:error, :lock_already_taken}
      end

    {:reply, result, db}
  end

  def handle_call({:unlock, key, schema}, _from, db) do
    {path, default_schema} = Process.get(:identity)
    name = {EventStoreLite.AdvisoryLocks, {path, schema || default_schema}, key}

    if holder = Process.delete({:lock, name}) do
      if :global.whereis_name(name) == holder, do: :global.unregister_name(name)
      send(holder, :stop)
    end

    {:reply, :ok, db}
  end

  def handle_call({:script, sql, opts}, _from, db) do
    schema = Keyword.get(opts, :schema, elem(Process.get(:identity), 1))
    {:reply, Sqlite3.execute(db, namespace(IO.iodata_to_binary(sql), schema)), db}
  end

  def handle_call({:query, sql, params, opts}, _from, db), do: {:reply, execute(db, sql, params, opts), db}

  def handle_call({:transaction, fun}, _from, db) do
    result =
      with :ok <- Sqlite3.execute(db, "BEGIN IMMEDIATE") do
        try do
          case fun.({__MODULE__, db}) do
            {:error, reason} ->
              Sqlite3.execute(db, "ROLLBACK")
              {:error, reason}

            value ->
              case Sqlite3.execute(db, "COMMIT") do
                :ok ->
                  {:ok, value}

                error ->
                  Sqlite3.execute(db, "ROLLBACK")
                  error
              end
          end
        rescue
          exception ->
            Sqlite3.execute(db, "ROLLBACK")
            {:error, exception}
        catch
          {:sqlite_rollback, reason} ->
            Sqlite3.execute(db, "ROLLBACK")
            {:error, reason}
        end
      end

    {:reply, result, db}
  end

  @impl true
  def terminate(_, db), do: Sqlite3.close(db)

  defp execute(db, sql, params, opts) do
    sql =
      sql
      |> IO.iodata_to_binary()
      |> namespace(Keyword.get(opts, :schema, elem(Process.get(:identity), 1)))

    with {:ok, stmt} <- Sqlite3.prepare(db, sql) do
      try do
        with :ok <- Sqlite3.bind(stmt, Enum.map(params, &encode/1)),
             {:ok, rows} <- Sqlite3.fetch_all(db, stmt),
             {:ok, changes} <- Sqlite3.changes(db),
             {:ok, columns} <- Sqlite3.columns(db, stmt) do
          {:ok,
           %{
             rows: Enum.map(rows, fn row -> Enum.zip_with(columns, row, &decode/2) end),
             num_rows: if(columns == [], do: changes, else: length(rows))
           }}
        end
      after
        Sqlite3.release(db, stmt)
      end
    end
  end

  @tables ~w(streams events stream_events stream_notifications subscriptions snapshots schema_migrations store_settings)
  @sql_tokens ~r/'(?:''|[^'])*'|--[^\n]*(?:\n|\z)|\/\*[\s\S]*?\*\/|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[a-zA-Z_][a-zA-Z0-9_]*|\s+|./s

  def namespace(sql, schema) do
    if schema not in [nil, "public"] and not Regex.match?(~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/, schema),
      do: raise(ArgumentError, "invalid schema")

    folded = if schema, do: String.downcase(schema)
    prefix = schema_prefix(schema)

    @sql_tokens
    |> Regex.scan(sql)
    |> Enum.map(fn [token] -> sql_token(token) end)
    |> rewrite_identifiers(folded, prefix)
    |> IO.iodata_to_binary()
  end

  defp schema_prefix(schema) when schema in [nil, "public"], do: ""

  defp schema_prefix(schema), do: "esl$" <> Base.encode16(schema, case: :lower) <> "__"

  # Strings and comments remain opaque; only identifier tokens can be namespaced.
  defp sql_token(<<quote, _::binary>> = token) when quote in [?", ?`, ?[] do
    name = token |> binary_part(1, byte_size(token) - 2) |> String.replace(<<quote, quote>>, <<quote>>)
    {:identifier, token, String.downcase(name)}
  end

  defp sql_token(token) do
    cond do
      String.starts_with?(token, ["'", "--", "/*"]) -> {:opaque, token}
      Regex.match?(~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/, token) -> {:identifier, token, String.downcase(token)}
      String.trim(token) == "" -> {:trivia, token}
      true -> {:symbol, token}
    end
  end

  defp rewrite_identifiers([], _schema, _prefix), do: []

  defp rewrite_identifiers([{:identifier, _, qualifier} = token | rest], schema, prefix) do
    {before_dot, following} = Enum.split_while(rest, &trivia?/1)

    case following do
      [{:symbol, "."} | after_dot] when qualifier == schema or qualifier == "public" ->
        {after_dot_space, following_table} = Enum.split_while(after_dot, &trivia?/1)

        case following_table do
          [{:identifier, _, table} | _] when table in @tables ->
            [
              Enum.map(before_dot ++ after_dot_space, &token_text/1)
              | rewrite_identifiers(following_table, schema, prefix)
            ]

          _ ->
            [rewrite_identifier(token, prefix) | rewrite_identifiers(rest, schema, prefix)]
        end

      _ ->
        [rewrite_identifier(token, prefix) | rewrite_identifiers(rest, schema, prefix)]
    end
  end

  defp rewrite_identifiers([token | rest], schema, prefix),
    do: [token_text(token) | rewrite_identifiers(rest, schema, prefix)]

  defp trivia?({:trivia, _}), do: true
  defp trivia?({:opaque, "--" <> _}), do: true
  defp trivia?({:opaque, "/*" <> _}), do: true
  defp trivia?(_), do: false

  defp token_text({:identifier, raw, _}), do: raw
  defp token_text({_, raw}), do: raw

  defp rewrite_identifier({:identifier, raw, _}, ""), do: raw

  defp rewrite_identifier({:identifier, raw, name}, prefix) do
    if name in @tables or name == "ix_stream_events_event_id" or String.starts_with?(name, "protect_") do
      case raw do
        <<quote, rest::binary>> when quote in [?", ?`, ?[] -> <<quote>> <> prefix <> rest
        _ -> prefix <> raw
      end
    else
      raw
    end
  end

  def encode(%DateTime{} = value), do: DateTime.to_iso8601(value)
  def encode(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value) <> "Z"

  def encode(value) when is_map(value) or is_list(value), do: payload(value)
  def encode(value), do: value

  @doc false
  def payload(nil), do: nil
  def payload(value), do: {:blob, :erlang.term_to_binary({:event_store_lite_payload, value})}

  defp decode(column, value) when column in ["data", "metadata"] and is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:event_store_lite_payload, payload} -> payload
      _ -> value
    end
  rescue
    ArgumentError -> value
  end

  defp decode(_column, value), do: value

  def timestamp(nil), do: nil
  def timestamp(%DateTime{} = value), do: value

  def timestamp(value) do
    {:ok, datetime, _} = DateTime.from_iso8601(value)
    datetime
  end
end
