defmodule EventStoreLite.Storage.Initializer do
  @moduledoc false

  alias EventStoreLite.Sql.Statements

  def run!(conn, config, opts \\ []) do
    opts = Keyword.put(opts, :schema, config[:schema])
    statements = Statements.initializers(config)

    execute!(conn, statements, opts)
  end

  def reset!(conn, config, opts \\ []) do
    opts = Keyword.put(opts, :schema, config[:schema])
    statements = Statements.reset(config)

    execute!(conn, statements, opts)
  end

  defp execute!(conn, statements, opts) do
    EventStoreLite.SQLite.transaction(
      conn,
      fn transaction ->
        Enum.each(statements, &query!(transaction, &1, opts))
      end,
      opts
    )
  end

  defp query!(conn, statement, opts) do
    EventStoreLite.SQLite.query!(conn, statement, [], opts)
  end
end
