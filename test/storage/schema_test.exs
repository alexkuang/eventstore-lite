defmodule EventStoreLite.Storage.SchemaTest do
  use ExUnit.Case, async: false

  alias EventStoreLite.Storage.Schema

  setup do
    config =
      Keyword.update!(SchemaEventStoreLite.config(), :schema, fn schema -> schema <> "_temp" end)

    on_exit(fn ->
      Schema.drop(config)
    end)

    [config: config]
  end

  test "create schema when already exists", %{config: config} do
    assert Schema.create(config) == :ok
    assert Schema.create(config) == {:error, :already_up}
  end

  test "drop schema when already dropped", %{config: config} do
    :ok = Schema.create(config)

    assert Schema.drop(config) == :ok
    assert Schema.drop(config) == {:error, :already_down}
  end
end
