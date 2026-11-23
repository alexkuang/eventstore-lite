defmodule EventStoreLite.SharedStorageTestCase do
  @moduledoc false
  defmacro define_tests(do: block) do
    quote do
      defmacro __using__(options) do
        block = unquote(Macro.escape(block))

        quote do
          use EventStoreLite.StorageCase, unquote(Keyword.take(options, [:async]))

          @moduletag unquote(Keyword.delete(options, :async))

          unquote(block)
        end
      end
    end
  end
end
