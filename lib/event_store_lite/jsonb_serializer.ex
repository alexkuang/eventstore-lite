defmodule EventStoreLite.JsonbSerializer do
  @moduledoc """
  A serializer for JSON-compatible values, using Jason.

  Typed replay reconstructs only the outer event struct. Nested objects keep
  string keys, including objects inside arrays. Nested structs require a
  `Jason.Encoder` implementation and replay as JSON values, not structs.
  Use `TermSerializer` when Elixir terms must survive unchanged.
  """

  @behaviour EventStoreLite.Serializer

  def serialize(%_{} = term), do: term |> Map.from_struct() |> serialize()

  def serialize(term), do: term |> Jason.encode!() |> Jason.decode!()

  def deserialize(term, config) do
    case Keyword.get(config, :type) do
      nil -> term
      type -> type |> String.to_existing_atom() |> to_struct(term)
    end
  end

  defp to_struct(type, term) do
    fields =
      for {field, _default} <- Map.from_struct(struct(type)),
          {:ok, value} <- [Map.fetch(term, Atom.to_string(field))] do
        {field, value}
      end

    struct(type, fields)
  end
end
