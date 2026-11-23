defmodule EventStoreLite.JsonbSerializerTest do
  use ExUnit.Case, async: true

  alias EventStoreLite.EventData
  alias EventStoreLite.JsonbSerializer
  alias EventStoreLite.RecordedEvent

  @moduletag :tmp_dir

  defmodule Store do
    @moduledoc false
    use EventStoreLite, otp_app: :eventstore_lite
  end

  defmodule AddressEvent do
    @moduledoc false
    defstruct [:address, country: "Spain"]
  end

  defmodule Address do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:city]
  end

  setup %{tmp_dir: tmp_dir} do
    config = [
      database: Path.join(tmp_dir, "events.sqlite3"),
      serializer: JsonbSerializer,
      column_data_type: "jsonb"
    ]

    start_supervised!({Store, config})
    %{config: config}
  end

  test "typed replay normalizes nested atom and string keys consistently", %{config: config} do
    input = %AddressEvent{address: %{city: "Madrid", alternatives: [%{"city" => "Seville"}, %{city: "Granada"}]}}

    expected = %AddressEvent{
      address: %{"city" => "Madrid", "alternatives" => [%{"city" => "Seville"}, %{"city" => "Granada"}]}
    }

    assert_replay(input, nil, expected, nil, config)
  end

  test "nested structs use JSON encoding in objects and arrays", %{config: config} do
    address = %Address{city: "Madrid"}
    input = %AddressEvent{address: %{primary: address, alternatives: [address, [address]], missing: nil}}
    json_address = %{"city" => "Madrid"}

    expected = %AddressEvent{
      address: %{"primary" => json_address, "alternatives" => [json_address, [json_address]], "missing" => nil}
    }

    assert_replay(input, nil, expected, nil, config)
  end

  test "metadata uses the same JSON boundary without reconstructing structs", %{config: config} do
    input = %AddressEvent{address: "Madrid"}
    metadata = %{source: "test", addresses: [%Address{city: "Madrid"}], verified: true, count: 2}
    expected = %{"source" => "test", "addresses" => [%{"city" => "Madrid"}], "verified" => true, "count" => 2}

    assert_replay(input, metadata, input, expected, config)
  end

  test "typed replay binds only declared outer fields and does not atomize JSON keys", %{config: config} do
    key = "external-key-#{System.unique_integer([:positive])}"
    input = %{"address" => %{key => "value"}, key => "ignored"}
    expected = %AddressEvent{address: %{key => "value"}}

    assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    assert_replay(input, nil, expected, nil, config)
    assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
  end

  test "unsupported Elixir terms fail before an event is acknowledged" do
    event = %EventData{event_type: Atom.to_string(AddressEvent), data: %AddressEvent{address: self()}}

    assert_raise Protocol.UndefinedError, fn -> Store.append_to_stream("addresses", 0, [event]) end
    assert {:error, :stream_not_found} = Store.read_stream_forward("addresses")
  end

  defp assert_replay(data, metadata, expected_data, expected_metadata, config) do
    event_type = Atom.to_string(AddressEvent)
    event = %EventData{event_type: event_type, data: data, metadata: metadata}

    assert :ok = Store.append_to_stream("addresses", 0, [event])

    assert {:ok, [%RecordedEvent{event_type: ^event_type, data: ^expected_data, metadata: ^expected_metadata}]} =
             Store.read_stream_forward("addresses")

    stop_supervised!(Store)
    start_supervised!({Store, config})

    assert {:ok, [%RecordedEvent{event_type: ^event_type, data: ^expected_data, metadata: ^expected_metadata}]} =
             Store.read_stream_forward("addresses")
  end
end
