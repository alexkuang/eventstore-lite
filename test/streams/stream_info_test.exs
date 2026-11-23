defmodule EventStoreLite.Streams.StreamInfoTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.Streams.StreamInfo
  alias EventStoreLite.UUID
  alias TestEventStoreLite, as: EventStoreLite

  describe "stream info" do
    test "should support global `$all` stream" do
      assert {:ok, all_stream} = EventStoreLite.stream_info(:all)

      assert match?(
               %StreamInfo{
                 stream_id: 0,
                 stream_uuid: "$all",
                 stream_version: 0,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               all_stream
             )
    end

    test "should include all created streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      assert {:ok, all_stream} = EventStoreLite.stream_info(:all)
      assert {:ok, stream1} = EventStoreLite.stream_info(stream1_uuid)
      assert {:ok, stream2} = EventStoreLite.stream_info(stream2_uuid)
      assert {:ok, stream3} = EventStoreLite.stream_info(stream3_uuid)

      assert match?(
               %StreamInfo{
                 stream_id: 0,
                 stream_uuid: "$all",
                 stream_version: 6,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               all_stream
             )

      assert match?(
               %StreamInfo{
                 stream_id: 1,
                 stream_uuid: ^stream1_uuid,
                 stream_version: 1,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               stream1
             )

      assert match?(
               %StreamInfo{
                 stream_id: 2,
                 stream_uuid: ^stream2_uuid,
                 stream_version: 2,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               stream2
             )

      assert match?(
               %StreamInfo{
                 stream_id: 3,
                 stream_uuid: ^stream3_uuid,
                 stream_version: 3,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               stream3
             )
    end

    test "should return an error for not found streams" do
      assert {:error, :stream_not_found} = EventStoreLite.stream_info("doesnotexist")
    end

    test "should return an error for soft and hard deleted streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      start_supervised!({EventStoreLite, name: :eventstore_lite, enable_hard_deletes: true})

      :ok = EventStoreLite.delete_stream(stream1_uuid, 1, :soft, name: :eventstore_lite)
      :ok = EventStoreLite.delete_stream(stream2_uuid, 2, :hard, name: :eventstore_lite)

      assert {:error, :stream_deleted} = EventStoreLite.stream_info(stream1_uuid)
      assert {:error, :stream_not_found} = EventStoreLite.stream_info(stream2_uuid)
      assert {:ok, %StreamInfo{}} = EventStoreLite.stream_info(stream3_uuid)
    end
  end

  defp append_events_to_stream(number_of_events, opts \\ []) do
    stream_uuid = Keyword.get_lazy(opts, :stream_uuid, &UUID.uuid4/0)
    events = EventFactory.create_events(number_of_events)

    :ok = EventStoreLite.append_to_stream(stream_uuid, 0, events)

    {:ok, stream_uuid}
  end
end
