defmodule EventStoreLite.Subscriptions.SubscriptionRecoveryTest do
  use EventStoreLite.StorageCase, async: false

  alias EventStoreLite.EventFactory
  alias EventStoreLite.RecordedEvent
  alias EventStoreLite.Subscriptions.Subscription
  alias EventStoreLite.UUID
  alias EventStoreLite.Wait
  alias TestEventStoreLite, as: EventStoreLite

  describe "subscription recovery" do
    test "should receive events after socket is closed" do
      subscription_name = UUID.uuid4()
      stream1_uuid = UUID.uuid4()

      append_to_stream(stream1_uuid, 10)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 10)

      receive_and_ack(subscription, stream1_uuid, 1)

      kill_socket()

      Process.sleep(10)

      append_to_stream(stream1_uuid, 10, 10)

      refute_receive {:events, _events}

      wait_socket()

      append_to_stream(stream1_uuid, 10, 20)

      receive_and_ack(subscription, stream1_uuid, 11)
      receive_and_ack(subscription, stream1_uuid, 21)

      refute_receive {:events, _events}
    end
  end

  # Suspend the local listener in place of closing a PostgreSQL socket. Events
  # committed during the outage must be caught up when delivery resumes.
  defp kill_socket do
    :sys.suspend(listener())
  end

  defp wait_socket do
    :sys.resume(listener())
    Wait.until(fn -> assert is_pid(Process.whereis(listener())) end)
  end

  defp listener, do: Module.concat(EventStoreLite, Elixir.EventStoreLite.Notifications.Listener)

  defp append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count)

    :ok = EventStoreLite.append_to_stream(stream_uuid, expected_version, events)
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(subscription_name, subscriber, opts) do
    {:ok, subscription} = EventStoreLite.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_initial_event_number) do
    assert_receive {:events, received_events}
    assert length(received_events) == 10

    received_events
    |> Enum.with_index(expected_initial_event_number)
    |> Enum.each(fn {event, expected_event_number} ->
      %RecordedEvent{event_number: event_number} = event

      assert event_number == expected_event_number
      assert event.stream_uuid == expected_stream_uuid
    end)

    :ok = Subscription.ack(subscription, received_events)
  end
end
