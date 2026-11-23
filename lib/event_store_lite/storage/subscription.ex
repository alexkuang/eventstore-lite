defmodule EventStoreLite.Storage.Subscription do
  @moduledoc false

  alias EventStoreLite.Sql.Statements
  alias EventStoreLite.Storage.Subscription
  alias EventStoreLite.Storage.Subscription.CreateSubscription
  alias EventStoreLite.Storage.Subscription.QueryAllSubscriptions
  alias EventStoreLite.Storage.Subscription.QuerySubscription

  require Logger

  @type t :: %Subscription{
          subscription_id: non_neg_integer(),
          stream_uuid: String.t(),
          subscription_name: String.t(),
          last_seen: non_neg_integer() | nil,
          created_at: DateTime.t()
        }

  defstruct [:subscription_id, :stream_uuid, :subscription_name, :last_seen, :created_at]

  defdelegate subscriptions(conn, opts), to: QueryAllSubscriptions, as: :execute

  defdelegate subscription(conn, stream_uuid, subscription_name, opts),
    to: QuerySubscription,
    as: :execute

  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from, opts) do
    case QuerySubscription.execute(conn, stream_uuid, subscription_name, opts) do
      {:ok, %Subscription{}} = reply ->
        reply

      {:error, :subscription_not_found} ->
        create_subscription(conn, stream_uuid, subscription_name, start_from, opts)

      {:error, _error} = reply ->
        reply
    end
  end

  def ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen, opts) do
    Subscription.Ack.execute(conn, stream_uuid, subscription_name, last_seen, opts)
  end

  def delete_subscription(conn, stream_uuid, subscription_name, opts),
    do: Subscription.Delete.execute(conn, stream_uuid, subscription_name, opts)

  defp create_subscription(conn, stream_uuid, subscription_name, start_from, opts) do
    case CreateSubscription.execute(conn, stream_uuid, subscription_name, start_from, opts) do
      {:ok, %Subscription{}} = reply ->
        reply

      {:error, :subscription_already_exists} ->
        QuerySubscription.execute(conn, stream_uuid, subscription_name, opts)

      {:error, _error} = reply ->
        reply
    end
  end

  defmodule QueryAllSubscriptions do
    @moduledoc false

    def execute(conn, opts) do
      schema = Keyword.get(opts, :schema)

      query = Statements.query_all_subscriptions(schema)

      case EventStoreLite.SQLite.query(conn, query, [], opts) do
        {:ok, %{num_rows: 0}} -> {:ok, []}
        {:ok, %{rows: rows}} -> {:ok, Subscription.Adapter.to_subscriptions(rows)}
        {:error, _error} = reply -> reply
      end
    end
  end

  defmodule QuerySubscription do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, opts) do
      schema = Keyword.get(opts, :schema)

      query = Statements.query_subscription(schema)

      case EventStoreLite.SQLite.query(conn, query, [stream_uuid, subscription_name], opts) do
        {:ok, %{num_rows: 0}} -> {:error, :subscription_not_found}
        {:ok, %{rows: rows}} -> {:ok, Subscription.Adapter.to_subscription(rows)}
        {:error, _error} = reply -> reply
      end
    end
  end

  defmodule CreateSubscription do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, start_from, opts) do
      Logger.debug(
        "Attempting to create subscription on stream " <>
          inspect(stream_uuid) <>
          " named " <> inspect(subscription_name) <> " starting from #" <> inspect(start_from)
      )

      schema = Keyword.get(opts, :schema)

      query = Statements.insert_subscription(schema)

      case EventStoreLite.SQLite.query(conn, query, [stream_uuid, subscription_name, start_from], opts) do
        {:ok, %{rows: rows}} ->
          Logger.debug("Created subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\"")

          {:ok, Subscription.Adapter.to_subscription(rows)}

        {:error, error} when is_binary(error) ->
          if String.contains?(error, "UNIQUE"), do: {:error, :subscription_already_exists}, else: {:error, error}

        {:error, error} = reply ->
          Logger.warning(
            "Failed to create stream create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: " <>
              inspect(error)
          )

          reply
      end
    end
  end

  defmodule Ack do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, last_seen, opts) do
      schema = Keyword.get(opts, :schema)

      query = Statements.subscription_ack(schema)

      case EventStoreLite.SQLite.query(conn, query, [stream_uuid, subscription_name, last_seen], opts) do
        {:ok, _result} ->
          :ok

        {:error, error} = reply ->
          Logger.warning(
            "Failed to ack last seen event on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: " <>
              inspect(error)
          )

          reply
      end
    end
  end

  defmodule Delete do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, opts) do
      Logger.debug("Attempting to delete subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\"")

      schema = Keyword.get(opts, :schema)

      query = Statements.delete_subscription(schema)

      case EventStoreLite.SQLite.query(conn, query, [stream_uuid, subscription_name], opts) do
        {:ok, _result} ->
          Logger.debug("Deleted subscription to stream \"#{stream_uuid}\" named \"#{subscription_name}\"")

          :ok

        {:error, error} = reply ->
          Logger.warning(
            "Failed to delete subscription to stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: " <>
              inspect(error)
          )

          reply
      end
    end
  end

  defmodule Adapter do
    @moduledoc false

    def to_subscriptions(rows), do: Enum.map(rows, &to_subscription_from_row/1)

    def to_subscription([row | _]), do: to_subscription_from_row(row)

    defp to_subscription_from_row(row) do
      [
        subscription_id,
        stream_uuid,
        subscription_name,
        last_seen,
        created_at
      ] = row

      %Subscription{
        subscription_id: subscription_id,
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        last_seen: last_seen,
        created_at: EventStoreLite.SQLite.timestamp(created_at)
      }
    end
  end
end
