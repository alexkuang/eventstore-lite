defmodule EventStoreLite.Smoke.Deposit do
  @moduledoc false
  defstruct [:account_id, :amount]
end

defmodule EventStoreLite.Smoke.Deposited do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:account_id, :amount]
end

defmodule EventStoreLite.Smoke.Account do
  @moduledoc false
  alias EventStoreLite.Smoke.Deposit
  alias EventStoreLite.Smoke.Deposited

  @derive Jason.Encoder
  defstruct balance: 0
  def execute(_state, %Deposit{amount: amount}) when amount <= 0, do: {:error, :invalid_amount}
  def execute(_state, %Deposit{account_id: id, amount: amount}), do: %Deposited{account_id: id, amount: amount}
  def apply(state, %Deposited{amount: amount}), do: %{state | balance: state.balance + amount}
end

defmodule EventStoreLite.Smoke.Store do
  @moduledoc false
  use EventStoreLite, otp_app: :eventstore_lite
end

defmodule EventStoreLite.Smoke.Router do
  use Commanded.Commands.Router

  alias EventStoreLite.Smoke.Account
  alias EventStoreLite.Smoke.Deposit

  identify(Account, by: :account_id)
  dispatch(Deposit, to: Account)
end

defmodule EventStoreLite.Smoke.App do
  @moduledoc false
  use Commanded.Application,
    otp_app: :eventstore_lite,
    event_store: [adapter: EventStoreLite.Commanded.Adapter, event_store: EventStoreLite.Smoke.Store]

  router(EventStoreLite.Smoke.Router)
end

defmodule EventStoreLite.Smoke.Handler do
  @moduledoc false
  use Commanded.Event.Handler,
    application: EventStoreLite.Smoke.App,
    name: "smoke-handler",
    consistency: :strong

  def handle(event, metadata) do
    send(EventStoreLite.Smoke.Observer, {:handled, event, metadata.event_number})
    :ok
  end
end

defmodule EventStoreLite.Smoke.Run do
  @moduledoc false
  import ExUnit.Assertions

  alias Commanded.Aggregates.Aggregate
  alias EventStoreLite.Smoke.Account
  alias EventStoreLite.Smoke.App
  alias EventStoreLite.Smoke.Deposit
  alias EventStoreLite.Smoke.Deposited
  alias EventStoreLite.Smoke.Handler
  alias EventStoreLite.Smoke.Observer
  alias EventStoreLite.Smoke.Store
  alias EventStoreLite.Tasks.Init

  def run do
    path = Path.join(System.tmp_dir!(), "eventstore-lite-commanded-#{System.unique_integer([:positive])}.sqlite3")
    Process.register(self(), Observer)

    Application.put_env(:eventstore_lite, Store,
      database: path,
      serializer: Commanded.Serialization.JsonSerializer,
      subscription_retry_interval: 50
    )

    :ok = Init.exec(Store.config(), quiet: true)
    {:ok, app} = start_app()
    {:ok, handler} = Handler.start_link()

    for {amount, number} <- [{10, 1}, {20, 2}, {5, 3}] do
      :ok = App.dispatch(%Deposit{account_id: "account-1", amount: amount}, consistency: :strong)
      assert_receive {:handled, %Deposited{amount: ^amount}, ^number}, 5_000
    end

    assert {:error, :invalid_amount} = App.dispatch(%Deposit{account_id: "account-1", amount: -1})
    assert %Account{balance: 35} = Aggregate.aggregate_state(App, Account, "account-1")
    assert {:ok, snapshot} = Store.read_snapshot("account-1")
    assert snapshot.source_version >= 2
    assert {:ok, events} = Store.read_stream_forward("account-1", 0, 100)
    assert Enum.map(events, & &1.data.amount) == [10, 20, 5]
    GenServer.stop(handler)
    Supervisor.stop(app)
    :ok = Init.exec(Store.config(), quiet: true)
    {:ok, app} = start_app()
    {:ok, handler} = Handler.start_link()
    :ok = App.dispatch(%Deposit{account_id: "account-1", amount: 7}, consistency: :strong)
    assert_receive {:handled, %Deposited{amount: 7}, 4}, 5_000
    refute_received {:handled, _, _}
    assert %Account{balance: 42} = Aggregate.aggregate_state(App, Account, "account-1")
    assert {:ok, events} = Store.read_stream_forward("account-1", 0, 100)
    assert length(events) == 4

    results =
      1..8
      |> Task.async_stream(
        fn _ ->
          Store.append_to_stream("contended", 0, [%EventStoreLite.EventData{event_type: "test", data: %{value: 1}}])
        end,
        max_concurrency: 8
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.count(results, &(&1 == {:error, :wrong_expected_version})) == 7
    conn = EventStoreLite.Config.lookup(Store, :conn)

    assert {:error, :smoke_rollback} =
             EventStoreLite.SQLite.transaction(conn, fn tx ->
               :ok =
                 Store.append_to_stream(
                   "rolled-back",
                   0,
                   [%EventStoreLite.EventData{event_type: "test", data: %{value: 2}}],
                   conn: tx
                 )

               EventStoreLite.SQLite.rollback(tx, :smoke_rollback)
             end)

    assert {:error, :stream_not_found} = Store.read_stream_forward("rolled-back", 0, 100)
    GenServer.stop(handler)
    Supervisor.stop(app)
    for suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix)
    Application.delete_env(:eventstore_lite, Store)
    Process.unregister(Observer)

    IO.puts(
      "PASS: Commanded dispatch, typed events, strong handler consistency, snapshots, full restart/replay, durable handler checkpoint, concurrent stale writers, rollback"
    )
  end

  defp start_app, do: App.start_link(snapshotting: %{Account => [snapshot_every: 2]})
end

EventStoreLite.Smoke.Run.run()
