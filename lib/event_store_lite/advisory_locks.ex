defmodule EventStoreLite.AdvisoryLocks do
  @moduledoc false

  use GenServer

  # TODO: Add cross-BEAM ownership before supporting unconnected nodes sharing a file.
  # Local ownership is scoped to the physical database and schema. Registration
  # arbitrates between independently started store instances in the same BEAM.
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def try_advisory_lock(server, key, timeout \\ 5_000) when is_integer(key) do
    GenServer.call(server, {:try_advisory_lock, key, self()}, timeout)
  end

  @impl true
  def init(opts) do
    conn = Keyword.fetch!(opts, :conn)
    database = Keyword.get(opts, :database, conn)

    database =
      case database do
        ":memory:" -> {:memory, conn}
        path when is_binary(path) -> Path.expand(path)
        other -> other
      end

    ref =
      case Keyword.get(opts, :monitored_server) do
        nil ->
          Process.monitor(conn)

        server ->
          {:ok, ref} = EventStoreLite.MonitoredServer.monitor(server)
          ref
      end

    {:ok,
     %{
       namespace: {database, Keyword.get(opts, :schema, "public")},
       conn_ref: ref,
       locks: %{}
     }}
  end

  @impl true
  def handle_call({:try_advisory_lock, key, owner}, _from, state) do
    name = {__MODULE__, state.namespace, key}
    existing = Enum.find(state.locks, fn {_ref, lock} -> lock.key == key end)

    holder =
      case existing do
        {_ref, %{owner: ^owner, holder: holder}} ->
          holder

        nil ->
          acquire(name)

        _ ->
          nil
      end

    if holder do
      ref = make_ref()
      monitor = Process.monitor(owner)
      lock = %{key: key, name: name, owner: owner, ref: ref, holder: holder}
      {:reply, {:ok, ref}, %{state | locks: Map.put(state.locks, monitor, lock)}}
    else
      {:reply, {:error, :lock_already_taken}, state}
    end
  end

  # :global permits only one name per process. Each lock has a linked holder,
  # so server failure also releases every registration without stale entries.
  defp acquire(name) do
    holder = spawn_link(fn -> receive do: (:release -> :ok) end)

    case :global.register_name(name, holder) do
      :yes ->
        holder

      :no ->
        send(holder, :release)
        nil
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{conn_ref: ref} = state) do
    release_locks(state.locks, reason)
    {:noreply, %{state | locks: %{}}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.locks, ref) do
      {nil, _locks} ->
        {:noreply, state}

      {lock, locks} ->
        :global.unregister_name(lock.name)
        send(lock.holder, :release)

        # Reentrant acquisitions share ownership; flush their monitors together
        # so a delayed DOWN cannot release a subsequent owner's lock.
        locks =
          Map.reject(locks, fn {monitor, acquired} ->
            if acquired.key == lock.key do
              Process.demonitor(monitor, [:flush])
              true
            else
              false
            end
          end)

        {:noreply, %{state | locks: locks}}
    end
  end

  @impl true
  def terminate(reason, state) do
    release_locks(state.locks, reason)
  end

  defp release_locks(locks, reason) do
    locks
    |> Map.values()
    |> Enum.uniq_by(& &1.name)
    |> Enum.each(fn lock ->
      :global.unregister_name(lock.name)
      send(lock.holder, :release)
    end)

    Enum.each(locks, fn {monitor, lock} ->
      Process.demonitor(monitor, [:flush])
      send(lock.owner, {__MODULE__, :lock_released, lock.ref, reason})
    end)
  end
end
