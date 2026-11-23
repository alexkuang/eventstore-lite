defmodule EventStoreLite.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      EventStoreLite.Config.Store,
      {Registry, keys: :duplicate, name: EventStoreLite.MonitoredServer.Registry}
    ]

    opts = [strategy: :one_for_one, name: EventStoreLite.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
