defmodule EventStoreLite.Sql.Statements do
  @moduledoc false

  alias EventStoreLite.Sql.Init
  alias EventStoreLite.Sql.Reset

  require EEx

  defdelegate initializers(config), to: Init, as: :statements
  defdelegate reset(config), to: Reset, as: :statements

  for {fun, args} <- [
        {:count_streams, [:_schema]},
        {:create_stream, [:_schema]},
        {:soft_delete_stream, [:_schema]},
        {:insert_subscription, [:_schema]},
        {:delete_subscription, [:_schema]},
        {:subscription_ack, [:_schema]},
        {:insert_snapshot, [:_schema]},
        {:delete_snapshot, [:_schema]},
        {:query_all_subscriptions, [:_schema]},
        {:query_snapshot, [:_schema]},
        {:query_stream_info, [:_schema]},
        {:query_stream_events_backward, [:_schema]},
        {:query_stream_events_forward, [:_schema]},
        {:query_streams, [:_schema, :sort_by, :sort_dir]},
        {:query_subscription, [:_schema]}
      ] do
    file = Path.expand("statements/#{fun}.sql.eex", __DIR__)

    @external_resource file

    EEx.function_from_file(:def, fun, file, args, engine: EventStoreLite.EExIOListEngine)
  end
end
