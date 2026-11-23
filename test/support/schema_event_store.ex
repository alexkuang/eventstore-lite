defmodule SchemaEventStoreLite do
  @moduledoc false
  use EventStoreLite, otp_app: :eventstore_lite, schema: "example"
end
