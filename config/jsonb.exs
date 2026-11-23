import Config

default_config = [
  column_data_type: "jsonb",
  database: "tmp/eventstore_jsonb_test.sqlite3",
  serializer: EventStoreLite.JsonbSerializer,
  subscription_retry_interval: 1_000
]

config :eventstore_lite, SchemaEventStoreLite, default_config

config :eventstore_lite,
       SecondEventStoreLite,
       Keyword.put(default_config, :database, "tmp/eventstore_jsonb_test_2.sqlite3")

config :eventstore_lite, TestEventStoreLite, default_config
config :eventstore_lite, event_stores: [TestEventStoreLite, SecondEventStoreLite, SchemaEventStoreLite]

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 100

config :logger, :default_handler, false
