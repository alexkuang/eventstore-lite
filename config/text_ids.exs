import Config

default_config = [
  correlation_id_type: "text",
  causation_id_type: "text",
  database: "tmp/eventstore_text_ids_test.sqlite3",
  serializer: EventStoreLite.JsonSerializer,
  subscription_retry_interval: 1_000
]

config :eventstore_lite, SchemaEventStoreLite, default_config

config :eventstore_lite,
       SecondEventStoreLite,
       Keyword.put(default_config, :database, "tmp/eventstore_text_ids_test_2.sqlite3")

config :eventstore_lite, TestEventStoreLite, default_config
config :eventstore_lite, event_stores: [TestEventStoreLite, SecondEventStoreLite, SchemaEventStoreLite]

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 100

config :logger, :default_handler, false
