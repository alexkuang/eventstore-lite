# EventStoreLite

sqlite impl of [eventstore](https://github.com/commanded/eventstore/) for
[commanded](https://github.com/commanded/commanded/).  NB: 100% slop fork, not suitable for production.  This is
published for reference and use in prototypes + personal projects.

Includes streams, optimistic concurrency, links, forward/backward reads, pagination, snapshots, transient subscriptions,
and persistent subscriptions with checkpoints, filtering, partitioning, concurrency, and backpressure.

## Use

Add the Hex package and Jason (for the JSON serializers) to your Mix dependencies:

```elixir
{:eventstore_lite, "~> 0.1.0"},
{:jason, "~> 1.4"}
```

Define a store and start it under your supervisor:

```elixir
defmodule MyApp.Store do
  use EventStoreLite, otp_app: :my_app
end
```

```elixir
config :my_app, MyApp.Store,
  database: "events.sqlite3",
  serializer: EventStoreLite.JsonSerializer
```

```elixir
children = [MyApp.Store]

:ok = MyApp.Store.append_to_stream("account-1", 0, [
  %EventStoreLite.EventData{event_type: "Opened", data: %{balance: 100}}
])
{:ok, events} = MyApp.Store.read_stream_forward("account-1")
```

`:schema` selects a case-sensitive table namespace within a SQLite file: `Acme`, `acme`, and `ACME` are separate stores.
Use separate files when independent backup or deletion is required.

`JsonbSerializer` uses Jason to normalize payloads through JSON before storage. Typed replay reconstructs the outer
event struct; nested objects keep string keys, including objects inside arrays. Nested structs need `Jason.Encoder` and
replay as JSON objects. Use `TermSerializer` to preserve arbitrary Elixir terms.

## Commanded

Add Commanded to your application dependencies, then configure its application:

```elixir
use Commanded.Application,
  otp_app: :my_app,
  event_store: [
    adapter: EventStoreLite.Commanded.Adapter,
    event_store: MyApp.Store
  ]
```

Configure `MyApp.Store` with `serializer: Commanded.Serialization.JsonSerializer`. The adapter starts the store; do not
also start it independently.

## Storage and deployment

Writes use SQLite WAL and serialized `BEGIN IMMEDIATE` transactions. Separate connections still contend through SQLite,
and expected-version checks run in the write transaction. Notifications read a durable outbox after commit, keeping
append batch boundaries and observing commits from other connections.

Use one BEAM node per database file: subscription ownership uses BEAM process registration. Keep the database on a local
filesystem that supports SQLite WAL.

The database file and its WAL belong together while the store is running. Stop the store before copying files, or use
`EventStoreLite.Storage.Database.dump/2` for a consistent SQLite backup. Protect the file as application data:
serialized payloads can contain Erlang terms and must not come from an untrusted database.

Restore with `EventStoreLite.Storage.Database.restore(config, backup_path)` only when all connections to the destination
are closed. Use a separate, standalone SQLite backup, such as the file produced by `dump/2`. If the backup cannot be
copied or fails SQLite validation, the existing database and WAL are unchanged. If a restore is interrupted, keep the
store stopped and retain any `.eventstore-lite-restore-*` directory beside the database. It can contain the original
WAL, which must only be paired with the original database.

Hard deletion requires `enable_hard_deletes: true` and removes linked events too.

## License

Apache-2.0. See [LICENSE](LICENSE).

## Development

From this directory:

```sh
just setup
just precommit
just test-all
just smoke
```

`just precommit` runs the standard suite and lint checks. The test configurations use disposable files under `tmp/`. See
[PARITY.md](PARITY.md) for port behavior and PostgreSQL-only skips, and [THIRD_PARTY.md](THIRD_PARTY.md) for
references. [MANUAL_TESTING.md](MANUAL_TESTING.md) records the Commanded integration exercise.
