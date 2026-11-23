# Upstream test parity

Source: [Commanded EventStore 1.4.10](https://github.com/commanded/eventstore/tree/49ba9f971bd8be8b9bd77702f9eab4013d3fbbdd).
The portable upstream suite runs against real SQLite. The 15 cases below remain
in the suite with `:postgres_only` and explicit skip reasons.

## Adaptations

- SQL queries return maps with `rows` and `num_rows`. Transactions use the SQLite
  adapter; caller rollback must undo event appends and links.
- Notification tests observe committed event batches through PubSub. Reconnect
  tests restart the listener or SQLite connection.
- Reopening tests use an API-generated fixture with events, links, snapshots,
  and subscription checkpoints. Schema initialization is idempotent.
- `JsonbSerializer` reconstructs only the outer event struct. Nested JSON objects
  keep string keys, unlike upstream's recursive atom conversion in plain maps.

## PostgreSQL-only exclusions

### Connection configuration

These `config_test.exs` cases require PostgreSQL connection settings or
`pg_stat_activity`:

- `default connections do not have application name setup`
- `parse database url from environment variable`
- `parse session_mode_url`
- `parse socket`
- `parse socket_dir`
- `parse url`
- `parse url with encoded hash in password`
- `parse url with query parameters`
- `passes parent config options to session mode pool config`
- `successfully connects with the application name setup`

### Connection pool counts

`shared_connection_pool_test.exs` — `ensure database connections are shared between instances`
requires `pg_stat_activity` backend counts. SQLite sharing and ownership tests
remain enabled.

### Schema upgrades

These cases require historical PostgreSQL archives, migration locks, or
`pg_dump` schema comparisons:

- `migrate_event_store_test.exs` — `should be identical to new event store schema`
- `migrate_event_store_test.exs` — `should prevent migrations from running concurrently`

These `list_event_store_migrations_test.exs` cases require historical pending
and completed version records:

- `exits with status 1 when running via mix`
- `should list completed and pending migrations`

## Verification

Run `just precommit`, `just test-all`, and `just smoke`. All other cases run by
default. The matrix covers the default, JSONB, text-ID, and standalone migration
configurations. See [MANUAL_TESTING.md](MANUAL_TESTING.md) for the smoke scenarios.
