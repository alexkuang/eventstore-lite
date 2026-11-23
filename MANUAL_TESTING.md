# Commanded integration check

Run `just smoke` from this directory. It uses the shipped adapter and a real
temporary SQLite file, not Commanded's in-memory event store.

The script checks:

- Command dispatch and typed event order: deposits of 10, 20, and 5 produce a
  balance of 35 and three events.
- Rejection: a negative deposit returns `:invalid_amount` without an event.
- Strong consistency: dispatch waits for the handler to process each event.
- Snapshots and restart: after the application restarts, a deposit of 7 produces
  a balance of 42 and event number 4.
- Durable handler checkpoints: the restarted handler receives event 4 without
  replaying the first three acknowledged events.
- Optimistic concurrency: one of eight stale writers succeeds; the others return
  `:wrong_expected_version`.
- Explicit transaction rollback: the appended stream remains absent.
