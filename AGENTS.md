# EventStoreLite

Run commands from this directory. `just setup` installs dependencies and
`just precommit` runs the complete portable suite, Styler formatting check,
and Credo. `just test-all` also checks the serializer/identifier configurations
and the standalone migration environment. `just smoke` runs real Commanded.

The upstream suite is the requirement, not a sample. Preserve portable
assertions. Record any PostgreSQL-only exclusion in `PARITY.md`, with a reason.
Keep added SQLite regressions enabled in every test configuration.

Use real SQLite transactions and constraints for persistence guarantees.
Keep notifications after commit and preserve append batch boundaries.
