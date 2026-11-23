setup:
    mix deps.get

test:
    mix test --include slow --include migration

lint:
    mix format --check-formatted
    mix credo

precommit: test lint

test-all:
    mix test --include slow --include migration
    MIX_ENV=jsonb mix test --include slow --include migration
    MIX_ENV=text_ids mix test --include slow --include migration
    MIX_ENV=migration mix test --include slow --include migration

smoke:
    MIX_ENV=test mix run test/manual/commanded_smoke.exs
