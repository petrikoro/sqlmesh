#!/usr/bin/env bash
# Applies version-specific patches and runs dbt compatibility tests.
set -euo pipefail

DBT_VERSION="${1:?Usage: $0 <DBT_VERSION>}"

# dbt < 1.6 doesn't support semantic_models / metrics YAML sections
case "$DBT_VERSION" in
  1.3|1.4|1.5)
    schema_file="tests/fixtures/dbt/sushi_test/models/schema.yml"
    if [ -f "$schema_file" ]; then
      awk '
        /^semantic_models:/ { skip=1; next }
        /^metrics:/         { skip=1; next }
        /^[^ ]/ && skip     { skip=0 }
        !skip               { print }
      ' "$schema_file" > "${schema_file}.tmp"
      mv "${schema_file}.tmp" "$schema_file"
    fi
    ;;
esac

make dbt-fast-test

# Verify sushi_dbt example works with this dbt version
cd examples/sushi_dbt
sed -i 's/target: in_memory/target: postgres/g' profiles.yml

# dbt < 1.5 doesn't support version= keyword argument
case "$DBT_VERSION" in
  1.3|1.4)
    sed -i -e 's/, version=1) }}/) }}/g' -e 's/, v=1) }}/) }}/g' models/top_waiters.sql
    ;;
esac

sqlmesh info --skip-connection
