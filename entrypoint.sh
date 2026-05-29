#!/bin/bash
set -e

echo "Starting dbt pipeline"
echo "Run date: ${RUN_DATE:-$(date +%Y-%m-%d)}"
echo "Target: ${DBT_TARGET:-prod}"

# Run full dbt build
# build = seed + run + test in correct order
dbt build \
  --profiles-dir . \
  --project-dir . \
  --target ${DBT_TARGET:-prod}

echo "dbt pipeline completed successfully"