#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT_JAR="$ROOT_DIR/target/dbeaver-cloud-sql-proxy-driver.jar"

if ! command -v mvn >/dev/null 2>&1; then
  echo "Error: mvn command not found. Install Maven first." >&2
  echo "Then rerun: ./scripts/build.sh" >&2
  exit 1
fi

cd "$ROOT_DIR"

mvn -q -DskipTests clean package

echo "Built fat jar: $OUTPUT_JAR"
