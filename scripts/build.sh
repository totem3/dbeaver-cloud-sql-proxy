#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD_DIR="$ROOT_DIR/build"
CLASSES_DIR="$BUILD_DIR/classes"
SRC_DIR="$ROOT_DIR/src/main/java"
RES_DIR="$ROOT_DIR/src/main/resources"
JAR_NAME="dbeaver-cloud-sql-proxy-driver.jar"
JAR_PATH="$BUILD_DIR/$JAR_NAME"

mkdir -p "$CLASSES_DIR"

find "$SRC_DIR" -name "*.java" > "$BUILD_DIR/sources.txt"

javac -source 1.8 -target 1.8 -d "$CLASSES_DIR" @"$BUILD_DIR/sources.txt"

if [ -d "$RES_DIR" ]; then
  cp -R "$RES_DIR"/. "$CLASSES_DIR"/
fi

jar cf "$JAR_PATH" -C "$CLASSES_DIR" .

echo "Built $JAR_PATH"
