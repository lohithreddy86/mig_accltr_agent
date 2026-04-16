#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ANTLR_JAR="${SCRIPT_DIR}/../../antlr-4.13.2-complete.jar"
mkdir -p "$SCRIPT_DIR/generated"
java -jar "$ANTLR_JAR" -Dlanguage=Python3 -visitor -no-listener -o "$SCRIPT_DIR/generated" "$SCRIPT_DIR/PLSQLProcedure.g4"
echo "Generated parser files in $SCRIPT_DIR/generated/"
