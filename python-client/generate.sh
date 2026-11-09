#!/usr/bin/env bash
# Generate Python stubs from the shared proto file.
set -euo pipefail
cd "$(dirname "$0")"
PY="${PYTHON:-.venv/bin/python}"
"$PY" -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/ping.proto
echo "Generated ping_pb2.py and ping_pb2_grpc.py"
