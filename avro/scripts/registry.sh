#!/usr/bin/env bash
# Show what Schema Registry knows about the topic's value schema.
REG=${REGISTRY:-http://localhost:8081}
SUBJECT=${1:-avro-messages-value}
echo "Subjects:"; curl -s "$REG/subjects"; echo
echo "Versions of $SUBJECT:"; curl -s "$REG/subjects/$SUBJECT/versions"; echo
for v in $(curl -s "$REG/subjects/$SUBJECT/versions" | tr -d '[]' | tr ',' ' '); do
  echo "--- version $v"
  curl -s "$REG/subjects/$SUBJECT/versions/$v" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("id:", d["id"], "| version:", d["version"]); print("schema:", d["schema"])'
done
