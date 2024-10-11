#!/bin/bash

# ./show_log.sh container_namr

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <container_name>"
  exit 1
fi

CONTAINER_NAME=$1
LOG_FILE="${CONTAINER_NAME}.log"

docker exec -it "$CONTAINER_NAME" tail -f "$LOG_FILE"
