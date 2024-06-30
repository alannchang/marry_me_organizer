#!/bin/bash

# ./show_log.sh container_name /path/to/log_file

# Check if two arguments are passed
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <container_name> <log_file>"
  exit 1
fi

CONTAINER_NAME=$1
LOG_FILE=$2

docker exec -it "$CONTAINER_NAME" tail -f "$LOG_FILE"
