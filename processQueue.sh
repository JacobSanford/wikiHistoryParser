#!/usr/bin/env bash
MAX_THREADS=20
MAX_FILES_COUNT=1300
SLEEP_TIME=120

SPAWNED_COUNT=0
while [ "$SPAWNED_COUNT" -lt "$MAX_FILES_COUNT" ]
do
  NUM_PROCESSES=$(docker ps | grep _processfile_run | wc -l)
  if [ "$NUM_PROCESSES" -lt "$MAX_THREADS" ]; then
    echo "Pruning Docker System As Tidy-Up..."
    docker system prune -f

    echo "$NUM_PROCESSES running threads found (Max: $MAX_THREADS), spawning new process [$SPAWNED_COUNT spawned total]."
    docker-compose run processfile &
    SPAWNED_COUNT=$((SPAWNED_COUNT+1))
  else
    echo "$NUM_PROCESSES threads found (Max: $MAX_THREADS), sleeping until slot available [$SPAWNED_COUNT spawned total]."
  fi
  sleep $SLEEP_TIME
done
echo "Hit maximum processed files ($MAX_FILES_COUNT), terminating."
