#!/bin/bash

docker-compose up -d --no-build

START=$(date +%s)
MAX_WAIT_SECONDS=300

while [ `docker exec sge_master qhost | grep lx26-amd64 | wc -l` -ne 2 ]
do
    if [[ $(($(date +%s) - $START)) -gt $MAX_WAIT_SECONDS ]]; then
        echo "Exiting after failing to start the cluster in $MAX_WAIT_SECONDS seconds"
        exit 1
    fi
    echo "Waiting for SGE slots to become available";
    sleep 1
done

echo "SGE properly configured"
