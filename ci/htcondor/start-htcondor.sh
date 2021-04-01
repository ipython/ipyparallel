#!/bin/bash

docker-compose up -d

while [ `docker-compose exec -T submit condor_status -af activity|grep Idle|wc -l` -ne 2 ]
  do
    echo "Waiting for cluster to become ready";
    sleep 2
  done
echo "HTCondor properly configured"
