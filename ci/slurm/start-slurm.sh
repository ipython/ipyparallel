#!/bin/bash

docker-compose up --build -d
while [ `./register_cluster.sh 2>&1 | grep "sacctmgr: error" | wc -l` -ne 0 ]
  do
    echo "Waiting for SLURM cluster to become ready";
    sleep 2
  done
echo "SLURM properly configured"
