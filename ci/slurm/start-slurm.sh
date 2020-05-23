#!/bin/bash

docker-compose up -d

while [ `./register_cluster.sh 2>&1 | grep "sacctmgr: error" | wc -l` -ne 0 ]
  do
    echo "Waiting for SLURM cluster to become ready";
    sleep 2
  done
echo "SLURM properly configured"

# On some clusters the login node does not have the same interface as the
# compute nodes. The next three lines allow to test this edge case by adding
# separate interfaces on the worker and on the scheduler nodes.
docker exec slurmctld ip addr add 10.1.1.20/24 dev eth0 label eth0:scheduler
docker exec c1 ip addr add 10.1.1.21/24 dev eth0 label eth0:worker
docker exec c2 ip addr add 10.1.1.22/24 dev eth0 label eth0:worker
