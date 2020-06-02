#!/bin/bash

docker-compose up -d
while [ `docker exec -u pbsuser pbs_master pbsnodes -a | grep "Mom = pbs_slave" | wc -l` -ne 2 ]
do
    echo "Waiting for PBS slave nodes to become available";
    sleep 2
done
echo "PBS properly configured"
