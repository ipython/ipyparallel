#!/usr/bin/env bash

set -x

function jobqueue_before_install {
    docker version
    docker-compose version

    # start pbs cluster
    cd ./ci/pbs
    docker-compose up -d
    while [ `docker exec -it -u pbsuser pbs_master pbsnodes -a | grep "Mom = pbs_slave" | wc -l` -ne 2 ]
    do
        echo "Waiting for PBS slave nodes to become available";
        sleep 2
    done
    echo "PBS properly configured"
    docker exec -it -u pbsuser pbs_master pbsnodes -a
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec -it pbs_master /bin/bash -c "cd /dask-jobqueue; python setup.py install"
}

function jobqueue_script {
    docker exec -it -u pbsuser pbs_master /bin/bash -c "cd /dask-jobqueue; py.test dask_jobqueue --verbose -E pbs"
}

function jobqueue_after_script {
    docker exec -it -u pbsuser pbs_master qstat
    docker exec -it pbs_master bash -c 'cat /var/spool/pbs/sched_logs/*'
    docker exec -it pbs_slave_1 bash -c 'cat /var/spool/pbs/mom_logs/*'
    docker exec -it pbs_slave_1 bash -c 'cat /var/spool/pbs/spool/*'
    docker exec -it pbs_slave_2 bash -c 'cat /var/spool/pbs/mom_logs/*'
    docker exec -it pbs_slave_2 bash -c 'cat /var/spool/pbs/spool/*'
}
