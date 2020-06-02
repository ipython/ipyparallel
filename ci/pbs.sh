#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start pbs cluster
    cd ./ci/pbs
    docker-compose pull
    ./start-pbs.sh
    cd -

    docker exec -u pbsuser pbs_master pbsnodes -a
    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec pbs_master /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec -u pbsuser pbs_master /bin/bash -c "cd; pytest /dask-jobqueue/dask_jobqueue --verbose -s -E pbs"
}

function jobqueue_after_script {
    docker exec -u pbsuser pbs_master qstat -fx
    docker exec pbs_master bash -c 'cat /var/spool/pbs/sched_logs/*|| true'
    docker exec pbs_master bash -c 'cat /var/spool/pbs/server_logs/*|| true'
    docker exec pbs_master bash -c 'cat /var/spool/pbs/server_priv/accounting/*|| true'
    docker exec pbs_slave_1 bash -c 'cat /var/spool/pbs/mom_logs/*|| true'
    docker exec pbs_slave_1 bash -c 'cat /var/spool/pbs/spool/*|| true'
    docker exec pbs_slave_1 bash -c 'cat /tmp/*.e*|| true'
    docker exec pbs_slave_1 bash -c 'cat /tmp/*.o*|| true'
    docker exec pbs_slave_2 bash -c 'cat /var/spool/pbs/mom_logs/*|| true'
    docker exec pbs_slave_2 bash -c 'cat /var/spool/pbs/spool/*|| true'
    docker exec pbs_slave_2 bash -c 'cat /tmp/*.e*|| true'
    docker exec pbs_slave_2 bash -c 'cat /tmp/*.o*|| true'
}
