#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start sge cluster
    cd ./ci/sge
    docker-compose pull
    ./start-sge.sh
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec sge_master /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec sge_master /bin/bash -c "cd /dask-jobqueue; pytest dask_jobqueue --verbose -s -E sge"
}

function jobqueue_after_script {
    docker exec sge_master bash -c 'cat /tmp/sge*'
    docker exec slave_one bash -c 'cat /tmp/exec*'
    docker exec slave_two bash -c 'cat /tmp/exec*'
}
