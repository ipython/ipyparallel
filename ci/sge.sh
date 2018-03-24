#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start sge cluster
    cd ./ci/sge
    ./start-sge.sh
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec -it sge_master /bin/bash -c "cd /dask-jobqueue; pip install --no-cache-dir ."
}

function jobqueue_script {
    docker exec -it sge_master /bin/bash -c "cd /dask-jobqueue; py.test dask_jobqueue --verbose -E sge"
}

function jobqueue_after_success {
    docker exec -it sge_master bash -c 'cat /tmp/sge*'
    docker exec -it slave_one bash -c 'cat /tmp/exec*'
    docker exec -it slave_two bash -c 'cat /tmp/exec*'
}
