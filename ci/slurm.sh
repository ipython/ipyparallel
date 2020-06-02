#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start slurm cluster
    cd ./ci/slurm
    docker-compose pull
    ./start-slurm.sh
    cd -

    docker ps -a
    docker images
    show_network_interfaces
}

function show_network_interfaces {
    for c in slurmctld c1 c2; do
        echo '------------------------------------------------------------'
        echo docker container: $c
        docker exec $c python -c 'import psutil; print(psutil.net_if_addrs().keys())'
        echo '------------------------------------------------------------'
    done
}

function jobqueue_install {
    docker exec slurmctld /bin/bash -c "cd /dask-jobqueue; pip install -e ."
}

function jobqueue_script {
    docker exec slurmctld /bin/bash -c "pytest /dask-jobqueue/dask_jobqueue --verbose -E slurm -s"
}

function jobqueue_after_script {
    docker exec slurmctld bash -c 'sinfo'
    docker exec slurmctld bash -c 'squeue'
    docker exec slurmctld bash -c 'sacct -l'
}
