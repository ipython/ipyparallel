#!/usr/bin/env bash

set -x

function jobqueue_before_install {
    docker version
    docker-compose version

    # start slurm cluster
    cd ./ci/slurm
    ./start-slurm.sh
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    docker exec -it slurmctld /bin/bash -c "cd /dask-jobqueue; python setup.py install"
}

function jobqueue_script {
    docker exec -it slurmctld /bin/bash -c "cd /dask-jobqueue; py.test dask_jobqueue --verbose -E slurm"
}

function jobqueue_after_script {
    docker exec -it slurmctld bash -c 'sinfo'
    docker exec -it slurmctld bash -c 'squeue'
    docker exec -it slurmctld bash -c 'sacct -l'
}
