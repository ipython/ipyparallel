#!/usr/bin/env bash

function jobqueue_before_install {
    docker version
    docker-compose version

    # start htcondor cluster
    cd ./ci/htcondor
    docker-compose pull
    ./start-htcondor.sh
    cd -

    docker ps -a
    docker images
}

function jobqueue_install {
    cd ./ci/htcondor
    docker-compose exec -T submit /bin/bash -c "cd /dask-jobqueue; pip3 install -e .;chown -R submituser ."
    cd -
}

function jobqueue_script {
    cd ./ci/htcondor
    docker-compose exec -T --user submituser submit /bin/bash -c "cd; pytest /dask-jobqueue/dask_jobqueue --verbose -E htcondor -s"
    cd -
}

function jobqueue_after_script {
    cd ./ci/htcondor
    docker-compose exec -T cm /bin/bash -c " grep -R \"\" /var/log/condor/	"
    cd -
}
