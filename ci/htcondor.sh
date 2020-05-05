#!/usr/bin/env bash

function jobqueue_before_install {
    docker version

    docker run -d --name jobqueue-htcondor-mini htcondor/mini:el7 # might fail if called as script

    docker ps -a
    docker images
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_before_install # excute if called as script

function jobqueue_install {
    docker exec --user root jobqueue-htcondor-mini  /bin/bash -c "
    	rm -rf /dask-jobqueue
   "
    docker cp . jobqueue-htcondor-mini:/dask-jobqueue 

    docker exec --user root jobqueue-htcondor-mini /bin/bash -c "
    	python3 -c 'import psutil' 2>/dev/null || yum -y install python3-psutil; # psutil has no wheel , install gcc even slower
	cd /dask-jobqueue; 
	pip3 install -e .;
	pip3 install pytest;
	rm -f /var/log/condor/*
	chown -R submituser:submituser /dask-jobqueue 
    "
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_install # excute if called as script

function jobqueue_script {
    docker exec --user=submituser jobqueue-htcondor-mini /bin/bash -c "
    	cd /dask-jobqueue; 
	pytest dask_jobqueue --verbose -s -E htcondor"
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_script # excute if called as script

function jobqueue_after_script {
    docker exec --user root jobqueue-htcondor-mini /bin/bash -c "
    	grep -R \"\" /var/log/condor/	
   "
}
[[ "${BASH_SOURCE[0]}" != "${0}" ]] || jobqueue_after_script # excute if called as script
