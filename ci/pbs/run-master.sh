#!/bin/bash

# Reduce time between PBS scheduling and add history
qmgr -c "set server scheduler_iteration = 20"
qmgr -c "set server job_history_enable = True"
qmgr -c "set server job_history_duration = 24:00:00"

# add two slaves to pbs
qmgr -c "create node pbs_slave_1"
qmgr -c "create node pbs_slave_2"

# Start hanging process to leave the container up and running
sleep infinity
