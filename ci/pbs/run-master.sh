#!/bin/bash

# Reduce time between PBS scheduling
qmgr -c "set server scheduler_iteration = 20"
# add two slaves to pbs
qmgr -c "create node pbs_slave_1"
qmgr -c "create node pbs_slave_2"

# wait until the end of tests
/bin/sleep 3600
