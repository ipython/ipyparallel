#!/bin/bash
set -ex
# set permissions on munge dir, may be mounted
chown -R munge:munge /etc/munge

echo "starting munge"
service munge start

echo "hostname=$(hostname)"
if [[ "$(hostname)" == *"slurmctl"* ]]; then
    echo "starting slurmctld"
    service slurmctld start
else
    echo "starting slurmd"
    service slurmd start
fi

exec "$@"
