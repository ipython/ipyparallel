#!/bin/sh
pbs_conf_file=/etc/pbs.conf
mom_conf_file=/var/spool/pbs/mom_priv/config
hostname=$(hostname)

# replace hostname in pbs.conf and mom_priv/config
sed -i "s/PBS_SERVER=.*/PBS_SERVER=$PBS_MASTER/" $pbs_conf_file
sed -i "s/\$clienthost .*/\$clienthost $hostname/" $mom_conf_file
sed -i "s/PBS_START_SERVER=.*/PBS_START_SERVER=0/" $pbs_conf_file
sed -i "s/PBS_START_SCHED=.*/PBS_START_SCHED=0/" $pbs_conf_file
sed -i "s/PBS_START_COMM=.*/PBS_START_COMM=0/" $pbs_conf_file
sed -i "s/PBS_START_MOM=.*/PBS_START_MOM=1/" $pbs_conf_file

# Prevent PBS trying to use scp between host for stdout and stderr file of jobs
# On standard PBS deployment, you would use a shared mount, or correctly configured passwordless scp
echo "\$usecp *:/home/ /home/" >> $mom_conf_file
echo "\$usecp *:/dask-jobqueue/ /tmp/" >> $mom_conf_file

# start PBS Pro
/etc/init.d/pbs start

# create default non-root user
adduser pbsuser

exec "$@"
