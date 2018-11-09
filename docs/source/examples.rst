Example Deployments
===================

Deploying dask-jobqueue on different clusters requires a bit of customization.
Below, we provide a few examples from real deployments in the wild:

Additional examples from other cluster welcome `here <https://github.com/dask/dask-jobqueue/issues/40>`_.

PBS Deployments
---------------

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(queue='regular',
                        project='DaskOnPBS',
                        local_directory='$TMPDIR',
                        threads=4,
                        processes=6,
                        memory='16GB',
                        resource_spec='select=1:ncpus=24:mem=100GB')

   cluster = PBSCluster(processes=18,
                        threads=4,
                        shebang='#!/usr/bin/env zsh',
                        memory="6GB",
                        project='P48500028',
                        queue='premium',
                        resource_spec='select=1:ncpus=36:mem=109G',
                        walltime='02:00:00',
                        interface='ib0')

Moab Deployments
~~~~~~~~~~~~~~~~

On systems which use the Moab Workload Manager, a subclass of ``PBSCluster``
can be used, called ``MoabCluster``:

.. code-block:: python

   import os
   from dask_jobqueue import MoabCluster

   cluster = MoabCluster(processes=6,
                         threads=1,
                         project='gfdl_m',
                         memory='16G',
                         resource_spec='pmem=96G',
                         job_extra=['-d /home/First.Last', '-M none'],
                         local_directory=os.getenv('TMPDIR', '/tmp'))

SGE Deployments
---------------

On systems which use SGE as the scheduler, ```SGECluster`` can be used:

.. code-block:: python

    from dask_jobqueue import SGECluster

    cluster = SGECluster(queue='default.q',
                         walltime="1500000",
                         processes=10,
                         memory='20GB')

LSF Deployments
---------------

.. code-block:: python

    from dask_jobqueue import LSFCluster

    cluster = LSFCluster(queue='general',
                         project='cpp',
                         walltime='00:30',
                         cores=15,
                         memory='25GB')

SLURM Deployments
-----------------

.. code-block:: python

   from dask_jobqueue import SLURMCluster

   cluster = SLURMCluster(processes=4,
                          threads=2,
                          memory="16GB",
                          project="woodshole",
                          walltime="01:00:00",
                          queue="normal")



SLURM Deployment: Low-priority node usage
-----------------------------------------

.. code-block:: python


    from dask_jobqueue import SLURMCluster

    cluster = SLURMCluster(processes=6,
                           threads=4,
                           memory="16GB",
                           project="co_laika",
                           queue='savio2_bigmem',
                           env_extra=['export LANG="en_US.utf8"',
                                      'export LANGUAGE="en_US.utf8"',
                                      'export LC_ALL="en_US.utf8"'],
                           job_extra=['--qos="savio_lowprio"'])
