Example Deployments
===================

Deploying dask-jobqueue on different clusters requires a bit of customization.
Below, we provide a few examples from real deployments in the wild:

Additional examples from other cluster welcome `here <https://github.com/dask/dask-jobqueue/issues/40>`__.

PBS Deployments
---------------

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(queue='regular',
                        project='DaskOnPBS',
                        local_directory='$TMPDIR',
                        cores=24,
                        processes=6,
                        memory='16GB',
                        resource_spec='select=1:ncpus=24:mem=100GB')

   cluster = PBSCluster(cores=24,
                        processes=6,
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

   cluster = MoabCluster(cores=6,
                         processes=6,
                         project='gfdl_m',
                         memory='16G',
                         resource_spec='pmem=96G',
                         job_extra=['-d /home/First.Last', '-M none'],
                         local_directory=os.getenv('TMPDIR', '/tmp'))

SGE Deployments
---------------

On systems which use SGE as the scheduler, ``SGECluster`` can be used. Note
that Grid Engine has a slightly involved `history
<https://en.wikipedia.org/wiki/Univa_Grid_Engine#History>`_ , so there are a
variety of Grid Engine derivatives. ``SGECluster`` can be used for any
derivative of Grid Engine, for example: SGE (Son of Grid Engine), Oracle Grid Engine,
Univa Grid Engine.

Because the variety of Grid Engine derivatives and configuration deployments,
it is not possible to use the ``memory`` keyword argument to automatically
specify the amount of RAM requested. Instead, you specify the resources desired
according to how your system is configured, using the ``resource_spec`` keyword
argument, in addition to the ``memory`` keyword argument (which is used by Dask
internally for memory management, see `this
<http://distributed.dask.org/en/latest/worker.html#memory-management>`_ for
more details).

In the example below, our system administrator has used the ``m_mem_free``
keyword argument to let us request for RAM. Other known keywords may include
``mem_req`` and ``mem_free``. We had to check with our cluster documentation
and/or system administrator for this. At the same time, we must also correctly
specify the ``memory`` keyword argument, to enable Dask's memory management to
do its work correctly.

.. code-block:: python

    from dask_jobqueue import SGECluster

    cluster = SGECluster(queue='default.q',
                         walltime="1500000",
                         processes=10,   # we request 10 processes per worker
                         memory='20GB',  # for memory requests, this must be specified
                         resource_spec='m_mem_free=20G',  # for memory requests, this also needs to be specified
                         )

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

   cluster = SLURMCluster(cores=8,
                          processes=4,
                          memory="16GB",
                          project="woodshole",
                          walltime="01:00:00",
                          queue="normal")



SLURM Deployment: Low-priority node usage
-----------------------------------------

.. code-block:: python


    from dask_jobqueue import SLURMCluster

    cluster = SLURMCluster(cores=24,
                           processes=6,
                           memory="16GB",
                           project="co_laika",
                           queue='savio2_bigmem',
                           env_extra=['export LANG="en_US.utf8"',
                                      'export LANGUAGE="en_US.utf8"',
                                      'export LC_ALL="en_US.utf8"'],
                           job_extra=['--qos="savio_lowprio"'])



SLURM Deployment: Providing additional arguments to the dask-workers
--------------------------------------------------------------------

Keyword arguments can be passed through to dask-workers. An example of such an
argument is for the specification of abstract resources, described `here
<http://distributed.dask.org/en/latest/resources.html>`__. This could be used
to specify special hardware availability that the scheduler is not aware of,
for example GPUs. Below, the arbitrary resources "ssdGB" and "GPU" are
specified. Notice that the ``extra`` keyword is used to pass through arguments
to the dask-workers.

.. code-block:: python

    from dask_jobqueue import SLURMCluster
    from distributed import Client
    from dask import delayed

    cluster = SLURMCluster(memory='8g',
                           processes=1,
                           cores=2,
                           extra=['--resources ssdGB=200,GPU=2'])

    cluster.start_workers(2)
    client = Client(cluster)

The client can then be used as normal. Additionally, required resources can be
specified for certain steps in the processing. For example:

.. code-block:: python

    def step_1_w_single_GPU(data):
        return "Step 1 done for: %s" % data


    def step_2_w_local_IO(data):
        return "Step 2 done for: %s" % data


    stage_1 = [delayed(step_1_w_single_GPU)(i) for i in range(10)]
    stage_2 = [delayed(step_2_w_local_IO)(s2) for s2 in stage_1]

    result_stage_2 = client.compute(stage_2,
                                    resources={tuple(stage_1): {'GPU': 1},
                                               tuple(stage_2): {'ssdGB': 100}})
