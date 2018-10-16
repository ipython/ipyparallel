.. _how-this-works:

How this works
==============

Scheduler and jobs
------------------

Dask-jobqueue creates a Dask Scheduler in the Python process where the cluster
object is instantiated:

.. code-block:: python

   cluster = PBSCluster(  # <-- scheduler started here
        cores=24,
        memory='100GB',
        processes=6,
        local_directory='$TMPDIR',
        resource_spec='select=1:ncpus=24:mem=100GB',
        queue='regular',
        project='my-project',
        walltime='02:00:00',
   )

You then ask for more workers using the ``scale`` command:

.. code-block:: python

   cluster.scale(36)

The cluster generates a traditional job script and submits that an appropriate
number of times to the job queue.  You can see the job script that it will
generate as follows:

.. code-block:: python

   >>> print(cluster.job_script())

.. code-block:: bash

   #!/bin/bash

   #PBS -N dask-worker
   #PBS -q regular
   #PBS -A P48500028
   #PBS -l select=1:ncpus=24:mem=100G
   #PBS -l walltime=02:00:00

   /home/username/path/to/bin/dask-worker tcp://127.0.1.1:43745
   --nthreads 4 --nprocs 6 --memory-limit 18.66GB --name dask-worker-3
   --death-timeout 60

Each of these jobs are sent to the job queue independently and, once that job
starts, a dask-worker process will start up and connect back to the scheduler
running within this process.

If the job queue is busy then it's possible that the workers will take a while
to get through or that not all of them arrive.  In practice we find that
because dask-jobqueue submits many small jobs rather than a single large one
workers are often able to start relatively quickly.  This will depend on the
state of your cluster's job queue though.

When the cluster object goes away, either because you delete it or because you
close your Python program, it will send a signal to the workers to shut down.
If for some reason this signal does not get through then workers will kill
themselves after 60 seconds of waiting for a non-existent scheduler.

Workers vs Jobs
---------------

In dask-distributed, a ``Worker`` is a Python object and node in a dask
``Cluster`` that serves two purposes, 1) serve data, and 2) perform
computations. ``Jobs`` are resources submitted to, and managed by, the job
queueing system (e.g. PBS, SGE, etc.). In dask-jobqueue, a single ``Job`` may
include one or more ``Workers``.
