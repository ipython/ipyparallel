
Dask-Jobqueue
=============

*Easy deployment of Dask Distributed on job queuing systems like
PBS, Slurm, and SGE.*

Motivation
----------

1. While ``dask.distributed`` offers a flexible library for distributed computing
   in Python, it is not always easy to deploy on systems that use job queuing
   systems. Dask-jobqueue provides a Pythonic interface for deploying and
   managing Dask clusters.
2. In practice, deploying distributed requires customization, both for the
   machine(s) that it will deployed on and for the specific application it will
   be deployed for. Dask-jobqueue provides users with an intuitive interface for
   customizing dask clusters.

Example
-------

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(processes=6, threads=4, memory="16GB")
   cluster.start_workers(10)

   from dask.distributed import Client
   client = Client(cluster)

See :doc:`Examples <examples>` for more real-world examples.


Adaptivity
----------

This can also adapt the cluster size dynamically based on current load.
This helps to scale up the cluster when necessary but scale it down and save
resources when not actively computing.

.. code-block:: python

   cluster.adapt(minimum=1, maximum=100)

.. toctree::
   :maxdepth: 1
   :hidden:

   install.rst
   examples.rst
   history.rst
   api.rst

How this works
--------------

This creates a Dask Scheduler in the Python process where the cluster object
is instantiated:

.. code-block:: python

   cluster = PBSCluster(processes=18,
                        threads=4,
                        memory="6GB",
                        project='P48500028',
                        queue='premium',
                        resource_spec='select=1:ncpus=36:mem=109G',
                        walltime='02:00:00')  # <-- scheduler started here

When you ask for more workers, such as with the ``scale`` command

.. code-block:: python

   cluster.scale(10)

The cluster generates a traditional job script and submits that an appropriate
number of times to the job queue.  You can see the job script that it will
generate as follows:

.. code-block:: python

   >>> print(cluster.job_script())

.. code-block:: bash

   #!/bin/bash

   #PBS -N dask-worker
   #PBS -q premium
   #PBS -A P48500028
   #PBS -l select=1:ncpus=36:mem=109G
   #PBS -l walltime=02:00:00

   /home/mrocklin/Software/anaconda/bin/dask-worker tcp://127.0.1.1:43745
   --nthreads 4 --nprocs 18 --memory-limit 6GB --name dask-worker-3
   --death-timeout 60

Each of these jobs are sent to the job queue independently and, once that job
starts, a dask-worker process will start up and connect back to the scheduler
running within this process.

If the job queue is busy then it's possible that the workers will take a while
to get through or that not all of them arrive.  In practice we find that
because dask-jobqueue submits many small jobs rather than a single large one
workers are often able to start relatively quickly.  This will depend on the
state of your cluster's job queue though.
