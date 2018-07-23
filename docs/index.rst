Dask-Jobqueue
=============

*Easily deploy Dask on job queuing systems like PBS, Slurm, MOAB, and SGE.*


The Dask-jobqueue project makes it easy to deploy Dask on common job queuing
systems typically found in high performance supercomputers, academic research
institutions, and other clusters.  It provides a convenient interface that is
accessible from interactive systems like Jupyter notebooks, or batch jobs.

Example
-------

.. code-block:: python

   from dask_jobqueue import PBSCluster
   cluster = PBSCluster()
   cluster.scale(10)         # Ask for ten jobs

   from dask.distributed import Client
   client = Client(cluster)  # Connect this local process to remote workers

   # wait for jobs to arrive, depending on the queue, this may take some time

   import dask.array as da
   x = ...                   # Dask commands now use these distributed resources

.. raw:: html

   <iframe width="560" height="315"
           src="https://www.youtube.com/embed/FXsgmwpRExM?rel=0"
           frameborder="0" allow="autoplay; encrypted-media"
           allowfullscreen></iframe>

Adaptivity
----------

Dask jobqueue can also adapt the cluster size dynamically based on current
load.  This helps to scale up the cluster when necessary but scale it down and
save resources when not actively computing.

.. code-block:: python

   cluster.adapt(minimum=1, maximum=100)


Configuration
-------------

Dask-jobqueue should be configured for your cluster so that it knows how many
resources to request of each job and how to break up those resources.  You can
specify configuration either with keyword arguments when creating a ``Cluster``
object, or with a configuration file.

Keyword Arguments
~~~~~~~~~~~~~~~~~

You can pass keywords to the Cluster objects to define how Dask-jobqueue should
define a single job:

.. code-block:: python

   cluster = PBSCluster(
        # Dask-worker specific keywords
        cores=24,             # Number of cores per job
        memory='100GB',       # Amount of memory per job
        processes=6,          # Number of Python processes to cut up each job
        local_directory='$TMPDIR',  # Location to put temporary data if necessary
        # Job scheduler specific keywords
        resource_spec='select=1:ncpus=24:mem=100GB',
        queue='regular',
        project='my-project',
        walltime='02:00:00',
   )

Note that the ``cores`` and ``memory`` keywords above correspond not to your
full desired deployment, but rather to the size of a *single job* which should
be no larger than the size of a single machine in your cluster.  Separately you
will specify how many jobs to deploy using the scale method.

.. code-block:: python

   cluster.scale(20)  # launch twenty jobs of the specification provided above

Configuration Files
~~~~~~~~~~~~~~~~~~~

Specifying all parameters to the Cluster constructor every time can be error
prone, especially when sharing this workflow with new users.  Instead, we
recommend using a configuration file like the following:

.. code-block:: yaml

   # jobqueue.yaml file
   jobqueue:
     pbs:
       cores: 24
       memory: 100GB
       processes: 6

       interface: ib0
       local-directory: $TMPDIR

       resource-spec: "select=1:ncpus=24:mem=100GB"
       queue: regular
       project: my-project
       walltime: 00:30:00

See :doc:`Configuration Examples <configurations>` for real-world examples.

If you place this in your ``~/.config/dask/`` directory then Dask-jobqueue will
use these values by default.  You can then construct a cluster object without
keyword arguments and these parameters will be used by default.

.. code-block:: python

   cluster = PBSCluster()

You can still override configuration values with keyword arguments

.. code-block:: python

   cluster = PBSCluster(processes=12)

If you have imported ``dask_jobqueue`` then a blank ``jobqueue.yaml`` will be
added automatically to ``~/.config/dask/jobqueue.yaml``.  You should use the
section of that configuation file that corresponds to your job scheduler.
Above we used PBS, but other job schedulers operate the same way.  You should
be able to share these with colleagues.  If you can convince your IT staff
you can also place such a file in ``/etc/dask/`` and it will affect all people
on the cluster automatically.

For more information about configuring Dask, see the `Dask configuration
documentation <http://dask.pydata.org/en/latest/configuration.html>`_


.. toctree::
   :maxdepth: 1
   :hidden:

   index.rst
   install.rst
   configurations.rst
   configuration-setup.rst
   history.rst
   api.rst

How this works
--------------

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

   cluster.scale(10)

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

   /home/mrocklin/Software/anaconda/bin/dask-worker tcp://127.0.1.1:43745
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
