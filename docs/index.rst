Dask-Jobqueue
=============

*Easily deploy Dask on job queuing systems like PBS, Slurm, MOAB, SGE, and LSF.*


The Dask-jobqueue project makes it easy to deploy Dask on common job queuing
systems typically found in high performance supercomputers, academic research
institutions, and other clusters.  It provides a convenient interface that is
accessible from interactive systems like Jupyter notebooks, or batch jobs.

Example
-------

.. code-block:: python

   from dask_jobqueue import PBSCluster
   cluster = PBSCluster()
   cluster.scale(10)         # Ask for ten workers

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

   cluster.adapt(minimum=6, maximum=90)  # auto-scale between 6 and 90 workers


Interactive Use
---------------

While dask-jobqueue can perfectly be used to submit batch processing, it is
better suited to interactive processing, using tools like ipython or jupyter
notebooks. Batch processing with dask-jobqueue can be tricky in some cases
depending on how your cluster is configured and which resources and queues you
have access to: scheduler might hang on for a long time before having some
connected workers, and you could end up with less computing power than you
expected. Another good solution for batch processing on HPC system using dask
is the `dask-mpi <http://dask.pydata.org/en/latest/setup/hpc.html#using-mpi>`_
command.

The following paragraphs describe how to have access to Jupyter notebook and
Dask dashboard on your HPC system.

Using Jupyter
~~~~~~~~~~~~~

It is convenient to run a Jupyter notebook server on the HPC for use with
dask-jobqueue. You may already have a Jupyterhub instance available on your
system, which can be used as is. Otherwise, a really good documentation for
starting your own notebook is available in the `Pangeo documentation
<http://pangeo-data.org/setup_guides/hpc.html#configure-jupyter>`_.

Once Jupyter is installed and configured, using a Jupyter notebook is done by:

- Starting a Jupyter notebook server on the HPC (it is often good practice to
  run/submit this as a job to an interactive queue, see Pangeo docs for more
  details).

.. code-block:: bash

   $ jupyter notebook --no-browser --ip=`hostname` --port=8888

- Reading the output of the command above to get the ip or hostname of your
  notebook, and use SSH tunneling on your local machine to access the notebook.
  This must only be done in the probable case where you don't have direct
  access to the notebook URL from your computer browser.

.. code-block:: bash

   $ ssh -N -L 8888:x.x.x.x:8888 username@hpc_domain

Viewing the Dask Dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~~

Whether or not you are using dask-jobqueue in Jupyter, ipython or other tools,
at one point you will want to have access to Dask Dashboard. Once you've
started a cluster and connected a client to it using commands described in
`Example`_), inspecting ``client`` object will give you the Dashboard URL,
for example ``http://172.16.23.102:8787/status``. The Dask Dashboard may be
accessible by clicking the link displayed, otherwise, you'll have to use SSH
tunneling:

.. code-block:: bash

    # General syntax
    $ ssh -fN your-login@scheduler-ip-address -L port-number:localhost:port-number
    # As applied to this example:
    $ ssh -fN username@172.16.23.102 -L 8787:localhost:8787

Now, you can go to ``http://localhost:8787`` on your browser to view the
dashboard. Note that you can do SSH tunneling for both Jupyter and Dashboard in
one command.

A good example of using Jupyter along with dask-jobqueue and the Dashboard is
availaible below:

.. raw:: html

   <iframe width="560" height="315"
           src="https://www.youtube.com/embed/nH_AQo8WdKw?rel=0"
           frameborder="0" allow="autoplay; encrypted-media"
           allowfullscreen></iframe>

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

   cluster.scale(12)  # launch 12 workers (2 jobs of 6 workers each) of the specification provided above

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
   examples.rst
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

Workers vs Jobs
---------------

In dask-distributed, a ``Worker`` is a Python object and node in a dask
``Cluster`` that serves two purposes, 1) serve data, and 2) perform
computations. ``Jobs`` are resources submitted to, and managed by, the job
queueing system (e.g. PBS, SGE, etc.). In dask-jobqueue, a single ``Job`` may
include one or more ``Workers``.

How to debug
------------

Dask jobqueue has been developed and tested by several contributors, each of
us having a given HPC system setup to work on: a job scheduler in a given
version running on a given OS. Thus, in some specific case, it might not work
out of the box on your system. This section provides some hints to help you
sort what may be going wrong.

Checking job script
~~~~~~~~~~~~~~~~~~~

Dask-jobqueue submits "job scripts" to your queueing system (see `How this
works`_). Inspecting these scripts often reveals errors in the configuration
of your Cluster object or maybe directives unexpected by your job scheduler,
in particular the header containing ``#PBS``, ``#SBATCH`` or equivalent lines.
This can be done easily once you've created a cluster object:

.. code-block:: python

   print(cluster.job_script())

If everything in job script appears correct, the next step is to try to submit
a test job using the script. You can simply copy and paste printed content to
a real job script file, and submit it using ``qsub``, ``sbatch``, ``bsub`` or
what is appropriate for you job queuing system.

To correct any problem detected at this point, you could try to use
``job_extra`` or ``env_extra`` kwargs when initializing your cluster object.

Activate debug mode
~~~~~~~~~~~~~~~~~~~

Dask-jobqueue uses python logging module. To understand better what is
happening under the hood, you may want to activate logging display. This can be
done by running this line of python code in your script or notebook:

.. code-block:: python

   import logging
   logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


Interact with you job queuing system
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every worker is launched inside a batch job, as explained above. It can be very
helpful to query your job queuing system. Some things you might want to check:

- are there running jobs related to dask-jobqueue?
- are there finished jobs, error jobs?
- what is the stdout or stderr of dask-jobqueue jobs?

Other things you might look at
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

From here it gets a little more complicated. A couple of other already seen
problems are the following:

- submit command used in dask-jobqueue (``qsub`` or equivalent) doesn't
  correspond to the one you use. Check in the given ``JobQueueCluster``
  implementation that job submission command and eventual arguments look
  familliar to you, eventually try them.

- submit command output is not the same as the one expected by dask-jobqueue.
  We use submit command stdout to parse the job_id corresponding to the
  launched group of worker. If the parsing fails, then dask-jobqueue won't work
  as expected and may throw exceptions. You can have a look at the parsing
  function ``JobQueueCluster._job_id_from_submit_output``.
