Configuration
=============

Dask-jobqueue should be configured for your cluster so that it knows how many
resources to request of each job and how to break up those resources.  You can
specify configuration either with keyword arguments when creating a ``Cluster``
object, or with a configuration file.

Keyword Arguments
-----------------

You can pass keywords to the Cluster objects to define how Dask-jobqueue should
define a single job:

.. code-block:: python

   cluster = PBSCluster(
        # Dask-worker specific keywords
        cores=24,             # Number of cores per job
        memory='100GB',       # Amount of memory per job
        shebang='#!/usr/bin/env zsh',   # Interpreter for your batch script (default is bash)
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
be no larger than the size of a single machine in your cluster.

Separately you will specify how many jobs to deploy using the scale method.
You can either specify the number of workers, or the total number of cores or
memory that you want.

.. code-block:: python

   cluster.scale(jobs=2)  # launch 2 workers, each of which starts 6 worker processes
   cluster.scale(cores=48)  # Or specify cores or memory directly
   cluster.scale(memory="200 GB")  # Or specify cores or memory directly

These all accomplish the same thing.  You can chose whichever makes the most
sense to you.


Configuration Files
-------------------

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
       shebang: "#!/usr/bin/env zsh"

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
section of that configuration file that corresponds to your job scheduler.
Above we used PBS, but other job schedulers operate the same way.  You should
be able to share these with colleagues.  If you can convince your IT staff
you can also place such a file in ``/etc/dask/`` and it will affect all people
on the cluster automatically.

For more information about configuring Dask, see the `Dask configuration
documentation <https://docs.dask.org/en/latest/configuration.html>`_
