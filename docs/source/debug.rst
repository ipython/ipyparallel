How to debug
============

Dask jobqueue has been developed and tested by several contributors, each
having a given HPC system setup to work on: a job scheduler in a given version
running on a given OS. Thus, in some specific cases, it might not work out of
the box on your system. This section provides some hints to help you determine
what may be going wrong.


Checking job script
-------------------

Dask-jobqueue submits "job scripts" to your queueing system (see
:ref:`how-this-works`). Inspecting these scripts often reveals errors in the
configuration of your Cluster object or maybe directives unexpected by your
job scheduler, in particular the header containing ``#PBS``, ``#SBATCH`` or
equivalent lines. This can be done easily once you've created a cluster object:

.. code-block:: python

   print(cluster.job_script())

If everything in job script appears correct, the next step is to try to submit
a test job using the script. You can simply copy and paste printed content to
a real job script file, and submit it using ``qsub``, ``sbatch``, ``bsub`` or
what is appropriate for you job queuing system.

To correct any problem detected at this point, you could try to use
``job_extra`` or ``env_extra`` kwargs when initializing your cluster object.

In particular, pay attention to the python executable used to launch the
workers, which by default is the one used to launch the scheduler (this makes
sense only if ``python`` is on a shared location accessible both to the Dask
scheduler and the Dask workers). You can use the ``python`` argument in
``SLURMCluster`` to specify the python executable you want to use to launch
your workers.

The typical error you might see is a ``ModuleNotFoundError``, even if you loaded
the right module just before:

.. code-block:: text

   Loading tensorflow-gpu/py3/2.1.0
     Loading requirement: cuda/10.1.2 cudnn/10.1-v7.5.1.10 nccl/2.5.6-2-cuda
       gcc/4.8.5 openmpi/4.0.2-cuda
   distributed.nanny - INFO -         Start Nanny at: 'tcp://10.148.3.252:39243'
   distributed.dashboard.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
   distributed.worker - INFO -       Start worker at:   tcp://10.148.3.252:42618
   distributed.worker - INFO -          Listening to:   tcp://10.148.3.252:42618
   distributed.worker - INFO -          dashboard at:         10.148.3.252:36903
   distributed.worker - INFO - Waiting to connect to:    tcp://10.148.0.20:35926
   distributed.worker - INFO - -------------------------------------------------
   distributed.worker - INFO -               Threads:                          1
   distributed.worker - INFO -                Memory:                   10.00 GB
   distributed.worker - INFO -       Local Directory: <local-dir>
   distributed.worker - INFO - -------------------------------------------------
   distributed.worker - INFO -         Registered to:    tcp://10.148.0.20:35926
   distributed.worker - INFO - -------------------------------------------------
   distributed.core - INFO - Starting established connection
   distributed.worker - WARNING -  Compute Failed
   Function:  train_dense_model
   args:      (None, False, 64)
   kwargs:    {}
   Exception: ModuleNotFoundError("No module named 'tensorflow'")
   
   slurmstepd: error: *** JOB 1368437 ON <node> CANCELLED AT 2020-04-10T17:14:30 ***
   distributed.worker - INFO - Connection to scheduler broken.  Reconnecting...
   distributed.worker - INFO - Stopping worker at tcp://10.148.3.252:42618
   distributed.nanny - INFO - Worker closed

This happens when you created the cluster using a different python than the one
you want to use for your workers (here ``module load python/3.7.5``), giving
the following job script (pay attention to the last line which will show which
``python`` is used):

.. code-block:: sh

   #!/usr/bin/env bash
   
   #SBATCH -J <job_name>
   #SBATCH -n 1
   #SBATCH --cpus-per-task=10
   #SBATCH --mem=10G
   #SBATCH -t 1:00:00
   #SBATCH --gres=gpu:1
   #SBATCH --qos=qos_gpu-dev
   #SBATCH --distribution=block:block
   #SBATCH --hint=nomultithread
   #SBATCH --output=%x_%j.out
   module purge
   module load tensorflow-gpu/py3/2.1.0
   /path/to/anaconda-py3/2019.10/bin/python -m distributed.cli.dask_worker tcp://10.148.0.20:44851 --nthreads 1 --memory-limit 10.00GB --name name --nanny --death-timeout 60 --interface ib0

Activate debug mode
-------------------

Dask-jobqueue uses the Python logging module. To understand better what is
happening under the hood, you may want to activate logging display. This can be
done by running this line of python code in your script or notebook:

.. code-block:: python

   import logging
   logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


Interact with your job queuing system
-------------------------------------

Every worker is launched inside a batch job, as explained above. It can be very
helpful to query your job queuing system. Some things you might want to check:

- are there running jobs related to dask-jobqueue?
- are there finished jobs, error jobs?
- what is the stdout or stderr of dask-jobqueue jobs?


Other things you might look at
------------------------------

From here it gets a little more complicated.  A couple of other already seen
problems are the following:

- The submit command used in dask-jobqueue (``qsub`` or equivalent) doesn't
  correspond to the one that you use. Check in the given ``JobQueueCluster``
  implementation that job submission command and arguments look familiar to
  you, eventually try them.

- The submit command output is not the same as the one expected by dask-jobqueue.
  We use submit command stdout to parse the job_id corresponding to the
  launched group of worker. If the parsing fails, then dask-jobqueue won't work
  as expected and may throw exceptions. You can have a look at the parsing
  function ``JobQueueCluster._job_id_from_submit_output``.
