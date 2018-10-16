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
