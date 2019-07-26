Configuration Examples
======================

We include configuration files for known supercomputers.
Hopefully these help both other users that use those machines and new users who
want to see examples for similar clusters.

Additional examples from other cluster welcome `here <https://github.com/dask/dask-jobqueue/issues/40>`_.

Cheyenne
--------

NCAR's `Cheyenne Supercomputer <https://www2.cisl.ucar.edu/resources/computational-systems/cheyenne>`_
uses both PBS (for Cheyenne itself) and Slurm (for the attached DAV clusters
Geyser/Caldera).

.. code-block:: yaml

   distributed:
     scheduler:
       bandwidth: 1000000000     # GB MB/s estimated worker-worker bandwidth
     worker:
       memory:
         target: 0.90  # Avoid spilling to disk
         spill: False  # Avoid spilling to disk
         pause: 0.80  # fraction at which we pause worker threads
         terminate: 0.95  # fraction at which we terminate the worker
     comm:
       compression: null

   jobqueue:
     pbs:
       name: dask-worker
       cores: 36                   # Total number of cores per job
       memory: '109 GB'            # Total amount of memory per job
       processes: 9                # Number of Python processes per job
       interface: ib0              # Network interface to use like eth0 or ib0

       queue: regular
       walltime: '00:30:00'
       resource-spec: select=1:ncpus=36:mem=109GB

     slurm:
       name: dask-worker

       # Dask worker options
       cores: 1                    # Total number of cores per job
       memory: '25 GB'             # Total amount of memory per job
       processes: 1                # Number of Python processes per job

       interface: ib0

       project: PXYZ123
       walltime: '00:30:00'
       job-extra: {-C geyser}


NERSC Cori
----------

`NERSC Cori Supercomputer <https://www.nersc.gov/systems/cori>`_

It should be noted that the the following config file assumes you are running the scheduler on a worker node. Currently the login node appears unable to talk to the worker nodes bidirectionally. As such you need to request an interactive node with the following:

.. code-block:: bash

    $ salloc -N 1 -C haswell --qos=interactive -t 04:00:00

Then you will run dask jobqueue directly on that interactive node. Note the distributed section that is set up to avoid having dask write to disk. This was due to some weird behavior with the local filesystem.

Alternatively you may use the `NERSC jupyterhub <https://jupyter.nersc.gov/>`_ which will launch a notebook server on a reserved large memory node of Cori. In this case no special interactive session is needed and dask jobqueue will perform as expected. You can also access the Dask dashboard directly. See `an example notebook <https://gist.github.com/zonca/76869ea30511cca301c13a33a9e34131>`_


.. code-block:: yaml

    distributed:
      worker:
        memory:
          target: False  # Avoid spilling to disk
          spill: False  # Avoid spilling to disk
          pause: 0.80  # fraction at which we pause worker threads
          terminate: 0.95  # fraction at which we terminate the worker

    jobqueue:
        slurm:
            cores: 64
            memory: 115GB
            processes: 4
            queue: debug
            walltime: '00:10:00'
            job-extra: ['-C haswell', '-L project, SCRATCH, cscratch1']


ARM Stratus
-----------

`Department of Energy Atmospheric Radiation Measurement (DOE-ARM) Stratus Supercomputer <https://adc.arm.gov/tutorials/cluster/stratusclusterquickstart.html>`_.

.. code-block:: yaml

    jobqueue:
      pbs:
        name: dask-worker
        cores: 36
        memory: 270GB
        processes: 6
        interface: ib0
        local-directory: $localscratch
        queue: high_mem # Can also select batch or gpu_ssd
        project: arm
        walltime: 00:30:00 #Adjust this to job size
        job-extra: ['-W group_list=cades-arm']
        
SDSC Comet
----------

San Diego Supercomputer Center's `Comet cluster <https://www.sdsc.edu/support/user_guides/comet.html>`_, available to US scientists via `XSEDE <https://www.xsede.org/>`_.
Also, note that port 8787 is open both on login and computing nodes, so you can directly access Dask's dashboard.

.. code-block:: yaml

   jobqueue:
     slurm:
       name: dask-worker

       # Dask worker options
       cores: 24                   # Total number of cores per job
       memory: 120GB               # Total amount of memory per job (total 128GB per node)
       processes: 1                # Number of Python processes per job

       interface: ib0              # Network interface to use like eth0 or ib0
       death-timeout: 60           # Number of seconds to wait if a worker can not find a scheduler
       local-directory: /scratch/$USER/$SLURM_JOB_ID # local SSD

       # SLURM resource manager options
       queue: compute
       # project: xxxxxxx # choose project other than default
       walltime: '00:30:00'
       job-mem: 120GB              # Max memory that can be requested to SLURM


Ifremer DATARMOR
----------------

See `this <https://wwz.ifremer.fr/pcdm/Equipement>`__ (French) or `this
<https://translate.google.com/translate?sl=auto&tl=en&u=https%3A%2F%2Fwwz.ifremer.fr%2Fpcdm%2FEquipement>`__
(English through Google Translate) for more details about the Ifremer DATARMOR
cluster.

See `this <https://github.com/dask/dask-jobqueue/issues/292>`__ for more details
about this ``dask-jobqueue`` config.

.. code-block:: yaml

   jobqueue:
     pbs:
       name: dask-worker

       # Dask worker options
       # number of processes and core have to be equal to avoid using multiple
       # threads in a single dask worker. Using threads can generate netcdf file
       # access errors.
       cores: 28
       processes: 28
       # this is using all the memory of a single node and corresponds to about
       # 4GB / dask worker. If you need more memory than this you have to decrease
       # cores and processes above
       memory: 120GB
       interface: ib0
       # This should be a local disk attach to your worker node and not a network
       # mounted disk. See
       # https://jobqueue.dask.org/en/latest/configuration-setup.html#local-storage
       # for more details.
       local-directory: $TMPDIR

       # PBS resource manager options
       queue: mpi_1
       project: myPROJ
       walltime: '48:00:00'
       resource-spec: select=1:ncpus=28:mem=120GB
       # disable email
       job-extra: ['-m n']
