Configure Dask-Jobqueue
=======================

To properly use Dask and Dask-Jobqueue on an HPC system you need to provide a
bit of information about that system and how you plan to use it.

You provide this information either as keyword arguments to the constructor:

.. code-block:: python

   cluster = PBSCluster(cores=36, memory='100GB', queue='regular', ...)

Or as part of a configuration file:

.. code-block:: yaml

   jobqueue:
     pbs:
       cores: 36
       memory: 100GB
       queue: regular
       ...

.. code-block:: python

   cluster = PBSCluster()

For more information on handling configuration files see `Dask configuration
documentation <https://docs.dask.org/en/latest/configuration.html>`_.

This page explains what these parameters mean and how to find out information
about them.

Cores and Memory
----------------

These numbers correspond to the size of a single job, which is typically the
size of a single node on your cluster.  It does not mean the total amount of
cores or memory that you want for your full deployment.  Recall that
dask-jobqueue will launch several jobs in normal operation.

Cores should be provided as an integer, while memory is typically provided as a
string, like "100 GB".

.. code-block:: yaml

   cores: 36
   memory: 100GB

Gigabyte vs Gibibyte
~~~~~~~~~~~~~~~~~~~~

It is important to note that Dask makes the difference between 
power of 2 and power of 10 when specifying memory. This means that:
- 1GB = :math:`10^9` bytes
- 1GiB = :math:`2^30` bytes

``memory`` configuration is interpreted by Dask memory parser, and for most
JobQueueCluster implementation translated as a resource requirement for job 
submission. 
But most job schedulers (this is the case with PBS and Slurm at least) uses
KB or GB, but mean KiB or GiB. Dask jobqueue takes that into account, so you
may not find the amount of memory you were expecting when querying your job
queuing system. To give an example, with PBSCluster, if you specify '20GB' for
the ``memory`` kwarg, you will end up with a request for 19GB on PBS side. 
This is because 20GB ~= 18.6GiB, which is rounded up.

This can be avoided by always using 'GiB' in dask-jobqueue configuration.

Processes
---------

By default Dask will run one Python process per job.  However, you can
optionally choose to cut up that job into multiple processes using the
``processes`` configuration value.  This can be advantageous if your
computations are bound by the GIL, but disadvantageous if you plan to
communicate a lot between processes.  Typically we find that for pure Numpy
workloads a low number of processes (like one) is best, while for pure Python
workloads a high number of processes (like one process per two cores) is best.
If you are unsure then you might want to experiment a bit, or just choose a
moderate number, like one process per four cores.

.. code-block:: yaml

   cores: 36
   memory: 100GB
   processes: 9

Queue
-----

Many HPC systems have a variety of different queues to which you can submit
jobs.  These typically have names like "regular", "debug", and "priority".
These are set up by your cluster administrators to help direct certain jobs
based on their size and urgency.

.. code-block:: yaml

   queue: regular

If you are unfamiliar with using queues on your system you should leave this
blank, or ask your IT administrator.

Project
-------

You may have an allocation on your HPC system that is referenced by a
*project*.  This is typically a short bit of text that references your group or
a particular project.  This is typically given to you by your IT administrator
when they give you an allocation of hours on the HPC system.

.. code-block:: yaml

   project: XYZW-1234

If this sounds foreign to you or if you don't use project codes then you should
leave this blank, or ask your IT administrator.


Local Storage
-------------

When Dask workers run out of memory they typically start writing data to disk.
This is often a wise choice on personal computers or analysis clusters, but can
be unwise on HPC systems if they lack local storage.  When Dask workers try to
write excess data to disk on systems that lack local storage this can cause the
Dask process to die in unexpected ways.

If your nodes have fast locally attached storage mounted somewhere then you
should direct dask-jobqueue to use that location.

.. code-block:: yaml

   local-directory: /scratch

Sometimes your job scheduler will give this location to you as an environment
variable.  If so you should include that environment variable, prepended with
the ``$`` sign and it will be expanded appropriately after the jobs start.

.. code-block:: yaml

   local-directory: $LOCAL_STORAGE


No Local Storage
----------------

If your nodes do not have locally attached storage then we recommend that you
turn off Dask's policy to write excess data to disk.  This must be done in a
configuration file and must be separate from the ``jobqueue`` configuration
section (though it is fine to include it in the same file).

.. code-block:: yaml

   jobqueue:
     pbs:
       cores: 36
       memory: 100GB
       ...

   distributed:
     worker:
       memory:
         target: False    # Avoid spilling to disk
         spill: False     # Avoid spilling to disk
         pause: .80       # Pause worker threads at 80% use
         terminate: 0.95  # Restart workers at 95% use


Network Interface
-----------------

HPC systems often have advanced networking hardware like Infiniband.
Dask workers can take use of this network using TCP-over-Infiniband, this can
yield improved bandwidth during data transfers.  To get this increased speed
you often have to specify the network interface of your accelerated hardware.
If you have sufficient permissions then you can find a list of all network
interfaces using the ``ifconfig`` UNIX command

.. code-block:: bash

   $ ifconfig
   lo          Link encap:Local Loopback                       # Localhost
               inet addr:127.0.0.1  Mask:255.0.0.0
               inet6 addr: ::1/128 Scope:Host
   eth0        Link encap:Ethernet  HWaddr XX:XX:XX:XX:XX:XX   # Ethernet
               inet addr:192.168.0.101
               ...
   ib0         Link encap:Infiniband                           # Fast InfiniBand
               inet addr:172.42.0.101

Note: on some clusters ``ifconfig`` may need root access. You can use this python
code to list all the network interfaces instead:

.. code-block:: python

   import psutil
   psutil.net_if_addrs()


Alternatively, your IT administrators will have this information.


Managing Configuration files
----------------------------

By default when dask-jobqueue is first imported it places a file at
``~/.config/dask/jobqueue.yaml`` with a commented out version of many different
job schedulers.  You may want to do a few things to clean this up:

1.  Remove all of the commented out portions that don't apply to you.  For
    example if you use only PBS, then consider removing the entries under SGE,
    SLURM, etc..
2.  Feel free to rename the file or to include other configuration options in
    the file for other parts of Dask.  The ``jobqueue.yaml`` filename is not
    special, nor is it special that each component of Dask has its own
    configuration file.  It is ok to combine or split up configuration files as
    suits your group.
3.  Ask your IT administrator to place a generic file in ``/etc/dask`` for
    global use.  Dask will look first in ``/etc/dask`` and then in
    ``~/.config/dask`` for any ``.yaml`` files preferring those in the user's
    home directory to those in the ``/etc/dask``.  By providing a global file
    IT should be able to provide sane settings for everyone on the same system
