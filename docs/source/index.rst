Dask-Jobqueue
=============

*Easily deploy Dask on job queuing systems like PBS, Slurm, MOAB, SGE, LSF, and HTCondor.*


The Dask-jobqueue project makes it easy to deploy Dask on common job queuing
systems typically found in high performance supercomputers, academic research
institutions, and other clusters.  It provides a convenient interface that is
accessible from interactive systems like Jupyter notebooks, or batch jobs.


.. _example:

Example
-------

.. code-block:: python

   from dask_jobqueue import PBSCluster
   cluster = PBSCluster()
   cluster.scale(jobs=10)    # Deploy ten single-node jobs

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

   cluster.adapt(minimum_jobs=10, maximum_jobs=100)  # auto-scale between 10 and 100 jobs
   cluster.adapt(maximum_memory="10 TB")  # or use core/memory limits

More details
------------
A good entry point to know more about how to use ``dask-jobqueue`` is
:ref:`talks-and-tutorials`.

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   install
   interactive
   talks-and-tutorials
   howitworks
   configuration

.. toctree::
   :maxdepth: 1
   :caption: Detailed use

   configuration-setup
   examples
   configurations
   advanced-tips-and-tricks
   api

.. toctree::
   :maxdepth: 1
   :caption: Help & Reference

   debug
   changelog
   develop
   history


