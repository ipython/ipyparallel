
Dask-Jobqueue
=============

*Easy deployment of Dask Distributed on job queuing systems such as
PBS, Slurm, or SGE.*

Motivation
----------

1. While ``dask.distributed`` offers a flexible distributed parallel computing
   Python, it is not always easy to deploy on systems that use job queuing
   systems. Dask-jobqueue provides a Pythonic interface for deploying and
   managing dask clusters.
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


Adaptivity
----------

This can also adapt the cluster size dynamically based on current load.
This helps to scale up the cluster when necessary but scale it down and save
resources when not actively computing.

.. code-block:: python

   cluster.adapt()


History
-------

This package came out of the `Pangeo <https://pangeo-data.github.io/>`_
collaboration and was copy-pasted from a live repository at
`this commit <https://github.com/pangeo-data/pangeo/commit/28f86b9c836bd622daa14d5c9b48ab73bbed4c73>`_.
Unfortunately, development history was not preserved.

Original developers include the following:

-  `Jim Edwards <https://github.com/jedwards4b>`_
-  `Joe Hamman <https://github.com/jhamman>`_
-  `Matthew Rocklin <https://github.com/mrocklin>`_

**Getting Started**

* :doc:`install`
* :doc:`examples`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   install.rst
   examples.rst

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Contents:

   api.rst
