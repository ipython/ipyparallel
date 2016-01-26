Using IPython for parallel computing
====================================

   :Release: |release|
   :Date: |today|

.. _install:

Installing IPython Parallel
---------------------------

As of 4.0, IPython parallel is now a standalone package called :mod:`ipyparallel`.
You can install it with::

    pip install ipyparallel

or::

    conda install ipyparallel

And if you want the IPython clusters tab extension in your Jupyter Notebook dashboard::

    ipcluster nbextension enable

Contents
--------

.. toctree::
   :maxdepth: 1

   changelog
   intro
   process
   multiengine
   magics
   task
   asyncresult
   mpi
   db
   security
   demos
   dag_dependencies
   details
   transition
   development/messages
   development/connections

ipyparallel API
===============

.. toctree::
    :maxdepth: 2

    api/ipyparallel.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

