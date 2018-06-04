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

As of IPython Parallel 6.2, this will additionally install and enable the IPython Clusters tab
in the Jupyter Notebook dashboard.

You can also enable/install the clusters tab yourself after the fact with:

    ipcluster nbextension enable

Or the individual Jupyter commands:

    jupyter serverextension install [--sys-prefix] --py ipyparallel
    jupyter nbextension install [--sys-prefix] --py ipyparallel
    jupyter nbextension enable [--sys-prefix] --py ipyparallel


Contents
--------

.. toctree::
   :maxdepth: 1

   changelog
   intro
   process
   direct
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

