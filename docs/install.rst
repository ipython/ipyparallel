Installing
==========

You can install dask-jobqueue with ``pip``, or by installing from source.

Pip
---

Pip can be used to install both dask-jobqueue and its dependencies (e.g. dask,
distributed,  NumPy, Pandas, and so on that are necessary for different
workloads).::

   pip install "dask_jobqueue"    # Install everything

Install from Source
-------------------

To install dask-jobqueue from source, clone the repository from `github
<https://github.com/dask/dask-jobqueue>`_::

    git clone https://github.com/dask/dask-jobqueue.git
    cd dask-jobqueue
    python setup.py install

or use ``pip`` locally if you want to install all dependencies as well::

    pip install -e .


Test
----

Test dask with ``py.test``::

    cd dask
    py.test dask_jobqueue
