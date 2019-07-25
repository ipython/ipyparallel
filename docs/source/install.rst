Installing
==========

You can install dask-jobqueue with ``pip``, ``conda``, or by installing from source.

Pip
---

Pip can be used to install both dask-jobqueue and its dependencies (e.g. dask,
distributed,  numpy, pandas, etc., that are necessary for different
workloads).::

   pip install dask-jobqueue --upgrade   # Install everything from last released version

Conda
-----

To install the latest version of dask-jobqueue from the
`conda-forge <https://conda-forge.github.io/>`_ repository using
`conda <https://www.anaconda.com/downloads>`_::

    conda install dask-jobqueue -c conda-forge

Install from Source
-------------------

To install dask-jobqueue from source, clone the repository from `github
<https://github.com/dask/dask-jobqueue>`_::

    git clone https://github.com/dask/dask-jobqueue.git
    cd dask-jobqueue
    python setup.py install

or use ``pip`` locally if you want to install all dependencies as well::

    pip install -e .

You can also install directly from git master branch::

    pip install git+https://github.com/dask/dask-jobqueue


Test
----

Test dask-jobqueue with ``pytest``::

    git clone https://github.com/dask/dask-jobqueue.git
    cd dask-jobqueue
    pytest dask_jobqueue
