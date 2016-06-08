.. _changelog:

Changes in IPython Parallel
===========================


5.1
---

dask, joblib
~~~~~~~~~~~~

IPython Parallel 5.1 adds integration with other parallel computing tools,
such as `dask.distributed <https://distributed.readthedocs.io>`_ and `joblib <https://pythonhosted.org/joblib>`__.

To turn an IPython cluster into a dask.distributed cluster,
call :meth:`~.Client.become_distributed`::

    executor = client.become_distributed(ncores=1)

which returns a distributed :class:`Executor` instance.

To register IPython Parallel as the backend for joblib::

    import ipyparallel as ipp
    ipp.register_joblib_backend()


nbextensions
~~~~~~~~~~~~

IPython parallel now supports the notebook-4.2 API for enabling server extensions,
to provide the IPython clusters tab::

    jupyter serverextension enable --py ipyparallel
    jupyter nbextension install --py ipyparallel
    jupyter nbextension enable --py ipyparallel

though you can still use the more convenient single-call::

    ipcluster nbextension enable

which does all three steps above.

Slurm support
~~~~~~~~~~~~~

`Slurm <https://computing.llnl.gov/tutorials/linux_clusters>`_ support is added to ipcluster.

5.1.0
~~~~~

`5.1.0 on GitHub <https://github.com/ipython/ipyparallel/milestones/5.1>`__

5.0
---

5.0.1
~~~~~

`5.0.1 on GitHub <https://github.com/ipython/ipyparallel/milestones/5.0.1>`__

- Fix imports in :meth:`use_cloudpickle`, :meth:`use_dill`.
- Various typos and documentation updates to catch up with 5.0.


5.0.0
~~~~~

`5.0 on GitHub <https://github.com/ipython/ipyparallel/milestones/5.0>`__

The highlight of ipyparallel 5.0 is that the Client has been reorganized a bit to use Futures.
AsyncResults are now a Future subclass, so they can be `yield` ed in coroutines, etc.
Views have also received an Executor interface.
This rewrite better connects results to their handles,
so the Client.results cache should no longer grow unbounded.

.. seealso::

    - The Executor API :class:`ipyparallel.ViewExecutor`
    - Creating an Executor from a Client: :meth:`ipyparallel.Client.executor`
    - Each View has an :attr:`executor` attribute


Part of the Future refactor is that Client IO is now handled in a background thread,
which means that :meth:`Client.spin_thread` is obsolete and deprecated.

Other changes:

- Add :command:`ipcluster nbextension enable|disable` to toggle the clusters tab in Jupyter notebook


Less interesting development changes for users:

Some IPython-parallel extensions to the IPython kernel have been moved to the ipyparallel package:

- :mod:`ipykernel.datapub` is now :mod:`ipyparallel.datapub`
- ipykernel Python serialization is now in :mod:`ipyparallel.serialize`
- apply_request message handling is implememented in a Kernel subclass,
  rather than the base ipykernel Kernel.

4.1
---

`4.1 on GitHub <https://github.com/ipython/ipyparallel/milestones/4.1>`__

- Add :meth:`.Client.wait_interactive`
- Improvements for specifying engines with SSH launcher.

4.0
---

`4.0 on GitHub <https://github.com/ipython/ipyparallel/milestones/4.0>`__

First release of ``ipyparallel`` as a standalone package.
