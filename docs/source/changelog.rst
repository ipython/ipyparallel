.. _changelog:

Changes in IPython Parallel
===========================

5.0
---

`5.0 on GitHub <https://github.com/ipython/ipyparallel/milestones/5.0>`__

The highlight of ipyparallel 5.0 is that the Client has been reorganized a bit to use Futures.
AsyncResults are now a Future subclass, so they can be `yield`ed in coroutines, etc.
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
