API Reference
=============

.. module:: ipyparallel

.. autodata:: version_info

    The IPython parallel version as a tuple of integers.
    There will always be 3 integers. Development releases will have 'dev' as a fourth element.

Classes
-------

.. autoclass:: Cluster

.. autoclass:: Client

.. autoclass:: DirectView

.. autoclass:: LoadBalancedView

.. autoclass:: BroadcastView

.. autoclass:: ViewExecutor

Decorators
----------

IPython parallel provides some decorators to assist in using your functions as tasks.

.. autofunction:: interactive
.. autofunction:: require
.. autofunction:: depend
.. autofunction:: remote
.. autofunction:: parallel


Exceptions
----------

.. autoexception:: RemoteError
.. autoexception:: CompositeError
.. autoexception:: NoEnginesRegistered
.. autoexception:: ImpossibleDependency
.. autoexception:: InvalidDependency
