API Reference
=============

.. module:: ipyparallel

.. autodata:: version_info

    The IPython parallel version as a tuple of integers.
    There will always be 3 integers. Development releases will have 'dev' as a fourth element.

Classes
-------

.. autoconfigurable:: Cluster
    :inherited-members:

.. autoclass:: Client
    :inherited-members:

.. autoclass:: DirectView
    :inherited-members:

.. autoclass:: LoadBalancedView
    :inherited-members:

.. autoclass:: BroadcastView
    :inherited-members:

.. autoclass:: AsyncResult
    :inherited-members:

.. autoclass:: ViewExecutor
    :inherited-members:

Decorators
----------

IPython parallel provides some decorators to assist in using your functions as tasks.

.. autodecorator:: interactive
.. autodecorator:: require
.. autodecorator:: depend
.. autodecorator:: remote
.. autodecorator:: parallel


Exceptions
----------

.. autoexception:: RemoteError
    :no-members:
.. autoexception:: CompositeError
    :no-members:
.. autoexception:: NoEnginesRegistered
.. autoexception:: ImpossibleDependency
.. autoexception:: InvalidDependency
