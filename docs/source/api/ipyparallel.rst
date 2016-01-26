ipyparallel
===========

.. module:: ipyparallel

.. autodata:: version_info

    The IPython parallel version as a tuple of integers.
    There will always be 3 integers. Development releases will have 'dev' as a fourth element.

Classes
-------

.. autoclass:: Client
    :members:

.. autoclass:: DirectView
    :members:

.. autoclass:: LoadBalancedView
    :members:

.. autoclass:: ViewExecutor
    :members:

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

