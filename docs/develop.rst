Development Guidelines
======================

This repository is part of the Dask_ projects.  General development guidelines
including where to ask for help, a layout of repositories, testing practices,
and documentation and style standards are available at the `Dask developer
guidelines`_ in the main documentation.

.. _Dask: http://dask.pydata.org/en/latest/
.. _`Dask developer guidelines`: http://dask.pydata.org/en/latest/develop.html

Install
-------

After setting up an environment as described in the `Dask developer
guidelines`_ you can clone this repository with git::

   git clone git@github.com:dask/dask-jobqueue.git

and install it from source::

   cd dask-jobqueue
   python setup.py install

Test
----

Test using ``py.test``::

   py.test dask-jobqueue --verbose

Test with Job scheduler
-----------------------

Some tests require to have a fully functional job queue cluster running, this
is done through Docker_ and `Docker compose`_ tools. You must thus have them
installed on your system following their docs.

You can then use the same commands as Travis CI does for your local testing,
for example with pbs::

   source ci/pbs.sh
   jobqueue_before_install
   jobqueue_install
   jobqueue_script

.. _Docker: https://www.docker.com/
.. _`Docker compose`: https://docs.docker.com/compose/

