(parallel-mpi)=

# Using MPI with IPython

Often, a parallel algorithm will require moving data between the engines. One
way of accomplishing this is by doing a pull and then a push using the
direct view. However, this will be slow as all the data has to go
through the controller to the client and then back through the controller, to
its final destination.

A much better way of moving data between engines is to use a message passing
library, such as the Message Passing Interface ([MPI][]). IPython's
parallel computing architecture has been designed from the ground up to
integrate with MPI. This document describes how to use MPI with IPython.

## Additional installation requirements

If you want to use MPI with IPython, you will need to install:

- A standard MPI implementation such as [OpenMPI][] or MPICH.
- The [mpi4py][] package.

```{note}
The mpi4py package is not a strict requirement. However, you need to
have _some_ way of calling MPI from Python. You also need some way of
making sure that {func}`MPI_Init` is called when the IPython engines start
up. There are a number of ways of doing this and a good number of
associated subtleties. We highly recommend using mpi4py as it
takes care of most of these problems. If you want to do something
different, let us know and we can help you get started.
```

## Starting the engines with MPI enabled

To use code that calls MPI, there are typically two things that MPI requires.

1. The process that wants to call MPI must be started using
   {command}`mpiexec` or a batch system (like PBS) that has MPI support.
2. Once the process starts, it must call {func}`MPI_Init`.

There are a couple of ways that you can start the IPython engines and get
these things to happen.

### Automatic starting using {command}`mpiexec` and {command}`ipcluster`

The easiest approach is to use the `MPI` Launcher,
which will first start a controller and then a set of engines using
{command}`mpiexec`:

```python
cluster = ipp.Cluster(engines="mpi")
cluster.start_cluster_sync()
```

or on the command-line

```
$ ipcluster start -n 4 --engines=mpi
```

### Automatic starting using batch systems such as PBS or Slurm

IPython Parallel also has launchers for several batch systems,
including PBS, Slurm, SGE, LSF, HTCondor.
Just like `mpi`, you can specify these as the controller

```python
cluster = ipp.Cluster(engines="slurm", controller="slurm")
```

:::{versionadded} 8.0

The `controller` and `engines` arguments are new in IPython Parallel 8.0.
In 7.x, these arguments had to be called `controller_launcher_class`
and `engine_launcher_class`, respectively.
:::

## Actually using MPI

Once the engines are running with MPI enabled, you are ready to go. You can
now call any code that uses MPI in the IPython engines. And, all of this can
be done interactively. Here we show a simple example that uses
[mpi4py][] version 1.1.0 or later.

First, lets define a function that uses MPI to calculate the sum of a
distributed array. Save the following text in a file called {file}`psum.py`:

```python
from mpi4py import MPI
import numpy as np

def psum(a):
    locsum = np.sum(a)
    rcvBuf = np.array(0.0, 'd')
    MPI.COMM_WORLD.Allreduce([locsum, MPI.DOUBLE],
        [rcvBuf, MPI.DOUBLE],
        op=MPI.SUM)
    return rcvBuf
```

Now, we can start an IPython cluster and use this function interactively.
In this case,
we create a distributed array and sum up all its elements in a distributed manner
using our {func}`psum` function:

```ipython
In [1]: import ipyparallel as ipp

In [2]: cluster = ipp.Cluster(engines="mpi", n=4)

In [3]: rc = cluster.start_and_connect_sync()

In [4]: view = rc[:]

In [5]: view.activate() # enable magics

# run the contents of the file on each engine:
In [6]: view.run('psum.py')

In [6]: view.scatter('a', np.arange(16,dtype='float'))

In [7]: view['a']
Out[7]: [array([ 0.,  1.,  2.,  3.]),
         array([ 4.,  5.,  6.,  7.]),
         array([  8.,   9.,  10.,  11.]),
         array([ 12.,  13.,  14.,  15.])]

In [8]: %px totalsum = psum(a)
Parallel execution on engines: [0,1,2,3]

In [9]: view['totalsum']
Out[9]: [120.0, 120.0, 120.0, 120.0]
```

Any Python code that makes calls to MPI can be used in this manner, including
compiled C, C++ and Fortran libraries that have been exposed to Python.

[mpi]: https://www.mcs.anl.gov/research/projects/mpi
[mpi4py]: https://mpi4py.readthedocs.io/
[openmpi]: https://www.open-mpi.org
