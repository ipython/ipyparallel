# Interactive Parallel Computing with IPython

IPython Parallel (`ipyparallel`) is a Python package and collection of CLI scripts for controlling clusters of IPython processes, built on the Jupyter protocol.

IPython Parallel provides the following commands:

- ipcluster - start/stop/list clusters
- ipcontroller - start a controller
- ipengine - start an engine

## Install

Install IPython Parallel:

    pip install ipyparallel

This will install and enable the IPython Parallel extensions
for Jupyter Notebook and (as of 7.0) Jupyter Lab 3.0.

## Run

Start a cluster:

    ipcluster start

Use it from Python:

```python
import os
import ipyparallel as ipp

cluster = ipp.Cluster(n=4)
with cluster as rc:
    ar = rc[:].apply_async(os.getpid)
    pid_map = ar.get_dict()
```

See [the docs](https://ipyparallel.readthedocs.io) for more info.
