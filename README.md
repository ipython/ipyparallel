# Interactive Parallel Computing with IPython

ipyparallel is the new home of IPython.parallel. ipyparallel is a Python package and collection of CLI scripts for controlling clusters for Jupyter. 

ipyparallel contains the following CLI scripts:

* ipcluster - start/stop a cluster
* ipcontroller - start a scheduler
* ipengine - start an engine

## Install

Install ipyparallel:

    pip install ipyparallel

To enable the `IPython Clusters` tab in Jupyter Notebook:

    ipcluster nbextension enable


To disable it again:

    ipcluster nbextension disable

See the [documentation on configuring the notebook server](https://jupyter-notebook.readthedocs.io/en/latest/public_server.html)
to find your config or setup your initial `jupyter_notebook_config.py`.

### JupyterHub Install

To install for all users on JupyterHub, as root:

    jupyter nbextension install --sys-prefix --py ipyparallel
    jupyter nbextension enable --sys-prefix --py ipyparallel
    jupyter serverextension enable --sys-prefix --py ipyparallel

## Run

Start a cluster:

    ipcluster start

Use it from Python:

```python
import os
import ipyparallel as ipp

rc = ipp.Client()
ar = rc[:].apply_async(os.getpid)
pid_map = ar.get_dict()
```

See [the docs](https://ipyparallel.readthedocs.io) for more info.
