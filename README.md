# Interactive Parallel Computing with IPython

ipyparallel is the new home of IPython.parallel.

## Install

Install ipyparallel:

    pip install ipyparallel

To enable the `IPython Clusters` tab in Jupyter Notebook:

    ipcluster nbextension enable


To disable it again:

    ipcluster nbextension disable


See the [documentation on configuring the notebook server](https://jupyter-notebook.readthedocs.org/en/latest/public_server.html)
to find your config or setup your initial `jupyter_notebook_config.py`.

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

See [the docs](https://ipyparallel.readthedocs.org) for more info.
