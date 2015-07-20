# Interactive Parallel Computing with IPython

ipyparallel is the new home of IPython.parallel.

## Install

Install ipyparallel:

    pip install ipyparallel

To install the `IPython Clusters` tab in Jupyter Notebook, add this to your `jupyter_notebook_config.py`:

```python
c.NotebookApp.server_extensions.append('ipyparallel.nbextension')
```


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
