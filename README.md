# Interactive Parallel Computing with IPython

ipyparallel is the new home of IPython.parallel.

## Install

Install ipyparallel:

    pip install ipyparallel

To install the `IPython Clusters` tab in Jupyter Notebook, add this to your `jupyter_notebook_config.py`:

```python
c.NotebookApp.server_extensions.append('ipyparallel.nbextension')
```

See the [documentation on configuring the notebook server](http://jupyter-notebook.readthedocs.org/en/latest/examples/Notebook/Configuring%20the%20Notebook%20and%20Server.html)
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
