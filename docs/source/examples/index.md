---
file_format: mystnb
kernelspec:
  name: python3
mystnb:
  execution_mode: "force"
  remove_code_source: true
---

# examples

Here you will find example notebooks and scripts for working with IPython Parallel.

## Tutorials

```{toctree}
Cluster API
Parallel Magics
progress
Data Publication API
visualizing-tasks
broadcast/Broadcast view
broadcast/memmap Broadcast
broadcast/MPI Broadcast
```

## Examples

```{toctree}
Monitoring an MPI Simulation - 1
Monitoring an MPI Simulation - 2
Parallel Decorator and map
Using MPI with IPython Parallel
Monte Carlo Options
rmt/rmt
```

## Integrating IPython Parallel with other tools

There are lots of cool tools for working with asynchronous and parallel execution. IPython Parallel aims to be fairly compatible with these, both by implementing explicit support via methods such as `Client.become_dask`, and by using standards such as the `concurrent.futures.Future` API.

```{toctree}
Futures
joblib
dask
Using Dill
```

## Non-notebook examples

This directory also contains some examples that are scripts instead of notebooks.

```{code-cell}
import glob
import os

from IPython.display import FileLink, FileLinks, display

FileLinks(".", included_suffixes=[".py"], recursive=True)
```
