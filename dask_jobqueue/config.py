from __future__ import absolute_import, division, print_function

import os

import dask
import yaml

fn = os.path.join(os.path.dirname(__file__), "jobqueue.yaml")
dask.config.ensure_file(source=fn)

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update(dask.config.config, defaults, priority="old")
