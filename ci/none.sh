#!/usr/bin/env bash

set -x

function jobqueue_before_install {
  # Install miniconda
  ./ci/conda_setup.sh
  export PATH="$HOME/miniconda/bin:$PATH"
  conda install --yes -c conda-forge python=$TRAVIS_PYTHON_VERSION dask distributed flake8 pytest docrep
  pip install --no-cache-dir git+https://github.com/dask/dask.git --upgrade --no-deps
  pip install --no-cache-dir git+https://github.com/dask/distributed.git --upgrade --no-deps
}

function jobqueue_install {
  which python
  pip install --no-deps -e .
}

function jobqueue_script {
  flake8 -j auto dask_jobqueue
  py.test --verbose
}

function jobqueue_after_script {
    echo "Done."
}
