#!/usr/bin/env bash

function jobqueue_before_install {
  # Install miniconda
  ./ci/conda_setup.sh
  export PATH="$HOME/miniconda/bin:$PATH"
  conda install --yes -c conda-forge python=$TRAVIS_PYTHON_VERSION dask distributed flake8 pytest docrep
  # black only available for python 3
  if [[ "$TRAVIS_PYTHON_VERSION" =~ ^[3-9].+ ]]; then
    pip install black
  fi
}

function jobqueue_install {
  which python
  pip install --no-deps -e .
}

function jobqueue_script {
  flake8 -j auto dask_jobqueue
  if [[ "$TRAVIS_PYTHON_VERSION" =~ ^[3-9].+ ]]; then
     black --exclude versioneer.py --check .
  fi
  pytest --verbose
}

function jobqueue_after_script {
  echo "Done."
}
