#!/usr/bin/env bash

function jobqueue_before_install {
  true  # Pass
}

function jobqueue_install {
  which python
  pip install --no-deps -e .
}

function jobqueue_script {
  flake8 -j auto dask_jobqueue
  black --exclude versioneer.py --check .
  pytest --verbose
}

function jobqueue_after_script {
  echo "Done."
}
