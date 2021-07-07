FROM jupyter/scipy-notebook:4a6f5b7e5db1
RUN mamba install -yq openmpi mpi4py
RUN pip install --upgrade https://github.com/ipython/ipyparallel/archive/HEAD.tar.gz
