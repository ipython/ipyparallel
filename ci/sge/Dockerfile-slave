FROM ubuntu:14.04

ENV LANG C.UTF-8

RUN apt-get update && apt-get install curl bzip2 git gcc -y --fix-missing

RUN curl -o miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
ARG PYTHON_VERSION
RUN conda install -c conda-forge python=$PYTHON_VERSION dask distributed pytest && conda clean -tipy

COPY ./setup-slave.sh /
COPY ./*.sh /
RUN bash ./setup-slave.sh
