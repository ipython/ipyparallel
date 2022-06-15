# syntax = docker/dockerfile:1.2.1
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
 rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update && apt-get -y install python3-pip slurm-wlm
ENV PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} python3 -m pip install ipyparallel pytest-asyncio pytest-cov
RUN mkdir /var/spool/slurmctl \
 && mkdir /var/spool/slurmd
COPY slurm.conf /etc/slurm-llnl/slurm.conf
COPY entrypoint.sh /entrypoint
ENV IPP_DISABLE_JS=1
ENTRYPOINT ["/entrypoint"]

# the mounted directory
RUN mkdir /io
ENV PYTHONPATH=/io
WORKDIR "/io"
