FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
 rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update && apt-get -y install python3-pip slurm-wlm python3-venv

ENV PIP_CACHE_DIR=/tmp/pip-cache \
    VIRTUAL_ENV=/srv/env \
    PATH=/srv/env/bin:${PATH}

RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    python3 -m venv $VIRTUAL_ENV \
 && $VIRTUAL_ENV/bin/python3 -m pip install ipyparallel pytest-asyncio pytest-cov
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
