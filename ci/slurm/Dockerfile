FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt \
 rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update && apt-get -y install \
        gosu \
        mysql-client \
        python3-venv \
        python3-pip \
        slurm-wlm \
        slurmdbd \
        slurm

ENV PIP_CACHE_DIR=/tmp/pip-cache \
    VIRTUAL_ENV=/srv/env \
    PATH=/srv/env/bin:${PATH} \
    IPP_DISABLE_JS=1

RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    python3 -m venv $VIRTUAL_ENV \
 && $VIRTUAL_ENV/bin/python3 -m pip install ipyparallel pytest-asyncio pytest-cov

# initialize some filesystem
RUN mkdir -p /etc/sysconfig/slurm \
        /var/spool/slurmd \
        /var/run/slurmd \
        /var/run/slurmdbd \
        /var/lib/slurmd \
        /data \
    && touch /var/lib/slurmd/node_state \
        /var/lib/slurmd/front_end_state \
        /var/lib/slurmd/job_state \
        /var/lib/slurmd/resv_state \
        /var/lib/slurmd/trigger_state \
        /var/lib/slurmd/assoc_mgr_state \
        /var/lib/slurmd/assoc_usage \
        /var/lib/slurmd/qos_usage \
        /var/lib/slurmd/fed_mgr_state \
    && chown -R slurm:slurm /var/*/slurm* \
    && mkdir /run/munge \
    && chown munge:munge /run/munge \
    && chmod a+rwxt /run/munge
    # && mungekey -c

COPY --chown=slurm:slurm --chmod=0600 etc_slurm/ /etc/slurm/


COPY entrypoint.sh /entrypoint
ENTRYPOINT ["/entrypoint"]

# the mounted directory
RUN mkdir /io
ENV PYTHONPATH=/io
WORKDIR "/io"

CMD [ "tail", "-f", "/var/log/slurm/slurmd.log" ]
