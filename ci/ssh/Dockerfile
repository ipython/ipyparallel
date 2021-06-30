# syntax = docker/dockerfile:1.2.1
FROM ubuntu:20.04
RUN --mount=type=cache,target=/var/cache/apt \
    rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update \
 && apt-get -y install wget openssh-server

ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV PATH=$MAMBA_ROOT_PREFIX/bin:$PATH
RUN wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba \
 && mv bin/micromamba /usr/local/bin/micromamba

RUN --mount=type=cache,target=${MAMBA_ROOT_PREFIX}/pkgs \
    micromamba install -y -p $MAMBA_ROOT_PREFIX -c conda-forge \
        python=3.8 \
        pip \
        ipyparallel

# generate a user with home directory and trusted ssh keypair
RUN useradd -m -s /bin/bash -N ciuser
USER ciuser
RUN mkdir ~/.ssh \
 && chmod 0700 ~/.ssh \
 && ssh-keygen -q -t rsa -N '' -f /home/ciuser/.ssh/id_rsa \
 && cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys \
 && chmod 0600 ~/.ssh/*
USER root


ENV PIP_CACHE_DIR=/tmp/pip-cache
COPY . /src/ipyparallel
RUN --mount=type=cache,target=${PIP_CACHE_DIR} python3 -m pip install -e 'file:///src/ipyparallel#egg=ipyparallel[test]'

# needed for sshd to start
RUN mkdir /run/sshd
# run sshd in the foreground
CMD /usr/sbin/sshd -D -e
