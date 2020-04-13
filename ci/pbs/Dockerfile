# inspired from https://github.com/PBSPro/pbspro/blob/v18.1.beta/docker/centos7/
# multi-stage build
# build script will be triggered
FROM centos:7.5.1804 AS builder
# install dependencies for building
RUN yum install -y gcc make rpm-build libtool hwloc-devel libX11-devel \
	libXt-devel libedit-devel libical-devel ncurses-devel perl \
	postgresql-devel python-devel tcl-devel  tk-devel swig expat-devel \
        openssl-devel libXext libXft git postgresql-contrib
# get known PBS Pro source code
RUN git clone --branch release_18_1_branch https://github.com/pbspro/pbspro.git /src/pbspro
COPY build.sh /
RUN bash /build.sh

# base image
FROM centos:7.5.1804
LABEL description="PBS Professional Open Source and conda"

#The pbs master node name, can be overridden if needed
ENV PBS_MASTER pbs_master
ENV PATH /opt/pbs/bin:/opt/anaconda/bin:$PATH
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

COPY --from=builder /root/rpmbuild/RPMS/x86_64/pbspro-server-*.rpm .
# install pbspro and useful packages
RUN yum install -y pbspro-server-*.rpm curl bzip2 git gcc sudo openssh-server && yum clean all
# install python
RUN curl -o miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
RUN conda install --yes -c conda-forge python=3.6 dask distributed flake8 pytest pytest-asyncio

# Copy entrypoint and other needed scripts
COPY ./*.sh /
RUN chmod a+x ./*.sh

# default entrypoint launch pbs master
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
