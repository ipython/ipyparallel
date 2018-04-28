# inspired from https://github.com/PBSPro/pbspro/blob/v18.1.beta/docker/centos7/
# multi-stage build
# build script will be triggered
FROM centos:7 AS builder
# install dependencies for building
RUN yum install -y gcc make rpm-build libtool hwloc-devel libX11-devel \
	libXt-devel libedit-devel libical-devel ncurses-devel perl \
	postgresql-devel python-devel tcl-devel  tk-devel swig expat-devel \
    openssl-devel libXext libXft git
# get latest PBS Pro source code
# TODO point to an identified tag
RUN git clone https://github.com/pbspro/pbspro.git /src/pbspro && \
    bash /src/pbspro/docker/centos7/build.sh

# base image 
FROM centos:7
LABEL description="PBS Professional Open Source"
# copy rpm and entrypoint script from builder
COPY --from=builder /root/rpmbuild/RPMS/x86_64/pbspro-server-*.rpm .
# install pbspro
RUN yum install -y pbspro-server-*.rpm
# install python and useful package
RUN yum install -y curl bzip2 git gcc sudo && yum clean all
RUN curl -o miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh

#The pbs master hostname and path
ENV PBS_MASTER pbs_master
ENV PATH /opt/pbs/bin:/opt/anaconda/bin:$PATH
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

RUN conda install --yes -c conda-forge python=3.6 dask distributed flake8 pytest docrep
# Take the latest version of distributed due to test failure otherwise (see #47 comment by mrocklin)
RUN pip install --no-cache-dir git+https://github.com/dask/distributed.git --upgrade
# Copy entrypoint
COPY ./*.sh /
RUN chmod a+x ./*.sh

# run entrypoint script
ENTRYPOINT ["bash", "/master-entrypoint.sh"]
