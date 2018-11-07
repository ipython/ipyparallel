FROM giovtorres/slurm-docker-cluster

RUN curl -o miniconda.sh https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash miniconda.sh -f -b -p /opt/anaconda && \
    /opt/anaconda/bin/conda clean -tipy && \
    rm -f miniconda.sh
ENV PATH /opt/anaconda/bin:$PATH
RUN conda install --yes -c conda-forge python=3.6 dask distributed flake8 pytest docrep

ENV LC_ALL en_US.UTF-8

COPY slurm.conf /etc/slurm/slurm.conf
