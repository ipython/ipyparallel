from __future__ import absolute_import, division, print_function

from .core import docstrings
from .pbs import PBSCluster


class MoabCluster(PBSCluster):
    __doc__ = docstrings.with_indents(
        """Launch Dask on a Moab cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#PBS -A` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other PBS options, for example -j oe. Each option will be prepended with the #PBS prefix.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> import os
    >>> from dask_jobqueue import MoabCluster
    >>> cluster = MoabCluster(processes=6, cores=6, project='gfdl_m',
                              memory='96G', resource_spec='96G',
                              job_extra=['-d /home/First.Last', '-M none'],
                              local_directory=os.getenv('TMPDIR', '/tmp'))
    >>> cluster.scale(60)  # submit enough jobs to deploy 10 workers

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """,
        4,
    )
    submit_command = "msub"
    cancel_command = "canceljob"
    scheduler_name = "moab"
