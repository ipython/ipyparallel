from .core import docstrings
from .pbs import PBSCluster


class MoabCluster(PBSCluster):
    __doc__ = docstrings.with_indents("""Launch Dask on a Moab cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#PBS -A` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l`
        option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other PBS options, for example -j oe. Each option will be
        prepended with the #PBS prefix.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import MoabCluster
    >>> cluster = MoabCluster(queue='regular', project='DaskOnMoab')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()

    It is a good practice to define local_directory to your Moab system scratch
    directory, and you should specify resource_spec according to the processes
    and threads asked:

    >>> cluster = MoabCluster(queue='regular', project='DaskOnMoab',
                              local_directory=os.getenv('TMPDIR', '/tmp'),
                              threads=4, processes=6, memory='16GB',
                              resource_spec='select=1:ncpus=24:mem=100GB')
    """, 4)
    submit_command = 'msub'
    cancel_command = 'canceljob'

    def _job_id_from_submit_output(self, out):
        return out.strip()
