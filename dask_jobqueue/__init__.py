# flake8: noqa
from .core import JobQueueCluster
from .pbs import PBSCluster
from .slurm import SLURMCluster
from .sge import SGECluster

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
