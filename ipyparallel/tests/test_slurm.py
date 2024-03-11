import shutil

import pytest
from traitlets.config import Config

from .conftest import temporary_ipython_dir
from .test_cluster import (
    test_get_output,  # noqa: F401
    test_restart_engines,  # noqa: F401
    test_signal_engines,  # noqa: F401
    test_start_stop_cluster,  # noqa: F401
    test_to_from_dict,  # noqa: F401
)


# put ipython dir on shared filesystem
@pytest.fixture(autouse=True, scope="module")
def ipython_dir(request):
    if shutil.which("sbatch") is None:
        pytest.skip("Requires slurm")
    with temporary_ipython_dir(prefix="/data/") as ipython_dir:
        yield ipython_dir


@pytest.fixture
def cluster_config():
    c = Config()
    c.Cluster.controller_ip = '0.0.0.0'
    return c


# override launcher classes
@pytest.fixture
def engine_launcher_class():
    if shutil.which("sbatch") is None:
        pytest.skip("Requires slurm")
    return 'slurm'


@pytest.fixture
def controller_launcher_class():
    if shutil.which("sbatch") is None:
        pytest.skip("Requires slurm")
    return 'slurm'
