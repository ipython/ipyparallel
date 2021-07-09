import shutil

import pytest

from .test_cluster import test_restart_engines  # noqa: F401
from .test_cluster import test_signal_engines  # noqa: F401
from .test_cluster import test_start_stop_cluster  # noqa: F401
from .test_cluster import test_to_from_dict  # noqa: F401

# import tests that use engine_launcher_class fixture

# override engine_launcher_class
@pytest.fixture
def engine_launcher_class():
    if shutil.which("sbatch") is None:
        pytest.skip("Requires slurm")
    return 'Slurm'
