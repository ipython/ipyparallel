import shutil

import pytest

from .test_cluster import (
    test_get_output,  # noqa: F401
    test_restart_engines,  # noqa: F401
    test_signal_engines,  # noqa: F401
    test_start_stop_cluster,  # noqa: F401
    test_to_from_dict,  # noqa: F401
)

# import tests that use engine_launcher_class fixture


# override engine_launcher_class
@pytest.fixture
def engine_launcher_class():
    if shutil.which("mpiexec") is None:
        pytest.skip("Requires mpiexec")
    return 'mpi'


@pytest.fixture
def controller_launcher_class():
    if shutil.which("mpiexec") is None:
        pytest.skip("Requires mpiexec")
    return 'mpi'
