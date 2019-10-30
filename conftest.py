# content of conftest.py

# Make loop fixture available in all tests
from distributed.utils_test import loop  # noqa: F401

import pytest

import dask_jobqueue.lsf


def pytest_addoption(parser):
    parser.addoption(
        "-E",
        action="store",
        metavar="NAME",
        help="only run tests matching the environment NAME.",
    )


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "env(name): mark test to run only on named environment"
    )


def pytest_runtest_setup(item):
    envnames = [mark.args[0] for mark in item.iter_markers(name="env")]
    if envnames:
        if item.config.getoption("-E") not in envnames:
            pytest.skip("test requires env in %r" % envnames)


@pytest.fixture(autouse=True)
def mock_lsf_version(monkeypatch, request):
    # Monkey-patch lsf_version() UNLESS the 'lsf' environment is selected.
    # In that case, the real lsf_version() function should work.
    markers = list(request.node.iter_markers())
    if any("lsf" in marker.args for marker in markers):
        return

    try:
        dask_jobqueue.lsf.lsf_version()
    except OSError:
        # Provide a fake implementation of lsf_version()
        monkeypatch.setattr(dask_jobqueue.lsf, "lsf_version", lambda: "10")
