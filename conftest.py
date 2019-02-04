# content of conftest.py

# Make loop fixture available in all tests
from distributed.utils_test import loop  # noqa: F401

import pytest


def pytest_addoption(parser):
    parser.addoption("-E", action="store", metavar="NAME",
                     help="only run tests matching the environment NAME.")


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line("markers",
                            "env(name): mark test to run only on named environment")


def pytest_runtest_setup(item):
    envnames = [mark.args[0] for mark in item.iter_markers(name='env')]
    if envnames:
        if item.config.getoption("-E") not in envnames:
            pytest.skip("test requires env in %r" % envnames)
