# content of conftest.py

import pytest


def pytest_addoption(parser):
    parser.addoption("-E", action="store", metavar="NAME",
                     help="only run tests matching the environment NAME.")


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "env(name): mark test to run only on named environment")


def pytest_runtest_setup(item):
    envmarker = item.get_marker("env")
    if envmarker is not None:
        envname = envmarker.args[0]
        if envname != item.config.getoption("-E"):
            pytest.skip("test requires env %r" % envname)
