"""pytest fixtures"""
import inspect
import os
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from IPython.testing.tools import default_config

from . import setup
from . import teardown


@pytest.fixture(autouse=True, scope="session")
def temp_ipython():
    with TemporaryDirectory(suffix="dotipython") as td:
        with mock.patch.dict(os.environ, {"IPYTHONDIR": td}):
            yield


def pytest_collection_modifyitems(items):
    """This function is automatically run by pytest passing all collected test
    functions.

    We use it to add asyncio marker to all async tests and assert we don't use
    test functions that are async generators which wouldn't make sense.
    """
    for item in items:
        if inspect.iscoroutinefunction(item.obj):
            item.add_marker('asyncio')
        assert not inspect.isasyncgenfunction(item.obj)


@pytest.fixture(scope="session")
def cluster(request):
    """Setup IPython parallel cluster"""
    setup()
    request.addfinalizer(teardown)


@pytest.fixture(scope='session')
def ipython():
    config = default_config()
    config.TerminalInteractiveShell.simple_prompt = True
    shell = TerminalInteractiveShell.instance(config=config)
    return shell


@pytest.fixture()
def ipython_interactive(request, ipython):
    """Activate IPython's builtin hooks

    for the duration of the test scope.
    """
    with ipython.builtin_trap:
        yield ipython
