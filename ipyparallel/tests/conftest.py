"""pytest fixtures"""

import pytest

from IPython.testing.tools import default_config
from IPython.terminal.interactiveshell import TerminalInteractiveShell

from . import setup, teardown


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
