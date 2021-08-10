"""pytest fixtures"""
import inspect
import logging
import os
import sys
from contextlib import contextmanager
from subprocess import check_call
from subprocess import check_output
from tempfile import TemporaryDirectory
from unittest import mock

import IPython.paths
import pytest
import zmq
from IPython.core.profiledir import ProfileDir
from IPython.terminal.interactiveshell import TerminalInteractiveShell
from IPython.testing.tools import default_config
from traitlets.config import Config

import ipyparallel as ipp
from . import setup
from . import teardown


@contextmanager
def temporary_ipython_dir():
    # FIXME: cleanup has issues on Windows
    # this is *probably* a real bug of holding open files,
    # but it is preventing feedback about test failures
    td_obj = TemporaryDirectory(suffix=".ipython")
    td = td_obj.name

    with mock.patch.dict(os.environ, {"IPYTHONDIR": td}):
        assert IPython.paths.get_ipython_dir() == td
        pd = ProfileDir.create_profile_dir_by_name(td, name="default")
        # configure fast heartbeats for quicker tests with small numbers of local engines
        with open(os.path.join(pd.location, "ipcontroller_config.py"), "w") as f:
            f.write("c.HeartMonitor.period = 200")
        try:
            yield td
        finally:
            try:
                td_obj.cleanup()
            except Exception as e:
                print(f"Failed to cleanup {td}: {e}", file=sys.stderr)


@pytest.fixture(autouse=True, scope="module")
def ipython_dir(request):
    with temporary_ipython_dir() as ipython_dir:
        yield ipython_dir


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


@pytest.fixture(scope="module")
def cluster(request, ipython_dir):
    """Setup IPython parallel cluster"""
    setup()
    try:
        yield
    finally:
        teardown()


@pytest.fixture(scope='module')
def ipython(ipython_dir):
    config = default_config()
    config.TerminalInteractiveShell.simple_prompt = True
    shell = TerminalInteractiveShell.instance(config=config)
    yield shell
    TerminalInteractiveShell.clear_instance()


@pytest.fixture()
def ipython_interactive(request, ipython):
    """Activate IPython's builtin hooks

    for the duration of the test scope.
    """
    with ipython.builtin_trap:
        yield ipython


@pytest.fixture(autouse=True)
def Context():
    ctx = zmq.Context.instance()
    try:
        yield ctx
    finally:
        ctx.destroy()


@pytest.fixture
def Cluster(request, ipython_dir, io_loop):
    """Fixture for instantiating Clusters"""

    def ClusterConstructor(**kwargs):
        log = logging.getLogger(__file__)
        log.setLevel(logging.DEBUG)
        log.handlers = [logging.StreamHandler(sys.stdout)]
        kwargs['log'] = log
        engine_launcher_class = kwargs.get("engine_launcher_class")

        cfg = kwargs.setdefault("config", Config())
        cfg.EngineLauncher.engine_args = ['--log-level=10']
        cfg.ControllerLauncher.controller_args = ['--log-level=10']
        kwargs.setdefault("controller_args", ['--ping=250'])
        kwargs.setdefault("load_profile", False)

        c = ipp.Cluster(**kwargs)
        if not kwargs['load_profile']:
            assert c.config == cfg
        request.addfinalizer(c.stop_cluster_sync)
        return c

    yield ClusterConstructor


@pytest.fixture(scope="session")
def ssh_dir(request):
    """Start the ssh service with docker-compose

    Fixture returns the directory
    """
    repo_root = os.path.abspath(os.path.join(ipp.__file__, os.pardir, os.pardir))
    ci_directory = os.environ.get("CI_DIR", os.path.join(repo_root, 'ci'))
    ssh_dir = os.path.join(ci_directory, "ssh")

    # only run ssh test if service was started before
    try:
        out = check_output(['docker-compose', 'ps', '-q'], cwd=ssh_dir)
    except Exception:
        pytest.skip("Needs docker compose")
    else:
        if not out.strip():
            pytest.skip("ssh service not running")

    # below is necessary for building/starting service as part of fixture
    # currently we use whether the service is already started to decide whether to run the tests
    # # build image
    # check_call(["docker-compose", "build"], cwd=ssh_dir)
    # # launch service
    # check_call(["docker-compose", "up", "-d"], cwd=ssh_dir)
    # # shutdown service when we exit
    # request.addfinalizer(lambda: check_call(["docker-compose", "down"], cwd=ssh_dir))
    return ssh_dir


@pytest.fixture
def ssh_key(tmpdir, ssh_dir):
    key_file = tmpdir.join("id_rsa")
    check_call(
        # this should be `docker compose cp sshd:...`
        # but docker-compose 1.x doesn't support `cp` yet
        [
            'docker',
            'cp',
            'ssh_sshd_1:/home/ciuser/.ssh/id_rsa',
            key_file,
        ],
        cwd=ssh_dir,
    )
    os.chmod(key_file, 0o600)
    with key_file.open('r') as f:
        assert 'PRIVATE KEY' in f.readline()
    return str(key_file)
