from functools import partial

import pytest
from traitlets.config import Config

from .conftest import Cluster as BaseCluster  # noqa: F401
from .test_cluster import test_restart_engines  # noqa: F401
from .test_cluster import test_signal_engines  # noqa: F401
from .test_cluster import test_start_stop_cluster  # noqa: F401
from .test_cluster import test_to_from_dict  # noqa: F401

# import tests that use engine_launcher_class fixture


@pytest.fixture
def ssh_config(ssh_key):
    c = Config()
    c.Cluster.controller_ip = '0.0.0.0'
    c.Cluster.engine_launcher_class = 'SSH'
    c.SSHEngineSetLauncher.scp_args = c.SSHLauncher.ssh_args = [
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "StrictHostKeyChecking=no",
        "-i",
        ssh_key,
    ]
    c.SSHEngineSetLauncher.engines = {"ciuser@127.0.0.1:2222": 4}
    c.SSHEngineSetLauncher.remote_python = "/opt/conda/bin/python3"
    c.SSHEngineSetLauncher.remote_profile_dir = "/home/ciuser/.ipython/profile_default"
    c.SSHEngineSetLauncher.engine_args = ['--debug']
    return c


@pytest.fixture
def Cluster(ssh_config, BaseCluster):  # noqa: F811
    """Override Cluster to add ssh config"""
    return partial(BaseCluster, config=ssh_config)


# override engine_launcher_class
@pytest.fixture
def engine_launcher_class(ssh_config):
    return 'SSH'
