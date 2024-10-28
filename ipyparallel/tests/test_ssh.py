import os
from functools import partial

import pytest
from traitlets.config import Config

from .conftest import Cluster as BaseCluster  # noqa: F401
from .test_cluster import (
    test_get_output,  # noqa: F401
    test_restart_engines,  # noqa: F401
    test_signal_engines,  # noqa: F401
    test_start_stop_cluster,  # noqa: F401
    test_to_from_dict,  # noqa: F401
)

# import tests that use engine_launcher_class fixture


@pytest.fixture(params=["SSH", "SSHProxy"])
def ssh_config(ssh_key, request):
    windows = os.name == "nt"

    if (
        windows and request.param == "SSHProxy"
    ):  # SSHProxy currently not working under Windows
        pytest.skip("Proxy tests currently not working under Windows")

    c = Config()
    c.Cluster.controller_ip = '0.0.0.0'
    c.Cluster.engine_launcher_class = request.param
    engine_set_cfg = c[f"{request.param}EngineSetLauncher"]
    engine_set_cfg.ssh_args = []
    if not windows:
        engine_set_cfg.ssh_args.extend(
            [
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "StrictHostKeyChecking=no",
                "-i",
                ssh_key,
            ]
        )
    engine_set_cfg.scp_args = list(engine_set_cfg.ssh_args)  # copy
    if windows:
        engine_set_cfg.remote_python = "python"
        engine_set_cfg.remote_profile_dir = "C:/Users/ciuser/.ipython/profile_default"
    else:
        engine_set_cfg.remote_python = "/opt/conda/bin/python3"
        engine_set_cfg.remote_profile_dir = "/home/ciuser/.ipython/profile_default"
    engine_set_cfg.engine_args = ['--debug']
    c.SSHProxyEngineSetLauncher.hostname = "127.0.0.1"
    c.SSHProxyEngineSetLauncher.ssh_args.append("-p2222")
    c.SSHProxyEngineSetLauncher.scp_args.append("-P2222")
    c.SSHProxyEngineSetLauncher.user = "ciuser"
    c.SSHEngineSetLauncher.engines = {"ciuser@127.0.0.1:2222": 4}
    return c


@pytest.fixture
def Cluster(ssh_config, BaseCluster):  # noqa: F811
    """Override Cluster to add ssh config"""
    return partial(BaseCluster, config=ssh_config)


# override engine_launcher_class
@pytest.fixture
def engine_launcher_class(ssh_config):
    return ssh_config.Cluster.engine_launcher_class
