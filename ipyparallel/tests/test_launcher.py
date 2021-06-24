"""Tests for launchers

Doesn't actually start any subprocesses, but goes through the motions of constructing
objects, which should test basic config.
"""
import logging
import os
import time

import pytest
from traitlets.config import Config

from ipyparallel.cluster import launcher as launcher_mod

# -------------------------------------------------------------------------------
# TestCase Mixins
# -------------------------------------------------------------------------------


@pytest.fixture()
def profile_dir(tmpdir):
    return str(tmpdir.mkdir("profile_foo"))


@pytest.fixture()
def cluster_id():
    return str(time.time())


@pytest.fixture()
def work_dir(tmpdir):
    return str(tmpdir.mkdir("work"))


@pytest.fixture()
def build_launcher(work_dir, profile_dir, cluster_id):
    default_kwargs = dict(
        work_dir=work_dir,
        profile_dir=profile_dir,
        cluster_id=cluster_id,
        log=logging.getLogger(),
    )

    def build_launcher(Launcher, **kwargs):
        kw = {}
        kw.update(default_kwargs)
        kw.update(kwargs)
        return Launcher(**kw)

    return build_launcher


@pytest.fixture(params=launcher_mod.all_launchers)
def launcher(request, build_launcher):
    return build_launcher(Launcher=request.param)


BATCH_LAUNCHERS = [
    cls
    for cls in launcher_mod.all_launchers
    if issubclass(
        cls, (launcher_mod.BatchControllerLauncher, launcher_mod.BatchEngineSetLauncher)
    )
]


@pytest.fixture(params=BATCH_LAUNCHERS)
def batch_launcher(request, build_launcher):
    return build_launcher(Launcher=request.param)


SSH_LAUNCHERS = [
    l
    for l in launcher_mod.all_launchers
    if issubclass(l, launcher_mod.SSHLauncher) and l is not launcher_mod.SSHLauncher
]


@pytest.fixture(params=SSH_LAUNCHERS)
def ssh_launcher(request, build_launcher):
    return build_launcher(Launcher=request.param)


def test_profile_dir_arg(launcher, profile_dir):
    assert "--profile-dir" in launcher.cluster_args
    arg_idx = launcher.cluster_args.index("--profile-dir")
    assert profile_dir in launcher.cluster_args
    assert launcher.cluster_args[arg_idx + 1] == profile_dir


def test_cluster_id_arg(launcher, cluster_id):
    assert "--cluster-id" in launcher.cluster_args
    arg_idx = launcher.cluster_args.index("--cluster-id")
    assert cluster_id in launcher.cluster_args
    assert launcher.cluster_args[arg_idx + 1] == cluster_id


def test_batch_template(batch_launcher, work_dir):
    launcher = batch_launcher
    batch_file = os.path.join(work_dir, launcher.batch_file_name)
    assert launcher.batch_file == batch_file
    launcher.write_batch_script(1)
    assert os.path.isfile(batch_file)


def test_ssh_remote_profile_dir(ssh_launcher, profile_dir):
    launcher = ssh_launcher
    assert launcher.remote_profile_dir == profile_dir
    cfg = Config()
    cfg[launcher.__class__.__name__].remote_profile_dir = "foo"
    launcher.update_config(cfg)
    assert launcher.remote_profile_dir == "foo"
