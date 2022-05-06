"""A build backend that handles installing the template files.

See https://peps.python.org/pep-0517/#in-tree-build-backends
"""
import glob
import os
import shutil
import subprocess

from flit_core.buildapi import build_editable  # noqa
from flit_core.buildapi import build_sdist  # noqa
from flit_core.buildapi import build_wheel  # noqa
from flit_core.buildapi import (
    get_requires_for_build_editable as get_requires_for_build_editable_orig,
)
from flit_core.buildapi import (
    get_requires_for_build_sdist as get_requires_for_build_sdist_orig,
)
from flit_core.buildapi import (
    get_requires_for_build_wheel as get_requires_for_build_wheel_orig,
)

osp = os.path
here = osp.abspath(osp.dirname(__file__))
share_dir = osp.join(here, "jupyter-data", "share", "jupyter")
nbclassic_path = osp.join(share_dir, "nbextensions", "ipyparallel")
lab_path = osp.join(share_dir, "labextensions", "ipyparallel-labextension")


def _handle_labextension(cmd="build:prod"):
    if os.environ.get("IPP_DISABLE_JS") == "1":
        print("Skipping js installation")
        return

    # this tells us if labextension is built at all, not if it's up-to-date
    labextension_built = glob.glob(os.path.join(lab_path, "*"))
    needs_js = True
    if not os.path.isdir(os.path.join(here, ".git")):
        print("Installing from a dist, not a repo")
        # not in a repo, probably installing from sdist
        # could be git-archive, though!
        # skip rebuilding js if it's already present
        if labextension_built:
            print(f"Not regenerating labextension in {lab_path}")
            needs_js = False

    if needs_js:
        subprocess.check_call(['jlpm'], cwd=here)
        subprocess.check_call(['jlpm', 'run', cmd], cwd=here)

        source = osp.join(here, 'ipyparallel', 'labextension')
        if labextension_built:
            shutil.rmtree(lab_path)
        shutil.copytree(source, lab_path)


def _handle_nbextension():
    source = osp.join(here, 'ipyparallel', 'nbextension', 'static')
    if osp.exists(nbclassic_path):
        shutil.rmtree(nbclassic_path)
    shutil.copytree(source, nbclassic_path)


def get_requires_for_build_wheel(config_settings=None):
    _handle_labextension()
    _handle_nbextension()
    return get_requires_for_build_wheel_orig(config_settings=config_settings)


def get_requires_for_build_sdist(config_settings=None):
    _handle_labextension()
    _handle_nbextension()
    return get_requires_for_build_sdist_orig(config_settings=config_settings)


def get_requires_for_build_editable(config_settings=None):
    _handle_labextension(cmd="build")
    _handle_nbextension()
    return get_requires_for_build_editable_orig(config_settings=config_settings)
