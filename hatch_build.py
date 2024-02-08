"""Custom build script for hatch backend"""

import glob
import os
import subprocess

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    def initialize(self, version, build_data):
        if self.target_name not in ["wheel", "sdist"]:
            return
        cmd = "build:prod" if version == "standard" else "build"
        osp = os.path
        here = osp.abspath(osp.dirname(__file__))
        lab_path = osp.join(here, 'ipyparallel', 'labextension')

        if os.environ.get("IPP_DISABLE_JS") == "1":
            print("Skipping js installation")
            return

        # this tells us if labextension is built at all, not if it's up-to-date
        labextension_built = glob.glob(osp.join(lab_path, "*"))
        needs_js = True
        if not osp.isdir(osp.join(here, ".git")):
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
