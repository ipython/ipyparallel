import os

DEFAULT_MINICONDA_PATH = os.path.join(os.getcwd(), "miniconda3/bin/:")
env = os.environ.copy()
env["PATH"] = DEFAULT_MINICONDA_PATH + env["PATH"]

from subprocess import check_call

GITHUB_TOKEN = ""  # Token for machine user
ASV_TESTS_REPO = "github.com/tomoboy/ipyparallel_master_project.git"
IPYPARALLEL_REPO = "github.com/tomoboy/ipyparallel.git"


def cmd_run(*args, log_filename=None, error_filename=None):
    if len(args) == 1:
        args = args[0].split(" ")
    print(f'$ {" ".join(args)}')
    if not log_filename and not error_filename:
        check_call(args, env=env)
    else:
        check_call(
            args,
            env=env,
            stdout=open(log_filename, 'w'),
            stderr=open(error_filename, 'w'),
        )


if __name__ == "__main__":
    cmd_run(
        f"git clone -q https://{GITHUB_TOKEN}@{ASV_TESTS_REPO}"
    )  # Get benchmarks from repo
    print("Finished cloning benchmark repo")
    # Installing ipyparallel from the dev branch
    cmd_run(f"pip install -q git+https://{GITHUB_TOKEN}@{IPYPARALLEL_REPO}")
    print("Installed ipyparallel")
    # Create profile for ipyparallel, (should maybe be copied if we want some cusom values here)
    cmd_run("ipython profile create --parallel --profile=asv")
    cmd_run('echo 120000 > /proc/sys/kernel/threads-max')
    cmd_run('echo 600000 > /proc/sys/vm/max_map_count')
    cmd_run('echo 200000 > /proc/sys/kernel/piad_max')
    cmd_run('echo "*    hard    nproc    100000" > /etc/security/limits.d')
    cmd_run('echo "*    soft    nproc    100000" > /etc/security/limits.d')
