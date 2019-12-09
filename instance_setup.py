from subprocess import check_call
import os

GITHUB_TOKEN = "***REMOVED***"  # Token for machine user
ASV_TESTS_REPO = "github.com/tomoboy/ipyparallel_master_project.git"
IPYPARALLEL_REPO = "github.com/tomoboy/ipyparallel.git"


DEFAULT_MINICONDA_PATH = os.path.join(os.getcwd(), "miniconda3/bin/:")
env = os.environ.copy()
env["PATH"] = DEFAULT_MINICONDA_PATH + env["PATH"]


def cmd_run(*args):
    if len(args) == 1:
        args = args[0].split(" ")
    print(f'$ {" ".join(args)}')
    check_call(args, env=env)


if __name__ == "__main__":
    cmd_run(
        "wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
    )  # Download miniconda
    cmd_run("bash Miniconda3-latest-Linux-x86_64.sh -b")  # Install miniconda
    cmd_run("conda init")  # init conda env
    cmd_run("conda install -c conda-forge asv -y")  # Install av
    cmd_run(
        f"git clone -q https://{GITHUB_TOKEN}@{ASV_TESTS_REPO}"
    )  # Get benchmarks from repo
    print("Finished cloning benchmark repo")
    # Installing ipyparallel from the dev branch
    cmd_run(f"pip install -q git+https://{GITHUB_TOKEN}@{IPYPARALLEL_REPO}")
    print("Installed ipyparallel")
    # Create profile for ipyparallel, (should maybe be copied if we want some cusom values here)
    cmd_run("ipython profile create --parallel --profile=asv")

    os.chdir("ipyparallel_master_project")
    cmd_run("ipcluster start -n 100 --daemon --profile=asv")  # Starting 100 engines
    cmd_run("asv run master..dev")
    cmd_run("ipcluster stop --profile=asv")
    # run asv benchmarks
