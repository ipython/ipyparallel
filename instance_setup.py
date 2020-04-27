import atexit
from subprocess import check_call
import os
import socket
import googleapiclient.discovery as gcd
from google.cloud import storage
import sys

GITHUB_TOKEN = "***REMOVED***"  # Token for machine user
ASV_TESTS_REPO = "github.com/tomoboy/ipyparallel_master_project.git"
IPYPARALLEL_REPO = "github.com/tomoboy/ipyparallel.git"
ZONE = "europe-west1-b"
PROJECT_NAME = "jupyter-simula"
BUCKET_NAME = 'ipyparallel_dev'

DEFAULT_MINICONDA_PATH = os.path.join(os.getcwd(), "miniconda3/bin/:")
env = os.environ.copy()
env["PATH"] = DEFAULT_MINICONDA_PATH + env["PATH"]

instance_name = socket.gethostname()
compute = gcd.build("compute", "v1")


def delete_self():
    print(f'deleting: {instance_name}')
    compute.instance().delete(
        project=PROJECT_NAME, zone=ZONE, instance=instance_name
    ).execute()


def upload_file(filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(instance_name)
    blob.upload_from_filename(filename)


def cmd_run(*args):
    if len(args) == 1:
        args = args[0].split(" ")
    print(f'$ {" ".join(args)}')
    check_call(args, env=env)


if __name__ == "__main__":
    atexit.register(delete_self)
    cmd_run(
        "wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
    )  # Download miniconda
    cmd_run("bash Miniconda3-latest-Linux-x86_64.sh -b")  # Install miniconda
    cmd_run("conda init")  # init conda env
    cmd_run("conda install -c conda-forge asv -y")  # Install av
    cmd_run(
        'pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib'
    )
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

    log_filename = f'{instance_name}.log'
    error_log_filename = f'{instance_name}.log'

    with open(log_filename, 'w') as log_file, open(
        error_log_filename, 'w'
    ) as error_log:

        sys.stdout = log_file
        sys.stderr = error_log

        cmd_run("ipcluster start -n 100 --daemon --profile=asv")  # Starting 100 engines
        cmd_run("asv run master..dev")
        cmd_run("ipcluster stop --profile=asv")

    print('uploading files')
    upload_file(log_filename)
    upload_file(error_log_filename)
    result_files = os.listdir('results')
    results_filename = f'results/{instance_name}.json'
    for file_name in result_files:
        if 'conda' in file_name:
            os.rename(f'results/{file_name}', results_filename)
            upload_file(results_filename)
            exit(0)