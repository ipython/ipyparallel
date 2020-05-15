import os

DEFAULT_MINICONDA_PATH = os.path.join(os.getcwd(), "miniconda3/bin/:")
env = os.environ.copy()
env["PATH"] = DEFAULT_MINICONDA_PATH + env["PATH"]

import atexit
from subprocess import check_call
import socket
import googleapiclient.discovery as gcd
from google.cloud import storage
import sys
from cluster_start import start_cluster

GITHUB_TOKEN = "***REMOVED***"  # Token for machine user
ASV_TESTS_REPO = "github.com/tomoboy/ipyparallel_master_project.git"
IPYPARALLEL_REPO = "github.com/tomoboy/ipyparallel.git"
ZONE = "europe-west1-b"
PROJECT_NAME = "jupyter-simula"
BUCKET_NAME = 'ipyparallel_dev'

instance_name = socket.gethostname()
compute = gcd.build("compute", "v1")


def delete_self():
    print(f'deleting: {instance_name}')
    compute.instances().delete(
        project=PROJECT_NAME, zone=ZONE, instance=instance_name
    ).execute()


def upload_file(filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{instance_name}/{filename}')
    print(f'Uploading {filename}')
    blob.upload_from_filename(filename)


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
    atexit.register(delete_self)
    template_name = sys.argv[2]

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
    error_log_filename = f'{instance_name}.error.log'
    ps = start_cluster(3, 'depth_3', 200) + start_cluster(0, 'depth_0', 200)

    def clean_up():
        for p in ps:
            ps.kill()

    atexit.register(clean_up)
    # cmd_run("ipcluster start -n 200 --daemon --profile=asv")  # Starting 200 engines
    cmd_run("asv run --quick --show-stderr", log_filename=log_filename, error_filename=error_log_filename)
    clean_up()
    # cmd_run("ipcluster stop --profile=asv")
    print('uploading files')
    upload_file(log_filename)
    upload_file(error_log_filename)
    results_dir = f'results/{template_name}'
    for file_name in os.listdir(results_dir):
        if 'conda' in file_name:
            upload_file(f'{results_dir}/{file_name}')
    print('script finished')