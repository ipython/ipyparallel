import atexit
import os
import resource
import socket
import sys
from subprocess import check_call

import googleapiclient.discovery as gcd
from cluster_start import start_cluster
from google.cloud import storage

os.environ['OPENBLAS_NUM_THREADS'] = '1'

DEFAULT_MINICONDA_PATH = os.path.abspath(os.path.join('..', "miniconda3/bin/:"))
env = os.environ.copy()
env["PATH"] = DEFAULT_MINICONDA_PATH + env["PATH"]

ZONE = "europe-west1-b"
PROJECT_NAME = "jupyter-simula"
BUCKET_NAME = 'ipyparallel_dev'

instance_name = socket.gethostname()
compute = gcd.build("compute", "v1")


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


def delete_self():
    print(f'deleting: {instance_name}')
    compute.instances().delete(
        project=PROJECT_NAME, zone=ZONE, instance=instance_name
    ).execute()


def upload_file(filename):
    blob_name = sys.argv[1]
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{instance_name}/{blob_name}')
    print(f'Uploading {filename}')
    blob.upload_from_filename(filename)


if __name__ == '__main__':
    # atexit.register(delete_self)
    benchmark_name = sys.argv[1]
    template_name = sys.argv[2]

    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard_limit, hard_limit))

    ps = []
    for i in [3, 4]:
        ps += start_cluster(i, 1030, '../miniconda3/bin/', log_output_to_file=True)
    # time.sleep(10)
    # ps += start_cluster(
    #     0, 'depth_0', 300, '../miniconda3/bin/',
    #     log_output_to_file=True)
    # time.sleep(10)
    # ps += start_cluster(
    #     0, 'depth_1', 150, '../miniconda3/bin/',
    #     log_output_to_file=True)

    log_filename = f'{instance_name}.log'
    error_log_filename = f'{instance_name}.error.log'

    def clean_up():
        for p in ps:
            p.kill()

    atexit.register(clean_up)
    # cmd_run("ipcluster start -n 200 --daemon --profile=asv")  # Starting 200 engines
    cmd_run(
        'asv run -b DepthTestingSuite --verbose --show-stderr',
        # log_filename=log_filename,
        # error_filename=error_log_filename,
    )
    clean_up()
    # cmd_run("ipcluster stop --profile=asv")
    print('uploading files')
    # upload_file(log_filename)
    # upload_file(error_log_filename)
    results_dir = f'results/{template_name}'
    for file_name in os.listdir(results_dir):
        if 'conda' in file_name:
            upload_file(f'{results_dir}/{file_name}')
    print('script finished')
