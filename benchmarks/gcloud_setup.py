#!/usr/bin/env python3
import multiprocessing as mp
import os
import sys
from subprocess import Popen, check_call
from time import sleep
from typing import List

import googleapiclient.discovery as gcd

from benchmarks.utils import get_time_stamp

CORE_NUMBERS_FOR_TEMPLATES = [64]

ZONE = "europe-west1-b"
PROJECT_NAME = "jupyter-simula"
INSTANCE_NAME_PREFIX = "asv-testing-"
MACHINE_CONFIGS_DIR = os.path.join(os.getcwd(), "machine_configs")
CONDA_PATH = 'miniconda3/bin/conda'

compute = gcd.build("compute", "v1")


def generate_template_name(number_of_cores_and_ram):
    return f"{INSTANCE_NAME_PREFIX}{number_of_cores_and_ram}"


def get_running_instance_names() -> List[str]:
    result = compute.instances().list(project=PROJECT_NAME, zone=ZONE).execute()
    return [item["name"] for item in result["items"]] if "items" in result else []


def delete_instance(instance_name) -> dict:
    print(f"Deleting instance: {instance_name}")
    return (
        compute.instances()
        .delete(project=PROJECT_NAME, zone=ZONE, instance=instance_name)
        .execute()
    )


def delete_all_instances():
    return [
        delete_instance(name)
        for name in get_running_instance_names()
        if INSTANCE_NAME_PREFIX in name
    ]


def gcloud_run(*args, block=True):
    cmd = ["gcloud", "compute"] + list(args)
    print(f'$ {" ".join(cmd)}')
    (
        check_call(
            cmd
            # stdout=open(get_gcloud_log_file_name(instance_name) + ".log", "a+"),
            # stderr=open(f"{get_gcloud_log_file_name(instance_name)}_error.out", "a+"),
        )
        if block
        else Popen(cmd)
    )


def copy_files_to_instance(instance_name, *file_names, directory="~"):
    for file_name in file_names:
        gcloud_run("scp", file_name, f"{instance_name}:{directory}", f"--zone={ZONE}")


def command_over_ssh(instance_name, *args, block=True):
    return gcloud_run("ssh", instance_name, f"--zone={ZONE}", "--", *args, block=block)


def run_on_instance(template_name):
    current_instance_name = f"{template_name}-{get_time_stamp()}"
    benchmark_name = sys.argv[1] if len(sys.argv) > 1 else ''

    print(f"Creating new instance with name: {current_instance_name}")

    gcloud_run(
        "instances",
        "create",
        current_instance_name,
        "--source-instance-template",
        template_name,
    )
    sleep(20)  # Waiting for ssh keys to propagate to instance
    command_over_ssh(current_instance_name, "sudo", "apt", "update")
    print("copying instance setup to instance")
    command_over_ssh(
        current_instance_name,
        'wget',
        '-q',
        'https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh',
    )
    print('installing miniconda')
    command_over_ssh(
        current_instance_name, 'bash', 'Miniconda3-latest-Linux-x86_64.sh', '-b'
    )
    command_over_ssh(
        current_instance_name,
        'echo',
        '"source  miniconda3/bin/activate"',
        '>>',
        '~/.bashrc',
    )
    command_over_ssh(current_instance_name, f'{CONDA_PATH}', 'init')
    print('installing asv and google api')
    command_over_ssh(
        current_instance_name,
        f'{CONDA_PATH}',
        'install',
        '-c',
        'conda-forge',
        'asv',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-auth-oauthlib',
        'google-cloud-storage',
        'numpy',
        '-y',
    )

    copy_files_to_instance(current_instance_name, "instance_setup.py")

    for config_name in os.listdir(MACHINE_CONFIGS_DIR):
        if config_name == template_name + ".json":
            copy_files_to_instance(
                current_instance_name,
                os.path.join(MACHINE_CONFIGS_DIR, config_name),
                directory="~/.asv-machine.json",
            )
            break
    else:
        print(f"Found no valid machine config for template: {template_name}.")
        exit(1)
    print("starting instance setup")
    command_over_ssh(
        current_instance_name,
        "miniconda3/bin/python3",
        "instance_setup.py",
        current_instance_name,
        template_name,
        block=True,
    )

    command_over_ssh(
        current_instance_name,
        'cd',
        'ipyparallel_master_project',
        ';',
        "~/miniconda3/bin/python3",
        'asv_runner.py',
        benchmark_name,
        template_name,
        block=False,
    )


if __name__ == "__main__":
    running_instances = get_running_instance_names()
    number_of_running_instances = len(running_instances)
    # atexit.register(delete_all_instances)

    print(f"Currently there are {number_of_running_instances} running instances.")
    if number_of_running_instances:
        print("Running instances: ")
        for instance in running_instances:
            print(f"  {instance}")

    if "-d" in sys.argv:
        result = delete_instance(sys.argv[2])
    elif "-da" in sys.argv:
        result = delete_all_instances()
    if "-q" in sys.argv:
        exit(0)

    if len(CORE_NUMBERS_FOR_TEMPLATES) == 1:
        run_on_instance(generate_template_name(CORE_NUMBERS_FOR_TEMPLATES[0]))
    else:
        with mp.Pool(len(CORE_NUMBERS_FOR_TEMPLATES)) as pool:
            result = pool.map_async(
                run_on_instance,
                [
                    generate_template_name(core_number)
                    for core_number in CORE_NUMBERS_FOR_TEMPLATES
                ],
            )
        result.wait()
    print("gcloud setup finished.")
