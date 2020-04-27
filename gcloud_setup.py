# /Users/tomo/anaconda3/envs/master_thesis/bin/python
import atexit
import sys

from benchmarks.utils import get_time_stamp
from subprocess import check_call
import os
import googleapiclient.discovery as gcd
from typing import List
import multiprocessing as mp
from time import sleep
from logger import get_gcloud_log_file_name

CORE_NUMBERS_FOR_TEMPLATES = [16, 32, 64]
ZONE = "europe-west1-b"
PROJECT_NAME = "jupyter-simula"
INSTANCE_NAME_PREFIX = "asv-testing-"
MACHINE_CONFIGS_DIR = os.path.join(os.getcwd(), "machine_configs")

compute = gcd.build("compute", "v1")


def generate_template_name(number_of_cores_and_ram):
    return f"{INSTANCE_NAME_PREFIX}{number_of_cores_and_ram}-{number_of_cores_and_ram}"


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


def gcloud_run(*args, instance_name=""):
    cmd = ["gcloud", "compute"] + list(args)
    print(f'$ {" ".join(cmd)}')
    check_call(
        cmd,
        stdout=open(get_gcloud_log_file_name(instance_name) + ".log", "a+"),
        stderr=open(f"{get_gcloud_log_file_name(instance_name)}_error.out", "a+"),
    )


def copy_files_to_instance(instance_name, *file_names, directory="~"):
    for file_name in file_names:
        gcloud_run(
            "scp",
            file_name,
            f"{instance_name}:{directory}",
            f"--zone={ZONE}",
            instance_name=instance_name,
        )


def command_over_ssh(instance_name, *args):
    return gcloud_run(
        "ssh", instance_name, f"--zone={ZONE}", "--", *args, instance_name=instance_name
    )


def run_on_instance(template_name):
    current_instance_name = f"{template_name}-{get_time_stamp()}"
    print(f"Creating new instance with name: {current_instance_name}")

    gcloud_run(
        "instances",
        "create",
        current_instance_name,
        "--source-instance-template",
        template_name,
        instance_name=current_instance_name,
    )
    sleep(20)  # Waiting for ssh keys to propagate to instance
    command_over_ssh(current_instance_name, "sudo", "apt", "update")
    command_over_ssh(
        current_instance_name,
        "sudo",
        "DEBIAN_FRONTEND=noninteractive",
        "apt",
        "install",
        "-y",
        "--yes",
        "--assume-yes",
        "python3-pip",
    )
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
    result_dir = f"results/{current_instance_name}"
    print("copying instance setup to instance")
    copy_files_to_instance(current_instance_name, "instance_setup.py")
    print("starting instance setup")
    command_over_ssh(
        current_instance_name,
        'pip',
        'install',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-auth-oauthlib',
    )
    command_over_ssh(
        current_instance_name, 'pip', 'install', 'google-cloud-storage'
    )

    command_over_ssh(
        current_instance_name, "python3", "instance_setup.py", current_instance_name
    )
    os.makedirs(result_dir)
    print("copying results from instance")
    gcloud_run(
        "scp",
        "--recurse",
        f"{current_instance_name}:~/ipyparallel_master_project/results/{template_name}/.",
        os.path.abspath(result_dir),
        f"--zone={ZONE}",
        instance_name=current_instance_name,
    )


if __name__ == "__main__":
    running_instances = get_running_instance_names()
    number_of_running_instances = len(running_instances)
    atexit.register(delete_all_instances)

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

    with mp.Pool(len(CORE_NUMBERS_FOR_TEMPLATES)) as pool:
        result = pool.map_async(
            run_on_instance,
            [
                generate_template_name(core_number)
                for core_number in CORE_NUMBERS_FOR_TEMPLATES
            ],
        )
        result.wait()
    print("script finished.")
