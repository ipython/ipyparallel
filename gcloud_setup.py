#! /usr/bin/python3
import datetime
import sys
from subprocess import check_call
import os
import googleapiclient.discovery as gcd
from typing import List


INSTANCE_SUFFIX = '16-16'
ZONE = 'europe-west1-b'
PROJECT_NAME = 'jupyter-simula'
TEMPLATE_NAME = 'asv-testing-' + INSTANCE_SUFFIX
INSTANCE_NAME_PREFIX = 'asv-testing-'
MACHINE_CONFIGS_DIR = os.path.join(os.getcwd(), 'machine_configs')

compute = gcd.build('compute', 'v1')


def get_running_instance_names() -> List[str]:
    result = compute.instances().list(project=PROJECT_NAME, zone=ZONE).execute()
    return [item['name'] for item in result['items']] if 'items' in result else []


def time_stamp() -> str:
    return (
        str(datetime.datetime.now()).split('.')[0].replace(' ', '-').replace(':', '-')
    )


def delete_instance(instance_name) -> dict:
    print(f'Deleting instance: {instance_name}')
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


def gcloud_run(*args):
    cmd = ['gcloud', 'compute'] + list(args)
    print(f'$ {" ".join(cmd)}')
    check_call(cmd)


def copy_files_to_instance(instance_name, *file_names, directory='~'):
    for file_name in file_names:
        gcloud_run('scp', file_name, f'{instance_name}:{directory}', f'--zone={ZONE}')


def command_over_ssh(instance_name, *args):
    return gcloud_run('ssh', instance_name, f'--zone={ZONE}', '--', *args)


if __name__ == '__main__':
    running_instances = get_running_instance_names()
    number_of_running_instances = len(running_instances)

    print(f'Currently there are {number_of_running_instances} running instances.')
    if number_of_running_instances:
        print('Running instances: ')
        for instance in running_instances:
            print(f'  {instance}')

    if '-d' in sys.argv:
        result = delete_instance(sys.argv[2])
    elif '-da' in sys.argv:
        result = delete_all_instances()
    if '-q' in sys.argv:
        exit(0)

    if '-use_last' in sys.argv and number_of_running_instances:
        current_instance_name = running_instances[-1]
        print(f'Using existing instance with name: {current_instance_name}')
    else:
        current_instance_name = f'{INSTANCE_NAME_PREFIX+INSTANCE_SUFFIX}-{time_stamp()}'
        print(f'Creating new instance with name: {current_instance_name}')
        gcloud_run(
            'instances',
            'create',
            current_instance_name,
            '--source-instance-template',
            TEMPLATE_NAME,
            '--metadata-from-file',
            'startup-script=startup_script.sh',
        )
        for config_name in os.listdir(MACHINE_CONFIGS_DIR):
            if config_name == TEMPLATE_NAME + '.json':
                copy_files_to_instance(
                    current_instance_name,
                    os.path.join(MACHINE_CONFIGS_DIR, config_name),
                    directory='~/.asv-machine.json',
                )
                break
        else:
            print(f'Found no valid machine config for template: {TEMPLATE_NAME}.')
            exit(1)

        copy_files_to_instance(current_instance_name, 'instance_setup.py')

    command_over_ssh(
        current_instance_name, 'python3', 'instance_setup.py', current_instance_name
    )

    delete_instance(current_instance_name)
