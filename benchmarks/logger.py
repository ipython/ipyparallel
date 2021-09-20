import os
from datetime import date

from ipyparallel_master_project.benchmarks.utils import get_time_stamp

LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
GCLOUD_DIR = os.path.join(LOGS_DIR, 'gcloud_output')
PROFILING_DIR = os.path.join(LOGS_DIR, 'profiling')
TODAY = str(date.today())


def get_dir(main_dir):
    target_dir = f"{main_dir}/{TODAY}"
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    return target_dir


def get_profiling_log_file_name():
    return os.path.join(get_dir(PROFILING_DIR), f'profiling_{get_time_stamp()}')


def get_gcloud_log_file_name(instance_name):
    return os.path.join(get_dir(GCLOUD_DIR), instance_name)


if __name__ == "__main__":
    print(LOGS_DIR)
