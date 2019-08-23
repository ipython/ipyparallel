from datetime import date
import os

GCLOUD_DIRECTORY = 'logs/gcloud_output'


def get_file_name(instance_name):
    today = str(date.today())
    target_dir = f'{GCLOUD_DIRECTORY}/{today}'
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    return f'{target_dir}/{instance_name}'
