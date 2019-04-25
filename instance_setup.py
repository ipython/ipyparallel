from subprocess import check_call
import os
import time

GITHUB_TOKEN = 'bab6822b7273de549eee0794b7095fd0ccf58b6d'


def cmd_run(*args):
    print(f'$ {" ".join(args)}')
    check_call(args)


if __name__ == '__main__':
    cmd_run(
        'git',
        'clone',
        f'https://{GITHUB_TOKEN}@github.com/tomoboy/ipyparallel_master_project.git',
    )

    while not os.path.isfile('/etc/startup_script_finished'):
        time.sleep(1)
    # Waiting for start up script to finish

    # run asv benchmarks
