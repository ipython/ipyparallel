from subprocess import check_call
import os
import time
import sys

GITHUB_TOKEN = '***REMOVED***'  # Token for machine user
ASV_TESTS_REPO = 'github.com/tomoboy/ipyparallel_master_project.git'
IPYPARALLEL_REPO = 'github.com/tomoboy/ipyparallel.git@dev'


DEFAULT_MINICONDA_PATH = os.path.join(os.getcwd(), 'miniconda3/bin/:')
env = os.environ.copy()
env['PATH'] = DEFAULT_MINICONDA_PATH + env['PATH']


def cmd_run(*args):
    if len(args) == 1:
        args = args[0].split(' ')
    print(f'$ {" ".join(args)}')
    check_call(args, env=env)


if __name__ == '__main__':
    CURRENT_INSTANCE_NAME = sys.argv[1]

    cmd_run(
        'wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh'
    )  # Download miniconda
    cmd_run('bash Miniconda3-latest-Linux-x86_64.sh -b')  # Install miniconda
    cmd_run('conda init')  # init conda env
    cmd_run('conda install -c conda-forge asv -y')  # Install av
    cmd_run(
        f'git clone -q https://{GITHUB_TOKEN}@{ASV_TESTS_REPO}'
    )  # Get benchmarks from repo

    while not os.path.isfile('/etc/startup_script_finished'):
        time.sleep(1)  # Wait for startup script to finish

    # Installing ipyparallel from the dev branch
    cmd_run(f'pip install -q git+https://{GITHUB_TOKEN}@{IPYPARALLEL_REPO}')
    # Create profile for ipyparallel, (should maybe be copied if we want some cusom values here)
    cmd_run('ipython profile create --parallel --profile=asv')

    os.chdir('ipyparallel_master_project')
    cmd_run('ipcluster start -n 100 --daemon --profile=asv')  # Starting 100 engines
    cmd_run('asv run')
    cmd_run('ipcluster stop --profile=asv')
    cmd_run('git config user.email "boyum90@gmail.com"')
    cmd_run('git config user.name "Tom-Olav Boyum"')
    cmd_run('git add .')
    cmd_run(
        'git', 'commit', '-m', f'"Benchmarking results ran on {CURRENT_INSTANCE_NAME}"'
    )
    cmd_run('git push -q')
    # run asv benchmarks
