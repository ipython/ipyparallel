from benchmarks.utils import wait_for
from logger import get_profiling_log_file_name
from subprocess import check_call
import profiling.profiling_code as profiling_code
import ipyparallel as ipp
import sys
import os

LOG_FILE_NAME = get_profiling_log_file_name()
PROFILING_CODE_PATH = os.path.abspath(profiling_code.__file__)
OUTPUT_FILE_NAME = f'{LOG_FILE_NAME}.cprof'


def start_cmd(cmd):
    check_call(cmd, stderr=open(f'{LOG_FILE_NAME}_error.out', 'a+'), shell=True)


if __name__ == "__main__":
    n = sys.argv[1] if len(sys.argv) > 1 else 16
    start_cmd(f'ipcluster start -n {n} --daemon --profile=asv')
    client = ipp.Client(profile='asv')
    print(f'Waiting for {n} engines to get available')
    wait_for(lambda: len(client) >= n)
    print('Starting the profiling')
    start_cmd(f'python -m cProfile -o {OUTPUT_FILE_NAME} {PROFILING_CODE_PATH} {n}')
    start_cmd(f'ipcluster stop --profile=asv')
    start_cmd(f'python view_profiling_results.py {OUTPUT_FILE_NAME} &')
