from subprocess import check_call
import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        results_file_name = sys.argv[1]
        check_call(f'pyprof2calltree -k -i {results_file_name} &', shell=True)
