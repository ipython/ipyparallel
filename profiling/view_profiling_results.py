import os

master_project_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir)
)
ALL_RESULTS_DIRECTORY = os.path.join(master_project_path, 'results', 'profiling')


def get_latest_results_dir():
    return os.path.join(
        ALL_RESULTS_DIRECTORY,
        max(dirname for dirname in os.listdir(ALL_RESULTS_DIRECTORY)),
    )


if __name__ == '__main__':
    print(get_latest_results_dir())
