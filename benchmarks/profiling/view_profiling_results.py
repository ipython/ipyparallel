import os

master_project_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir)
)
ALL_RESULTS_DIRECTORY = os.path.join(master_project_path, 'results', 'profiling')


def get_latest_results_dir():
    return os.path.join(
        'results',
        'profiling',
        max(
            dirname
            for dirname in os.listdir(ALL_RESULTS_DIRECTORY)
            if 'initial_results' not in dirname
        ),
    )


def get_initial_results_dir():
    return os.path.join(
        'results',
        'profiling',
        next(
            (
                dirname
                for dirname in os.listdir(ALL_RESULTS_DIRECTORY)
                if 'initial_results' in dirname
            ),
            max(dirname for dirname in os.listdir(ALL_RESULTS_DIRECTORY)),
        ),
    )


if __name__ == '__main__':
    print(get_latest_results_dir())
