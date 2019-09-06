import os

from benchmark_result import BenchMarkResult

RESULTS_DIR = "results"


def get_benchmark_results():
    return {
        dir_content: BenchMarkResult(
            os.path.join(os.getcwd(), RESULTS_DIR, dir_content, file_name)
        )
        for dir_content in os.listdir("results")
        if (
            os.path.isdir(os.path.join(os.getcwd(), RESULTS_DIR, dir_content))
            and "asv-testing" in dir_content
        )
        for file_name in os.listdir(f"{RESULTS_DIR}/{dir_content}")
        if "machine" not in file_name
    }


if __name__ == "__main__":
    results = get_benchmark_results()
    print()
