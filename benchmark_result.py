import json
from datetime import datetime
from itertools import product
import os
from utils import seconds_to_ms

RESULTS_DIR = "results"


class BenchMarkResult:
    def __init__(self, benchmark_results_file_name):
        with open(benchmark_results_file_name, "r") as results_file:
            results_data = json.load(results_file)
        self.date = datetime.fromtimestamp(
            results_data["date"] // 1000
        )  # Date is in ms
        self.machine_config = results_data["params"]
        self.results_dict = {
            benchmark_name: [
                Result(
                    duration_in_seconds,
                    stats_for_measurement,
                    param1,
                    param2,
                    is_delay="time_n_tasks" in benchmark_name,
                )
                for duration_in_seconds, stats_for_measurement, (param1, param2) in zip(
                    result_data["result"],
                    result_data["stats"],
                    product(result_data["params"][0], result_data["params"][1]),
                )
            ]
            for benchmark_name, result_data in results_data["results"].items()
        }


class Result:
    def __init__(
        self, duration_in_seconds, stats_for_measurement, param1, param2, is_delay=True
    ):
        if not duration_in_seconds:
            self.failed = True
        else:
            if is_delay:
                self.delay = float(param2)
                self.number_of_tasks = int(param1)
            else:
                self.number_of_bytes = int(param2)
                self.number_of_engines = int(param1)
            self.failed = False
            self.stats_for_measurement = stats_for_measurement
            self.duration_in_ms = seconds_to_ms(duration_in_seconds)


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
