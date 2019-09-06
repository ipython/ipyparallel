import json
from datetime import datetime
from itertools import product

from utils import seconds_to_ms


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
                    duration_in_seconds, stats_for_measurement, number_of_tasks, delay
                )
                for duration_in_seconds, stats_for_measurement, (
                    number_of_tasks,
                    delay,
                ) in zip(
                    result_data["result"],
                    result_data["stats"],
                    product(result_data["params"][0], result_data["params"][1]),
                )
            ]
            for benchmark_name, result_data in results_data["results"].items()
        }


class Result:
    def __init__(
        self, duration_in_seconds, stats_for_measurement, number_of_tasks, delay
    ):
        self.number_of_tasks = int(number_of_tasks)
        self.delay = float(delay)
        if not duration_in_seconds:
            self.failed = True
            self.stats_for_measurement = None
            self.duration_in_ms = None
        else:
            self.failed = False
            self.stats_for_measurement = stats_for_measurement
            self.duration_in_ms = seconds_to_ms(duration_in_seconds)
