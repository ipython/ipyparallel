import json
import pickle
from datetime import datetime
from enum import Enum
from itertools import product
import os

from benchmarks.constants import DEFAULT_NUMBER_OF_ENGINES
from utils import seconds_to_ms
import re

RESULTS_DIR = "results"


class BenchmarkType(Enum):
    ECHO_MANY_ARGUMENTS = 'EchoManyArguments'
    TIME_N_TASKS = 'Engines'
    BROADCAST = 'BroadCast'
    TIME_N_TASKS_NO_DELAY = 'NoDelay'


def get_benchmark_type(benchmark_name):
    if BenchmarkType.ECHO_MANY_ARGUMENTS.value in benchmark_name:
        return BenchmarkType.ECHO_MANY_ARGUMENTS
    elif BenchmarkType.TIME_N_TASKS_NO_DELAY.value in benchmark_name:
        return BenchmarkType.TIME_N_TASKS_NO_DELAY
    elif BenchmarkType.TIME_N_TASKS.value in benchmark_name:
        return BenchmarkType.TIME_N_TASKS
    else:
        return BenchmarkType.BROADCAST


def is_direct_view(benchmark_name):
    return 'DirectView' in benchmark_name or 'Broadcast' in benchmark_name


class BenchmarkResult:
    def __init__(self, benchmark_results_file_name):
        with open(benchmark_results_file_name, "r") as results_file:
            results_data = json.load(results_file)
        self.date = datetime.fromtimestamp(
            results_data["date"] // 1000
        )  # Date is in ms
        self.machine_config = results_data["params"]
        self.results_dict = {
            benchmark_name: {
                'benchmark_type': get_benchmark_type(benchmark_name),
                'engines': get_number_of_engines(benchmark_name),
                'is_direct_view': is_direct_view(benchmark_name),
                'results': [
                    Result(
                        duration_in_seconds,
                        stats_for_measurement,
                        param,
                        benchmark_name,
                        get_benchmark_type(benchmark_name),
                    )
                    for duration_in_seconds, stats_for_measurement, param in zip(
                        result_data["result"],
                        result_data["stats"],
                        (
                            product(result_data["params"][0], result_data["params"][1])
                            if len(result_data["params"]) > 1
                            else result_data['params'][0]
                        ),
                    )
                ],
            }
            for benchmark_name, result_data in results_data["results"].items()
        }


def get_number_of_engines(benchmark_name):
    match = re.search('[0-9]+', benchmark_name)
    return int(match[0]) if match else DEFAULT_NUMBER_OF_ENGINES


class Result:
    def __init__(
        self,
        duration_in_seconds,
        stats_for_measurement,
        param,
        benchmark_name,
        benchmark_type,
    ):
        if not duration_in_seconds:
            self.failed = True
            return
        self.stats_for_measurement = stats_for_measurement
        self.failed = False

        self.duration_in_ms = seconds_to_ms(duration_in_seconds)
        if benchmark_type is BenchmarkType.ECHO_MANY_ARGUMENTS:
            self.number_of_arguments = int(param)
            self.number_of_engines = DEFAULT_NUMBER_OF_ENGINES
        elif (
            benchmark_type is BenchmarkType.TIME_N_TASKS_NO_DELAY
            or benchmark_type is BenchmarkType.TIME_N_TASKS
        ):
            self.number_of_tasks = int(param[0])
            self.number_of_engines = get_number_of_engines(benchmark_name)
            self.delay = (
                0
                if benchmark_type is BenchmarkType.TIME_N_TASKS_NO_DELAY
                else float(param[1])
            )
        elif benchmark_type is BenchmarkType.BROADCAST:
            self.number_of_bytes = int(param[1])
            self.number_of_engines = int(param[0])
        else:
            raise Exception('BenchmarkType not found in Result constructor')


def get_benchmark_results():
    return {
        dir_content: BenchmarkResult(
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


def get_value_dict(engines_or_cores='engines'):
    return {
        'Duration in ms': [],
        'Number of tasks': [],
        f'Number of {engines_or_cores}': [],
    }


def ensure_time_n_tasks_source_structure(datasource, delay, number_of_cores, view_type):
    if delay not in datasource:
        datasource[delay] = {number_of_cores: {view_type: get_value_dict()}}
    elif number_of_cores not in datasource[delay]:
        datasource[delay][number_of_cores] = {view_type: get_value_dict()}
    elif view_type not in datasource[delay][number_of_cores]:
        datasource[delay][number_of_cores][view_type] = get_value_dict()


def add_time_n_tasks_source(source, benchmark, number_of_cores):
    number_of_engines = benchmark['engines']
    view_type = 'direct_view' if benchmark['is_direct_view'] else 'load_balanced'
    for duration, tasks_num, delay in [
        (result.duration_in_ms, result.number_of_tasks, seconds_to_ms(result.delay))
        for result in benchmark['results']
        if not result.failed
    ]:
        ensure_time_n_tasks_source_structure(source, delay, number_of_cores, view_type)
        dict_to_append_to = source[delay][number_of_cores][view_type]
        dict_to_append_to['Duration in ms'].append(duration)
        dict_to_append_to['Number of tasks'].append(tasks_num)
        dict_to_append_to['Number of engines'].append(number_of_engines)


def get_number_of_cores(machine_name):
    return int(re.findall('\d+', machine_name)[0])


def get_time_n_tasks_source(benchmark_results=None):
    if not benchmark_results:
        benchmark_results = get_benchmark_results()
    source = {}
    for machine_name, benchmark_run in benchmark_results.items():
        number_of_cores = get_number_of_cores(machine_name)
        for benchmark_name, benchmark in benchmark_run.results_dict.items():
            if benchmark['benchmark_type'] == BenchmarkType.TIME_N_TASKS:
                add_time_n_tasks_source(source, benchmark, number_of_cores)
    return source


def make_source():
    benchmark_results = get_benchmark_results()
    time_n_tasks_nodelay_source = get_value_dict('cores')
    throughput_source = {}
    for machine_name, benchmark_run in benchmark_results.items():
        number_of_cores = int(re.findall('\d+', machine_name)[0])
        for benchmark_name, benchmark in benchmark_run.results_dict.items():
            if benchmark['benchmark_type'] == BenchmarkType.TIME_N_TASKS:
                add_time_n_tasks_source()

        for result in benchmark_run.results_dict[
            'overhead_latency.Engines100NoDelay.time_n_tasks'
        ]:
            if result.failed:
                continue
            time_n_tasks_nodelay_source['Duration in ms'].append(result.duration_in_ms)
            time_n_tasks_nodelay_source['Number of tasks'].append(
                result.number_of_tasks
            )
            time_n_tasks_nodelay_source['Number of cores'].append(number_of_cores)
        throughput_source[number_of_cores] = {
            'Number of engines': [],
            'Number of bytes': [],
            'Duration in ms': [],
        }
        for result in benchmark_run.results_dict[
            'throughput.NumpyArrayBroadcast.time_broadcast'
        ]:
            if result.failed:
                continue
            throughput_source[number_of_cores]['Duration in ms'].append(
                result.duration_in_ms
            )
            throughput_source[number_of_cores]['Number of bytes'].append(
                result.number_of_bytes
            )
            throughput_source[number_of_cores]['Number of engines'].append(
                result.number_of_engines
            )


if __name__ == "__main__":
    get_time_n_tasks_source()
    # results = get_benchmark_results()
    # with open("saved_results.pkl", "wb") as saved_results:
    #     pickle.dump(get_benchmark_results(), saved_results)
