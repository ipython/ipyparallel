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
    TIME_N_TASKS_NO_DELAY_NON_BLOCKING = 'non_blocking'
    TIME_N_TASKS_NO_DELAY = 'NoDelay'


def get_benchmark_type(benchmark_name):
    if BenchmarkType.ECHO_MANY_ARGUMENTS.value in benchmark_name:
        return BenchmarkType.ECHO_MANY_ARGUMENTS
    elif BenchmarkType.TIME_N_TASKS_NO_DELAY_NON_BLOCKING.value in benchmark_name:
        return BenchmarkType.TIME_N_TASKS_NO_DELAY_NON_BLOCKING
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
        self.results_file_name = benchmark_results_file_name
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
            or benchmark_type is BenchmarkType.TIME_N_TASKS_NO_DELAY_NON_BLOCKING
            or benchmark_type is BenchmarkType.TIME_N_TASKS
        ):
            self.number_of_tasks = int(param[0])
            self.number_of_engines = get_number_of_engines(benchmark_name)
            self.delay = (
                0
                if benchmark_type is BenchmarkType.TIME_N_TASKS_NO_DELAY
                or benchmark_type is BenchmarkType.TIME_N_TASKS_NO_DELAY_NON_BLOCKING
                else float(param[1])
            )
        elif benchmark_type is BenchmarkType.BROADCAST:
            self.number_of_bytes = int(param[1])
            self.number_of_engines = int(param[0])
        else:
            raise Exception('BenchmarkType not found in Result constructor')


def get_benchmark_results():
    results_for_machines = {
        dir_content: BenchmarkResult(
            os.path.join(os.getcwd(), RESULTS_DIR, dir_content, file_name)
        )
        for dir_content in os.listdir(RESULTS_DIR)
        if (
            os.path.isdir(os.path.join(os.getcwd(), RESULTS_DIR, dir_content))
            and "asv-testing" in dir_content
        )
        for file_name in os.listdir(f"{RESULTS_DIR}/{dir_content}")
        if "machine" not in file_name
    }

    sorted_results = {}
    for machine_name, result in results_for_machines.items():
        number_of_cores = get_number_of_cores(machine_name)
        if (
            number_of_cores not in sorted_results
            or sorted_results[number_of_cores].date < result.date
        ):
            sorted_results[number_of_cores] = result
    return {result.results_file_name: result for result in sorted_results.values()}


def get_value_dict(engines_or_cores='engines', bytes_or_tasks='tasks'):
    return {
        'Duration in ms': [],
        f'Number of {bytes_or_tasks}': [],
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


def add_no_delay_tasks_source(source, benchmark, number_of_cores):
    view_type = 'direct_view' if benchmark['is_direct_view'] else 'load_balanced'
    if benchmark['benchmark_type'] not in source:
        source[benchmark['benchmark_type']] = {
            view_type: get_value_dict(engines_or_cores='cores')
        }
    if view_type not in source[benchmark['benchmark_type']]:
        source[benchmark['benchmark_type']][view_type] = get_value_dict(
            engines_or_cores='cores'
        )
    dict_to_append_to = source[benchmark['benchmark_type']][view_type]

    for duration, tasks_num in [
        (result.duration_in_ms, result.number_of_tasks)
        for result in benchmark['results']
        if not result.failed
    ]:

        dict_to_append_to['Duration in ms'].append(duration)
        dict_to_append_to['Number of tasks'].append(tasks_num)
        dict_to_append_to['Number of cores'].append(number_of_cores)


def add_to_broadcast_source(source, benchmark, number_of_cores):
    if number_of_cores not in source:
        source[number_of_cores] = get_value_dict(bytes_or_tasks='bytes')

    dict_to_append_to = source[number_of_cores]
    for duration, bytes, engines in [
        (result.duration_in_ms, result.number_of_bytes, result.number_of_engines)
        for result in benchmark['results']
        if not result.failed
    ]:
        dict_to_append_to['Duration in ms'].append(duration)
        dict_to_append_to['Number of bytes'].append(bytes)
        dict_to_append_to['Number of engines'].append(engines)


def add_to_echo_many_arguments_source(source, benchmark, number_of_cores):
    view_type = 'direct_view' if benchmark['is_direct_view'] else 'load_balanced'
    if 'number_of_engines' not in source and benchmark['results']:
        source['number_of_engines'] = benchmark['results'][0].number_of_engines

    if view_type not in source:
        source[view_type] = {
            'Duration in ms': [],
            'Number of arguments': [],
            'Number of cores': [],
        }

    for duration, arguments in (
        (result.duration_in_ms, result.number_of_arguments)
        for result in benchmark['results']
        if not result.failed
    ):
        source[view_type]['Duration in ms'].append(duration)
        source[view_type]['Number of arguments'].append(arguments)
        source[view_type]['Number of cores'].append(number_of_cores)


def get_number_of_cores(machine_name):
    return int(re.findall('\d+', machine_name)[0])


def make_source(benchmark_type, add_to_source_f, benchmark_results=None):
    if not benchmark_results:
        benchmark_results = get_benchmark_results()
    source = {}
    for machine_name, benchmark_run in benchmark_results.items():
        number_of_cores = get_number_of_cores(machine_name)
        for benchmark_name, benchmark in benchmark_run.results_dict.items():
            if (
                isinstance(benchmark_type, list)
                and benchmark['benchmark_type'] in benchmark_type
            ) or benchmark['benchmark_type'] == benchmark_type:
                add_to_source_f(source, benchmark, number_of_cores)
    return source


def get_time_n_tasks_source(benchmark_results=None):
    return make_source(
        BenchmarkType.TIME_N_TASKS, add_time_n_tasks_source, benchmark_results
    )


def get_no_delay_source(benchmark_results=None):
    return make_source(
        [
            BenchmarkType.TIME_N_TASKS_NO_DELAY,
            BenchmarkType.TIME_N_TASKS_NO_DELAY_NON_BLOCKING,
        ],
        add_no_delay_tasks_source,
        benchmark_results,
    )


def get_broad_cast_source(benchmark_results=None):
    return make_source(
        BenchmarkType.BROADCAST, add_to_broadcast_source, benchmark_results
    )


def get_echo_many_arguments_source(benchmark_results=None):
    return make_source(
        BenchmarkType.ECHO_MANY_ARGUMENTS,
        add_to_echo_many_arguments_source,
        benchmark_results,
    )


if __name__ == "__main__":
    results = get_benchmark_results()
    get_time_n_tasks_source()
    source = get_echo_many_arguments_source()
    with open("saved_results.pkl", "wb") as saved_results:
        pickle.dump(get_benchmark_results(), saved_results)
