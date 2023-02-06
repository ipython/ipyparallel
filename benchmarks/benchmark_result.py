import json
import os
import pickle
import re
from datetime import datetime
from enum import Enum
from itertools import product

from utils import seconds_to_ms

from benchmarks.constants import DEFAULT_NUMBER_OF_ENGINES

RESULTS_DIR = "results"


class BenchmarkType(Enum):
    # ECHO_MANY_ARGUMENTS = 'EchoManyArguments'
    # TIME_N_TASKS = 'Engines'
    # TIME_N_TASKS_NO_DELAY_NON_BLOCKING = 'non_blocking'
    # TIME_N_TASKS_NO_DELAY = 'NoDelay'
    __order__ = 'PUSH TIME_ASYNC DEPTH_TESTING BROADCAST'
    PUSH = 'Push'
    TIME_ASYNC = 'Async'
    DEPTH_TESTING = 'DepthTesting'
    BROADCAST = 'Broadcast'


class SchedulerType(Enum):
    __order__ = 'DIRECT_VIEW LOAD_BALANCED BROADCAST_NON_COALESCING BROADCAST_COALESCING BROADCAST'
    DIRECT_VIEW = 'DirectView'
    LOAD_BALANCED = 'LoadBalanced'
    BROADCAST_NON_COALESCING = 'NonCoalescing'
    BROADCAST_COALESCING = 'Coalescing'
    BROADCAST = 'Depth'


def get_benchmark_type(benchmark_name):
    for benchmark_type in BenchmarkType:
        if benchmark_type.value.lower() in benchmark_name.lower():
            return benchmark_type
    else:
        raise NotImplementedError(
            f"Couldn't find matching benchmarkType for {benchmark_name} "
        )


def get_scheduler_type(benchmark_name):
    for scheduler_type in SchedulerType:
        if scheduler_type.value in benchmark_name:
            return scheduler_type


class BenchmarkResult:
    def __init__(self, benchmark_results_file_name):
        with open(benchmark_results_file_name) as results_file:
            results_data = json.load(results_file)

        self.results_file_name = benchmark_results_file_name
        self.date = datetime.fromtimestamp(
            results_data["date"] // 1000
        )  # Date is in ms
        self.machine_config = results_data["params"]
        self.results_dict = {
            benchmark_name: {
                'benchmark_type': get_benchmark_type(benchmark_name),
                'scheduler_type': get_scheduler_type(benchmark_name),
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
                            product(
                                result_data["params"][0],
                                result_data["params"][1],
                                result_data['params'][2],
                            )
                            if len(result_data["params"]) > 2
                            else product(
                                result_data['params'][0], result_data['params'][1]
                            )
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
        if (
            benchmark_type is BenchmarkType.BROADCAST
            or benchmark_type is BenchmarkType.PUSH
        ):
            self.number_of_engines = int(param[0])
            self.number_of_bytes = int(param[1])
        elif benchmark_type is BenchmarkType.TIME_ASYNC:
            self.number_of_engines = int(param[0])
            self.number_of_messages = int(param[1])
        elif benchmark_type is BenchmarkType.DEPTH_TESTING:
            self.number_of_engines = int(param[0])
            self.is_coalescing = True if param[1] == 'True' else False
            self.depth = int(param[2])
        else:
            raise NotImplementedError('BenchmarkType not found in Result constructor')


def get_benchmark_results():
    results_for_machines = {
        file_name: BenchmarkResult(
            os.path.join(os.getcwd(), RESULTS_DIR, dir_content, file_name)
        )
        for dir_content in os.listdir(RESULTS_DIR)
        if (
            os.path.isdir(os.path.join(os.getcwd(), RESULTS_DIR, dir_content))
            and "asv-testing-64-2020-04-28" in dir_content
        )
        for file_name in os.listdir(f"{RESULTS_DIR}/{dir_content}")
        if "machine" not in file_name
    }

    return results_for_machines


def get_value_dict(engines_or_cores='engines', bytes_or_tasks='tasks'):
    return {
        'Duration in ms': [],
        f'Number of {bytes_or_tasks}': [],
        f'Number of {engines_or_cores}': [],
    }


def ensure_time_n_tasks_source_structure(
    datasource, delay, number_of_cores, scheduler_type
):
    if delay not in datasource:
        datasource[delay] = {number_of_cores: {scheduler_type: get_value_dict()}}
    elif number_of_cores not in datasource[delay]:
        datasource[delay][number_of_cores] = {scheduler_type: get_value_dict()}
    elif scheduler_type not in datasource[delay][number_of_cores]:
        datasource[delay][number_of_cores][scheduler_type] = get_value_dict()


def add_time_n_tasks_source(source, benchmark, number_of_cores):
    number_of_engines = benchmark['engines']
    scheduler_type = benchmark['scheduler_type']
    for duration, tasks_num, delay in [
        (result.duration_in_ms, result.number_of_tasks, seconds_to_ms(result.delay))
        for result in benchmark['results']
        if not result.failed
    ]:
        ensure_time_n_tasks_source_structure(
            source, delay, number_of_cores, scheduler_type
        )
        dict_to_append_to = source[delay][number_of_cores][scheduler_type]
        dict_to_append_to['Duration in ms'].append(duration)
        dict_to_append_to['Number of tasks'].append(tasks_num)
        dict_to_append_to['Number of engines'].append(number_of_engines)


def add_no_delay_tasks_source(source, benchmark, number_of_cores):
    scheduler_type = benchmark['scheduler_type']
    if benchmark['benchmark_type'] not in source:
        source[benchmark['benchmark_type']] = {
            scheduler_type: get_value_dict(engines_or_cores='cores')
        }
    if scheduler_type not in source[benchmark['benchmark_type']]:
        source[benchmark['benchmark_type']][scheduler_type] = get_value_dict(
            engines_or_cores='cores'
        )
    dict_to_append_to = source[benchmark['benchmark_type']][scheduler_type]

    for duration, tasks_num in [
        (result.duration_in_ms, result.number_of_tasks)
        for result in benchmark['results']
        if not result.failed
    ]:
        dict_to_append_to['Duration in ms'].append(duration)
        dict_to_append_to['Number of tasks'].append(tasks_num)
        dict_to_append_to['Number of cores'].append(number_of_cores)


def add_to_broadcast_source(source, benchmark):
    scheduler_type = benchmark['scheduler_type'].value

    if scheduler_type not in source:
        source[scheduler_type] = get_value_dict(bytes_or_tasks='bytes')

    for duration, bytes, engines in [
        (result.duration_in_ms, result.number_of_bytes, result.number_of_engines)
        for result in benchmark['results']
        if not result.failed
    ]:
        source[scheduler_type]['Duration in ms'].append(duration)
        source[scheduler_type]['Number of bytes'].append(bytes)
        source[scheduler_type]['Number of engines'].append(engines)


def add_to_async_source(source, benchmark):
    scheduler_type = benchmark['scheduler_type'].value

    if scheduler_type not in source:
        source[scheduler_type] = get_value_dict(bytes_or_tasks='messages')

    for duration, number_of_messages, engines in [
        (result.duration_in_ms, result.number_of_messages, result.number_of_engines)
        for result in benchmark['results']
        if not result.failed
    ]:
        source[scheduler_type]['Duration in ms'].append(duration)
        source[scheduler_type]['Number of messages'].append(number_of_messages)
        source[scheduler_type]['Number of engines'].append(engines)


def add_to_depth_testing_source(source, benchmark):
    scheduler_type = benchmark['scheduler_type'].value
    if scheduler_type not in source:
        source[scheduler_type] = {
            'Duration in ms': [],
            'Number of engines': [],
            'Is coalescing': [],
            'Depth': [],
        }
    for duration, number_of_engines, is_coalescing, depth in [
        (
            result.duration_in_ms,
            result.number_of_engines,
            result.is_coalescing,
            result.depth,
        )
        for result in benchmark['results']
        if not result.failed
    ]:
        source[scheduler_type]['Duration in ms'].append(duration)
        source[scheduler_type]['Number of engines'].append(number_of_engines)
        source[scheduler_type]['Is coalescing'].append(is_coalescing)
        source[scheduler_type]['Depth'].append(depth)


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
    return int(re.findall(r'\d+', machine_name)[0])


def make_source(benchmark_type, add_to_source_f, benchmark_results=None):
    if not benchmark_results:
        benchmark_results = get_benchmark_results()
    source = {}

    for benchmark_name, benchmark in benchmark_results['results_dict'].items():
        if (
            isinstance(benchmark_type, list)
            and benchmark['benchmark_type'] in benchmark_type
        ) or benchmark['benchmark_type'] == benchmark_type:
            add_to_source_f(source, benchmark)
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


def get_broadcast_source(benchmark_results=None):
    return make_source(
        BenchmarkType.BROADCAST, add_to_broadcast_source, benchmark_results
    )


def get_push_source(benchmark_results=None):
    return make_source(BenchmarkType.PUSH, add_to_broadcast_source, benchmark_results)


def get_async_source(benchmark_results):
    return make_source(BenchmarkType.TIME_ASYNC, add_to_async_source, benchmark_results)


def get_echo_many_arguments_source(benchmark_results=None):
    return make_source(
        BenchmarkType.ECHO_MANY_ARGUMENTS,
        add_to_echo_many_arguments_source,
        benchmark_results,
    )


def get_depth_testing_source(benchmark_results):
    return make_source(
        BenchmarkType.DEPTH_TESTING, add_to_depth_testing_source, benchmark_results
    )


if __name__ == "__main__":
    benchmark_results = get_benchmark_results()
    combined_results = {
        'date': benchmark_results['CoalescingBroadcast.json'].date,
        'machine_config': benchmark_results['CoalescingBroadcast.json'].machine_config,
        'results_dict': {},
    }
    for result in benchmark_results.values():
        for benchmark_name, benchmark_result in result.results_dict.items():
            combined_results['results_dict'][benchmark_name] = benchmark_result

    # source = get_broadcast_source(combined_results)
    # source = get_async_source(combined_results)
    # source = get_push_source(combined_results)
    # source = get_time_n_tasks_source(benchmark_results)
    # source = get_no_delay_source(benchmark_results)
    # source = get_echo_many_arguments_source()
    source = get_depth_testing_source(combined_results)
    with open("saved_results.pkl", "wb") as saved_results:
        pickle.dump(combined_results, saved_results)
