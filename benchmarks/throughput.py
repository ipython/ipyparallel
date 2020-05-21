import ipyparallel as ipp
import timeit
import time
import numpy as np

delay = [0]
engines = [2, 8, 16, 32, 64, 128, 256, 512]
byte_param = [1000, 10_000, 100_000, 1_000_000, 2_000_000]

apply_replies = {}


class ArrayNotEqual(Exception):
    pass


def wait_for(condition):
    for _ in range(750):
        if condition():
            break
        else:
            time.sleep(0.1)
    if not condition():
        raise TimeoutError('wait_for took to long to finish')


def echo(delay=0):
    def inner_echo(x, **kwargs):
        import time

        if delay:
            time.sleep(delay)
        return x

    return inner_echo


def make_benchmark(benchmark_name, get_view):
    class ThroughputSuite:
        param_names = ['Number of engines', 'Number of bytes']
        timer = timeit.default_timer
        timeout = 120
        params = [engines, byte_param]
        processes = 1
        view = None
        client = None
        reply = None

        def setup(self, number_of_engines, number_of_bytes):
            if not self.client:
                self.client = ipp.Client(profile='asv')
            self.view = get_view(self)
            self.view.targets = list(range(number_of_engines))
            wait_for(lambda: len(self.client) >= number_of_engines)

        def time_broadcast(self, delay, engines, number_of_bytes):
            self.reply = self.view.apply_sync(
                echo(delay), np.array([0] * number_of_bytes, dtype=np.int8)
            )


    return ThroughputSuite


class DirectViewBroadcast(
    make_benchmark(
        'DirectViewBroadcast', lambda benchmark: benchmark.client.direct_view()
    )
):
    pass


class CoalescingBroadcast(
    make_benchmark(
        'CoalescingBroadcast',
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=True),
    )
):
    pass


class NonCoalescingBroadcast(
    make_benchmark(
        'NonCoalescingBroadcast',
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=False),
    )
):
    pass


#
# class DepthTestingSuite:
#     param_names = ['Number of engines', 'is_coalescing', 'depth']
#     timer = timeit.default_timer
#     timeout = 60
#     params = [engines, [True, False], [0, 3]]
#
#     view = None
#     client = None
#     reply = None
#
#     def setup(self, number_of_engines, is_coalescing, depth):
#         self.client = ipp.Client(profile='asv', cluster_id=f'depth_{depth}')
#         self.view = self.client.broadcast_view(is_coalescing=is_coalescing)
#         self.view.targets = list(range(number_of_engines))
#
#         wait_for(lambda: len(self.client) >= number_of_engines)
#
#     def time_broadcast(self, number_of_engines,  *args):
#         self.reply = self.view.apply_sync(
#             echo(0),
#             np.array([0] * 1000, dtype=np.int8),
#         )
#
#     def teardown(self, *args):
#         replies_key = tuple(args)
#         if replies_key in apply_replies:
#             if any(
#                 not np.array_equal(new_reply, stored_reply)
#                 for new_reply, stored_reply in zip(
#                     self.reply, apply_replies[replies_key]
#                 )
#             ):
#                 raise ArrayNotEqual('DepthTestingSuite', args)
#         if self.client:
#             self.client.close()
#
#
def make_multiple_message_benchmark(get_view):
    class AsyncMessagesSuite:
        param_names = ['Number of engines', 'number_of_messages']
        timer = timeit.default_timer
        timeout = 60
        params = [engines, [1, 5, 10, 20, 50, 75, 100]]
        processes = 1

        view = None
        client = None
        reply = None

        def setup(self, number_of_engines, number_of_messages):
            if not self.client:
                self.client = ipp.Client(profile='asv')
            self.view = get_view(self)
            self.view.targets = list(range(number_of_engines))

            wait_for(lambda: len(self.client) >= number_of_engines)

        def time_async_messages(self, number_of_engines, number_of_messages):
            replies = []
            for i in range(number_of_messages):
                reply = self.view.apply_async(
                    echo(0), np.array([0] * 1000, dtype=np.int8)
                )
                replies.append(reply)
            for reply in replies:
                reply.get()

    return AsyncMessagesSuite


class DirectViewAsync(
    make_multiple_message_benchmark(lambda benchmark: benchmark.client.direct_view())
):
    pass


class CoalescingAsync(
    make_multiple_message_benchmark(
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=True)
    )
):
    pass


class NonCoalescingAsync(
    make_multiple_message_benchmark(
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=False)
    )
):
    pass


def make_push_benchmark(get_view):
    class PushMessageSuite:
        param_names = ['Number of engines', 'Number of bytes']
        timer = timeit.default_timer
        timeout = 120
        params = [engines, byte_param]
        processes = 1

        view = None
        client = None

        def setup(self, number_of_engines, number_of_bytes):
            if not self.client:
                self.client = ipp.Client(profile='asv')
            self.view = get_view(self)
            self.view.targets = list(range(number_of_engines))
            wait_for(lambda: len(self.client) >= number_of_engines)

        def time_broadcast(self, engines, number_of_bytes):
            reply = self.view.apply_sync(
                lambda x: None, np.array([0] * number_of_bytes, dtype=np.int8)
            )


    return PushMessageSuite



class DirectViewPush(
    make_push_benchmark(lambda benchmark: benchmark.client.direct_view())
):
    pass


class CoalescingPush(
    make_push_benchmark(
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=True)
    )
):
    pass


class NonCoalescingPush(
    make_push_benchmark(
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=False)
    )
):
    pass
