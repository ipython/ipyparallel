import ipyparallel as ipp
import timeit
import time
import numpy as np

delay = [0]
engines = [2, 8, 64, 128]
byte_param = [10, 100, 1000, 10_000, 100_000, 1_000_000]

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
        param_names = ['delay', 'Number of engines', 'Number of bytes']
        timer = timeit.default_timer
        timeout = 300
        params = [delay, engines, byte_param]

        view = None
        client = None
        reply = None

        def setup(self, *args):
            self.client = ipp.Client(profile='asv', cluster_id='depth_3')
            self.view = get_view(self)
            wait_for(lambda: len(self.client) >= max(engines))

        def time_broadcast(self, delay, engines, number_of_bytes):
            self.reply = self.view.apply_sync(
                echo(delay),
                np.array([0] * number_of_bytes, dtype=np.int8),
                targets=slice(engines),
            )

        def teardown(self, *args):
            replies_key = tuple(args)
            if replies_key in apply_replies:
                if any(
                    not np.array_equal(new_reply, stored_reply)
                    for new_reply, stored_reply in zip(
                        self.reply, apply_replies[replies_key]
                    )
                ):
                    raise ArrayNotEqual(benchmark_name, args)
            else:
                apply_replies[replies_key] = self.reply

            if self.client:
                self.client.close()

    return ThroughputSuite

#
# class DirectViewBroadCast(
#     make_benchmark(
#         'DirectViewBroadcast', lambda benchmark: benchmark.client.direct_view()
#     )
# ):
#     pass
#
#
# class CoalescingBroadcast(
#     make_benchmark(
#         'CoalescingBroadcast',
#         lambda benchmark: benchmark.client.broadcast_view(is_coalescing=True),
#     )
# ):
#     pass
#
#
# class NonCoalescingBroadcast(
#     make_benchmark(
#         'NonCoalescingBroadcast',
#         lambda benchmark: benchmark.client.broadcast_view(is_coalescing=False),
#     )
# ):
#     pass
#
#
# class DepthTestingSuite:
#     param_names = ['Number of engines', 'Number of bytes', 'is_coalescing', 'depth']
#     timer = timeit.default_timer
#     timeout = 300
#     params = [engines, [1000, 10_000, 100_000], [True, False], [1, 3]]
#
#     view = None
#     client = None
#     reply = None
#
#     def setup(self, number_of_engines, number_of_bytes, is_coalescing, depth):
#         self.client = ipp.Client(profile='asv', cluster_id=f'depth_{depth}')
#         self.view = self.client.broadcast_view(is_coalescing=is_coalescing)
#
#         wait_for(lambda: len(self.client) >= max(engines))
#
#     def time_broadcast(self, number_of_engines, number_of_bytes, *args):
#         self.reply = self.view.apply_sync(
#             echo(0),
#             np.array([0] * number_of_bytes, dtype=np.int8),
#             targets=slice(number_of_engines),
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


def make_multiple_message_benchmark(get_view):
    class AsyncMessagesSuite:
        param_names = ['Number of engines', 'Number of bytes', 'number_of_messages']
        timer = timeit.default_timer
        timeout = 300
        params = [engines, [1000, 10_000], [1, 10, 100]]

        view = None
        client = None
        reply = None

        def setup(self, number_of_engines, number_of_bytes, number_of_messages):
            self.client = ipp.Client(profile='asv', cluster_id=f'depth_3')
            self.view = get_view(self)

            wait_for(lambda: len(self.client) >= max(engines))

        def time_async_messages(
            self, number_of_engines, number_of_bytes, number_of_messages
        ):
            replies = []
            number_of_messages_to_send = number_of_messages // number_of_engines
            if number_of_messages_to_send < 1:
                number_of_messages_to_send = 1
            for i in range(number_of_messages_to_send):
                reply = self.view.apply(
                    echo(0),
                    np.array([0] * number_of_bytes, dtype=np.int8),
                    targets=slice(number_of_engines),
                    block=False,
                )
                replies.append(reply)
            for reply in replies:
                reply.get()

        def teardown(self, *args):
            if self.client:
                self.client.close()

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
