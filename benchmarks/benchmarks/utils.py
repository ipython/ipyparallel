import datetime
import time
from typing import Callable


def wait_for(condition: Callable):
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


def get_time_stamp() -> str:
    return (
        str(datetime.datetime.now()).split(".")[0].replace(" ", "-").replace(":", "-")
    )
