import time
from typing import Callable


def wait_for(condition: Callable):
    for _ in range(450):
        if condition():
            break
        else:
            time.sleep(.1)
    assert condition()