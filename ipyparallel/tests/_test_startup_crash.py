import os
import time

time.sleep(int(os.environ.get("CRASH_DELAY") or "1"))
os._exit(1)
