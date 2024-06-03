"""toplevel setup/teardown for parallel tests."""

import asyncio
import os
import time
from subprocess import Popen

from IPython.paths import get_ipython_dir

from ipyparallel import Client, error
from ipyparallel.cluster.launcher import (
    SIGKILL,
    LocalProcessLauncher,
    ProcessStateError,
    ipcontroller_cmd_argv,
    ipengine_cmd_argv,
)

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

# globals
launchers = []


# Launcher class
class TestProcessLauncher(LocalProcessLauncher):
    """subclass LocalProcessLauncher, to prevent extra sockets and threads being created on Windows"""

    def start(self):
        if self.state == 'before':
            # Store stdout & stderr to show with failing tests.
            # This is defined in IPython.testing.iptest
            self.process = Popen(self.args, env=os.environ, cwd=self.work_dir)
            self.notify_start(self.process.pid)
            self.poll = self.process.poll
        else:
            s = 'The process was already started and has state: %r' % self.state
            raise ProcessStateError(s)


# show tracebacks for RemoteErrors
class RemoteErrorWithTB(error.RemoteError):
    def __str__(self):
        s = super().__str__()
        return '\n'.join([s, self.traceback or ''])


# global setup/teardown


def setup():
    error.RemoteError = RemoteErrorWithTB

    cluster_dir = os.path.join(get_ipython_dir(), 'profile_iptest')
    engine_json = os.path.join(cluster_dir, 'security', 'ipcontroller-engine.json')
    client_json = os.path.join(cluster_dir, 'security', 'ipcontroller-client.json')
    for json in (engine_json, client_json):
        if os.path.exists(json):
            os.remove(json)

    cp = TestProcessLauncher()
    cp.cmd_and_args = ipcontroller_cmd_argv + [
        '--profile=iptest',
        '--log-level=10',
        '--ping=250',
        '--dictdb',
    ]
    if os.environ.get("IPP_CONTROLLER_IP"):
        cp.cmd_and_args.append(f"--ip={os.environ['IPP_CONTROLLER_IP']}")
    cp.start()
    launchers.append(cp)
    tic = time.time()
    while not os.path.exists(engine_json) or not os.path.exists(client_json):
        if cp.poll() is not None:
            raise RuntimeError("The test controller exited with status %s" % cp.poll())
        elif time.time() - tic > 15:
            raise RuntimeError("Timeout waiting for the test controller to start.")
        time.sleep(0.1)
    add_engines(1)


def add_engines(n=1, profile='iptest', total=False):
    """add a number of engines to a given profile.

    If total is True, then already running engines are counted, and only
    the additional engines necessary (if any) are started.
    """
    rc = Client(profile=profile)
    base = len(rc)

    if total:
        n = max(n - base, 0)

    eps = []
    for i in range(n):
        ep = TestProcessLauncher()
        ep.cmd_and_args = ipengine_cmd_argv + [
            '--profile=%s' % profile,
            '--InteractiveShell.colors=nocolor',
            '--log-level=10',
        ]
        ep.start()
        launchers.append(ep)
        eps.append(ep)
    tic = time.time()
    while len(rc) < base + n:
        if any([ep.poll() is not None for ep in eps]):
            raise RuntimeError("A test engine failed to start.")
        elif time.time() - tic > 15:
            raise RuntimeError("Timeout waiting for engines to connect.")
        time.sleep(0.1)
    rc.close()
    return eps


def teardown():
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        return
    while launchers:
        p = launchers.pop()
        if p.poll() is None:
            try:
                f = p.stop()
                if f:
                    asyncio.run(f)
            except Exception as e:
                print(e)
                pass
        if p.poll() is None:
            try:
                time.sleep(0.25)
            except KeyboardInterrupt:
                return
        if p.poll() is None:
            try:
                print('cleaning up test process...')
                p.signal(SIGKILL)
            except Exception:
                print("couldn't shutdown process: ", p)
