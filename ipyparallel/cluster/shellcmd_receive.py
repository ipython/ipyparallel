#!/usr/bin/env python
"""
Receiver side of OS independent shell commands

For a list of supported commands see top of file shellcmd.py.

Important: The shellcmd concept also supports receiver code sending (useful for testing and developing)
which transfers this file to the 'other side'. However, this limits the imports to standard python
packages. Hence, DO NOT USE ANY ipyparallel IMPORTS IN THIS FILE!
"""

import logging
import os
import shutil
import sys
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from random import randint
from subprocess import DEVNULL, Popen


class ShellCommandReceiveBase(metaclass=ABCMeta):
    """
    Base class for receiving and performing shell commands in a platform independent form

    All supported shell commands have a cmd_ prefix. When adding new functions make sure that there is an
    equivalent in the ShellCommandSend class. When a command failed a non-zero exit code will be returned.
    Hence, the ShellCommandSend class always uses subprocess.check_output for assessing if the command was
    successful. Some command require information to be returned (cmd_exists, cmd_start, cmd_running) which
    is written to stdout in the following form: __<key>=<value>__

    This base class contains all platform independent code and commands. Only, cmd_start, cmd_running and
    cmd_kill needs to be overwritten in childern classes (see ShellCommandReceiveWindows and
    ShellCommandReceivePosix)
    """

    def _log(self, msg):
        if not self.log:
            return
        self.log.info(f"[id={self.ranid}] {msg}")

    def __init__(self, debugging=False, log=None):
        self.debugging = debugging
        self.log = None
        if log is None:
            log = os.getenv("SHELLCMD_LOG")
        if log:
            log_file = Path(log).expanduser()
            self.log = logging.getLogger("shellcmd")
            self.log.setLevel(logging.DEBUG if self.debugging else logging.INFO)
            self.log.addHandler(logging.FileHandler(log_file, mode="a"))

        self.ranid = None
        if self.log:
            self.ranid = randint(0, 999)
            self._log(f"{self.__class__.__name__} instance created")

    def close(self):
        # perform possible clean up actions (currently not required)
        if self.log:
            self._log("ShellCommandReceiveBase closed")

    def _prepare_cmd_start(self, start_cmd, env):
        if env:
            self._log(f"env={env!r}")
            if not isinstance(env, dict):
                raise TypeError(f"env must be a dict, got {env!r}")

            # update environment
            for key, value in env.items():
                if value is not None and value != '':
                    # set entry
                    os.environ[key] = str(value)
                else:
                    # unset entry if needed
                    if key in os.environ:
                        del os.environ[key]

        if isinstance(start_cmd, str):
            start_cmd = [start_cmd]

        if not all(isinstance(item, str) for item in start_cmd):
            raise TypeError(f"Only str in start_cmd allowed ({start_cmd!r})")

        return start_cmd

    @abstractmethod
    def cmd_start(self, cmd, env=None, output_file=None):
        pass

    @abstractmethod
    def cmd_running(self, pid):
        pass

    @abstractmethod
    def cmd_kill(self, pid, sig=None):
        pass

    def cmd_mkdir(self, path):
        self._log(f"Make directory '{path}'")
        os.makedirs(path, exist_ok=True)  # we allow that the directory already exists

    def cmd_rmdir(self, path):
        self._log(f"Remove directory '{path}'")
        shutil.rmtree(path)

    def cmd_exists(self, path):
        self._log(f"Check if path exists '{path}'")
        if os.path.exists(path):
            print("__exists=1__")
        else:
            print("__exists=0__")

    def cmd_remove(self, path):
        self._log(f"Remove file '{path}'")
        os.remove(path)


class ShellCommandReceiveWindows(ShellCommandReceiveBase):
    """Windows Implementation of ShellCommandReceive class"""

    def __init__(self, debugging=False, use_breakaway=True, log=None):
        super().__init__(debugging, log)
        self.use_breakaway = use_breakaway

    def cmd_start(self, cmd, env=None, output_file=None):
        start_cmd = self._prepare_cmd_start(cmd, env)

        # under windows we need to remove embracing double quotes
        for idx, p in enumerate(start_cmd):
            if p[0] == '"' and p[-1] == '"':
                start_cmd[idx] = p.strip('"')

        self._log(f"start_cmd={start_cmd}  (use_breakaway={self.use_breakaway})")

        from subprocess import CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_CONSOLE

        flags = 0
        if self.use_breakaway:
            flags |= CREATE_NEW_CONSOLE
            flags |= CREATE_BREAKAWAY_FROM_JOB

        pkwargs = {
            'close_fds': True,  # close stdin/stdout/stderr on child
            'creationflags': flags,
        }
        if output_file:
            fo = open(output_file, "w")
            pkwargs['stdout'] = fo
            pkwargs['stderr'] = fo
            pkwargs['stdin'] = DEVNULL

        self._log(f"Popen(**pkwargs={pkwargs})")
        p = Popen(start_cmd, **pkwargs)
        self._log(f"pid={p.pid}")

        print(f'__remote_pid={p.pid}__')
        sys.stdout.flush()
        if not self.use_breakaway:
            self._log("before wait")
            p.wait()
            self._log("after wait")

    def cmd_running(self, pid):
        self._log(f"Check if pid {pid} is running")

        # taken from https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
        import ctypes

        PROCESS_QUERY_INFROMATION = (
            0x1000  # if actually PROCESS_QUERY_LIMITED_INFORMATION
        )
        STILL_ACTIVE = 259
        processHandle = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_INFROMATION, 0, pid
        )
        if processHandle == 0:
            print('__running=0__')
        else:
            i = ctypes.c_int(0)
            pi = ctypes.pointer(i)
            if ctypes.windll.kernel32.GetExitCodeProcess(processHandle, pi) == 0:
                print('__running=0__')
            if i.value == STILL_ACTIVE:
                print('__running=1__')
            else:
                print('__running=0__')
            ctypes.windll.kernel32.CloseHandle(processHandle)

    def cmd_kill(self, pid, sig=None):
        self._log(f"Kill pid {pid} (signal={sig})")

        # os.kill doesn't work reliable under windows. also see
        # https://stackoverflow.com/questions/28551180/how-to-kill-subprocess-python-in-windows

        # solution using taskill
        # import subprocess
        # subprocess.call(['taskkill', '/F', '/T', '/PID',  str(pid)])  # /T kills all child processes as well

        # use windows api to kill process (doesn't kill children processes)
        # To kill all children process things are more complicated. see e.g.
        # http://mackeblog.blogspot.com/2012/05/killing-subprocesses-on-windows-in.html
        import ctypes

        PROCESS_TERMINATE = 0x0001
        kernel32 = ctypes.windll.kernel32
        processHandle = kernel32.OpenProcess(PROCESS_TERMINATE, 0, pid)
        if processHandle:
            kernel32.TerminateProcess(
                processHandle, 3
            )  # 3 is just an arbitrary exit code
            kernel32.CloseHandle(processHandle)


class ShellCommandReceivePosix(ShellCommandReceiveBase):
    """Posix implementation of the ShellCommandReceive class"""

    def cmd_start(self, cmd, env=None, output_file=None):
        start_cmd = self._prepare_cmd_start(cmd, env)

        fo = DEVNULL
        if output_file:
            fo = open(output_file, "w")

        p = Popen(
            start_cmd, start_new_session=True, stdout=fo, stderr=fo, stdin=DEVNULL
        )
        print(f'__remote_pid={p.pid}__')
        sys.stdout.flush()

    def cmd_running(self, pid):
        self._log(f"Check if pid {pid} is running")
        try:
            # use os.kill with signal 0 to check if process is still running
            os.kill(pid, 0)
            print('__running=1__')
        except OSError:
            print('__running=0__')

    def cmd_kill(self, pid, sig=None):
        self._log(f"Kill pid {pid} (signal={sig})")
        os.kill(pid, sig)


@contextmanager
def ShellCommandReceive(debugging=False, use_breakaway=True, log=None):
    """Generator returning the corresponding platform dependent ShellCommandReceive object (as Context Manager)"""
    if sys.platform.lower().startswith("win"):
        receiver = ShellCommandReceiveWindows(debugging, use_breakaway, log)
    else:
        receiver = ShellCommandReceivePosix(debugging, log)
    try:
        yield receiver
    finally:
        receiver.close()
