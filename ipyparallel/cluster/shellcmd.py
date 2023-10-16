#!/usr/bin/env python
"""
Shell helper application for OS independent shell commands

Currently, the following command are supported:
* start:   starts a process into background and returns its pid
* running: check if a process with a given pid is running
* kill:    kill a process with a given pid
* mkdir:   creates directory recursively (no error if directory already exists)
* rmdir:   remove directory recursively
* exists:  checks if a file/directory exists
* remove:  removes a file
"""
import json
import time
from subprocess import check_output, CalledProcessError, TimeoutExpired
import re, os
import inspect
import shlex
import base64


class ShellCommandReceive:
    """
    Helper class for receiving and performing shell commands in a platform independent form

    All supported shell commands have a cmd_ prefix. When adding new functions make sure that the is an
    equivalent in the ShellCommandSend class. When a command failed a non zero exit code will be returned.
    Hence, the ShellCommandSend class always uses subprocess.check_output for assessing if the command was
    successfull. Some command require information to be returned (cmd_exists, cmd_start, cmd_running) which
    is written to stdout in the following form: __<key>=<value>__
    """

    def __init__(self, debugging=False):
        self.debugging = debugging
        self.windows = True if os.name == 'nt' else False
        pass

    def _linux_quote(self, p):
        if "'" in p:
            return '"' + p + '"'
        else:
            return p

    def cmd_start(self, start_cmd, env=None, output_file=None):
        if env:
            # add provided entries to environment
            if isinstance(env, str):
                if env[0] == '"' and env[-1] == '"':
                    env = env.strip('"')  # occurs under windows cmd
                if "\\'" in env:
                    env = env.replace("\\'", "'")  # replace quoted
                env_dict = eval(env)
                assert (isinstance(env_dict, dict) is True)
                env = env_dict

            assert (isinstance(env, dict) is True)  # make sure that env is a dictionary

            # update environment
            for key, value in env.items():
                if value is not None and value != '':
                    if isinstance(value, dict):
                        os.environ[key] = json.dumps(value)
                    else:
                        os.environ[key] = str(value)
                else:
                    # unset entry if needed
                    if key in os.environ:
                        del os.environ[key]

        if isinstance(start_cmd, str):
            start_cmd = [start_cmd]

        if self.windows:
            # under windows we need to remove embracing double quotes
            for idx, p in enumerate(start_cmd):
                if not isinstance(p, str):
                    start_cmd[idx] = str(p)
                    continue
                if p[0] == '"' and p[-1] == '"':
                    start_cmd[idx] = p.strip('"')

        if self.debugging:
            with open("start_cmd.txt", "w") as f:
                f.write(str(start_cmd))

        if self.windows:
            from subprocess import Popen
            from subprocess import CREATE_NEW_CONSOLE
            from subprocess import CREATE_BREAKAWAY_FROM_JOB
            from subprocess import DEVNULL

            flags = 0
            flags |= CREATE_NEW_CONSOLE
            flags |= CREATE_BREAKAWAY_FROM_JOB

            pkwargs = {
                'close_fds': True,  # close stdin/stdout/stderr on child
                'creationflags': flags
            }
            if output_file:
                fo = open(output_file, "w")
                pkwargs['stdout'] = fo
                pkwargs['stderr'] = fo
                pkwargs['stdin'] = DEVNULL

            p = Popen(start_cmd, **pkwargs)

            print(f'__remote_pid={p.pid}__')
        else:
            start_cmd = [self._linux_quote(x) for x in start_cmd]

            if output_file:
                nohup_start = f"nohup {' '.join(start_cmd)} >{output_file} 2>&1 </dev/null & echo __remote_pid=$!__"
            else:
                nohup_start = f"nohup {' '.join(start_cmd)} >/dev/null 2>&1 </dev/null & echo __remote_pid=$!__"
            os.system(nohup_start)

    def cmd_running(self, pid):
        if self.windows:
            # taken from https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
            import ctypes
            PROCESS_QUERY_INFROMATION = 0x1000  # if actually PROCESS_QUERY_LIMITED_INFORMATION
            STILL_ACTIVE = 259
            processHandle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_INFROMATION, 0, pid)
            if processHandle == 0:
                print(f'__running=0__')
            else:
                i = ctypes.c_int(0)
                pi = ctypes.pointer(i)
                if ctypes.windll.kernel32.GetExitCodeProcess(processHandle, pi) == 0:
                    print(f'__running=0__')
                if i.value == STILL_ACTIVE:
                    print(f'__running=1__')
                else:
                    print(f'__running=0__')
                ctypes.windll.kernel32.CloseHandle(processHandle)
        else:
            ps_cmd = f'ps -p {pid} > /dev/null && echo "__running=1__" || echo "__running=0__"'
            os.system(ps_cmd)

    def cmd_kill(self, pid, sig=None):
        if self.windows:
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
            processHandle = kernel32.OpenProcess(PROCESS_TERMINATE, 0, pid);
            if processHandle:
                kernel32.TerminateProcess(processHandle, 3)  # 3 is just an arbitrary exit code
                kernel32.CloseHandle(processHandle)
        else:
            os.kill(pid, sig)

    def cmd_mkdir(self, path):
        os.makedirs(path, exist_ok=True)  # we allow that the directory already exists

    def cmd_rmdir(self, path):
        import shutil
        shutil.rmtree(path)

    def cmd_exists(self, path):
        if os.path.exists(path):
            print("__exists=1__")
        else:
            print("__exists=0__")

    def cmd_remove(self, path):
        os.remove(path)


class ShellCommandSend:
    """
        Helper class for sending shell commands in generic and OS independent form

        The class is designed to send shell commands to an ssh connection. Nevertheless, it can be
        used for send commands to different local shell as well, which is useful for testing. Since
        the concept uses the ipyparallel package (class ipyparallel.cluster.ShellCommandReceive) for
        executing the commands, it is necessary that a valid python installation (python_path) is provided.
        By calling has_python this can be evaluated. Furthermore, get_shell_info can be called to retrieve OS and
        shell information independent of a valid python installation.

        Since some operation require different handling for different platforms/shells the class gathers
        necessary information during initialization. This per default automatically at object instantiation.
        Nevertheless, it can be postponed (initialize=False) but before any member function is called, the
        initialize function has to be called.

        Beside generic check_output[...] functions (equivalent to subprocess.check_output), the class provides
        specific shell commands which have a cmd_ prefix. When adding new functions make sure that an
        equivalent is added in the ShellCommandReceive class as well.
    """
    package_name = "ipyparallel.shellcmd"  # package name for send the command
    output_template = re.compile(r"__([a-z][a-z0-9_]+)=([a-z0-9\-\.]+)__", re.IGNORECASE)
    receiver_code = inspect.getsource(ShellCommandReceive)
    _python_chars_map = str.maketrans({"\\": "\\\\", "'": "\\'"})

    def __init__(self, shell, args, python_path, initialize=True, send_receiver_class=False):
        self.shell = shell
        self.args = args
        self.python_path = python_path

        # shell dependent values. Those values are determined in the initialize function
        self.shell_info = None
        self.is_linux = None  # flag if shell is one a linux machine
        self.is_powershell = None  # flag if shell is windows powershell (requires special parameter quoting)
        self.join_params = True  # join all cmd params into a single param. does NOT work with windows cmd
        self.pathsep = "/"  # equivalent to os.pathsep (will be changed during initialization)

        self.send_receiver_class = send_receiver_class  # should be activated when developing...
        self.debugging = False  # for outputs to file for easier debugging

        if initialize:
            self.initialize()

    @staticmethod
    def _check_output(cmd, **kwargs):
        output = check_output(cmd, **kwargs)
        if isinstance(output,str):
            return output
        else:
            return output.decode('utf8', 'replace')

    @staticmethod
    def _runs_successful(cmd):
        try:
            check_output(cmd)
        except CalledProcessError as e:
            return False
        return True

    @staticmethod
    def _as_list(cmd):
        if isinstance(cmd, str):
            return [cmd]
        elif isinstance(cmd, list):
            return cmd
        else:
            raise Exception("Unknown command type")

    @staticmethod
    def _format_for_python(param):
        if isinstance(param, str):
            return f"'{param.translate(ShellCommandSend._python_chars_map)}'"
        else:
            return str(param)

    def _cmd_send_via_shell(self, paramlist):
        # unused/deprecated function which send a command as shell command rather than as python code (see _cmd_send)
        # since it requires proper quoting for different shells, it's much more sensitive and complex than sending
        # poor python code
        def _powershell_quote(param):
            if '"' in param or "'" in param or " " in param:
                # we need to replace single and double quotes be two double quotes, but if we are inside a string,
                # we need to prepend a backslash to the double quote. Otherwise it will get removed
                quoted = ""
                in_string = False
                for idx, c in enumerate(param):
                    prev_c = None if idx == 0 else param[idx - 1]
                    next_c = None if idx == len(param) - 1 else param[idx + 1]
                    if c == '"' and prev_c != "\\":
                        in_string = not in_string
                        quoted += '"' * 2
                        continue
                    if c == "'":
                        if in_string:
                            quoted += '\\"' * 2
                        else:
                            quoted += '"' * 2
                        continue
                    quoted += c
                return "'" + quoted + "'"
            else:
                return param
        def _cmd_quote(param):
            if "'" in param:
                tmp = param.strip()  # if already double quoted we do not need to quote
                if tmp[0] == '"' and tmp[-1] == '"':
                    return tmp
                else:
                    return '"' + tmp + '"'
            else:
                return param

        if self.is_linux:
            paramlist = [shlex.quote(p) for p in paramlist]
        else:
            if self.is_powershell:
                paramlist = [_powershell_quote(p) for p in paramlist]
            else:
                paramlist = [_cmd_quote(p) for p in paramlist]

        full_list = [self.python_path, "-m", self.package_name] + paramlist
        if self.join_params:
            cmd = self.shell + self.args + [" ".join(full_list)]
        else:
            cmd = self.shell + self.args + full_list

        if self.debugging:
            with open("send_cmd.txt", "w") as f:
                for idx, c in enumerate(cmd):
                    f.write(f"{idx}:{c}$\n")

        return self._check_output(cmd)

    def _cmd_send(self, cmd, *args, **kwargs):
        if not self.send_receiver_class:
            preamble = f"from ipyparallel.cluster.shellcmd import ShellCommandReceive\n"
        else:
            preamble = f"import sys, os, json\n{self.receiver_code}\n"

        # in send receiver mode it is not required that the ipyparallel.cluster.shellcmd
        # exists (or is update to date) on the 'other' side of the shell. This is particular
        # useful when doing further development without copying the adapted file before each
        # test run. Furthermore, the calls are much faster.
        debug_str = ""
        if self.debugging:
            debug_str = "debugging=True"

        py_cmd = f"{preamble}\nShellCommandReceive({debug_str}).cmd_{cmd}("
        assert len(args) == 1
        for a in args:
            py_cmd += self._format_for_python(a)
        if len(kwargs) > 0:
            py_cmd += ", "
        py_cmd += ", ".join(f'{k}={self._format_for_python(v)}' for k, v in kwargs.items())
        py_cmd += ")"
        cmd = self.shell + self.args + [self.python_path]
        return self._check_output(cmd, universal_newlines=True, input=py_cmd)

    def _get_pid(self, output):
        # need to extract pid value
        values = dict(self.output_template.findall(output))
        if 'remote_pid' in values:
            return int(values['remote_pid'])
        else:
            raise RuntimeError(f"Failed to get pid from output: {output}")

    def initialize(self):
        """initialize necessary variables by sending an echo command that works on all OS and shells"""
        if self.shell_info:
            return
        #  example outputs on
        #   windows-powershell: OS-WIN-CMD=%OS%;OS-WIN-PW=Windows_NT;OS-LINUX=;SHELL=
        #   windows-cmd       : "OS-WIN-CMD=Windows_NT;OS-WIN-PW=$env:OS;OS-LINUX=$OSTYPE;SHELL=$SHELL"
        #   ubuntu-bash       : OS-WIN-CMD=Windows_NT;OS-WIN-PW=:OS;OS-LINUX=linux-gnu;SHELL=/bin/bash
        #
        cmd = self.shell + self.args + ['echo "OS-WIN-CMD=%OS%;OS-WIN-PW=$env:OS;OS-LINUX=$OSTYPE;SHELL=$SHELL"']
        timeout = 10
        try:
            output = self._check_output(cmd, timeout=timeout)    # timeout for command is set to 10s
        except CalledProcessError as e:
            raise Exception("Unable to get remote shell information. Are the ssh connection data correct?")
        except TimeoutExpired as e:
            raise Exception(f"Timeout of {timeout}s reached while retrieving remote shell information. Are the ssh connection data correct?")

        entries = output.strip().strip('\\"').strip('"').split(";")
        # filter output non valid entries: contains =$ or =% or has no value assign (.....=)
        valid_entries = list(filter(lambda e: not ("=$" in e or "=%" in e or "=:" in e or e[-1] == '='), entries))
        system = shell = None

        # currently we do not check if double entries are found
        for e in valid_entries:
            key, val = e.split("=")
            if key == "OS-WIN-CMD":
                system = val
                shell = "cmd.exe"
                self.is_powershell = False
                self.is_linux = False
                self.join_params = False  # disable joining, since it does not work for windows cmd.exe
                self.pathsep = "\\"
            elif key == "OS-WIN-PW":
                system = val
                shell = "powershell.exe"
                self.is_powershell = True
                self.is_linux = False
                self.pathsep = "\\"
            elif key == "OS-LINUX":
                system = val
                self.is_powershell = False
                self.is_linux = True
            elif key == "SHELL":
                shell = val

        self.shell_info = (system, shell)

    def get_shell_info(self):
        """
        get shell information
        :return: (str, str): string of system and shell
        """
        assert self.shell_info  # make sure that initialize was called already
        return self.shell_info

    def has_python(self, python_path=None):
        """Check if remote python can be started ('python --version')
        :return: bool: flag if python start was found
        """
        assert self.shell_info  # make sure that initialize was called already
        if not python_path:
            python_path = self.python_path
        cmd = self.shell + self.args + [python_path, '--version']
        return self._runs_successful(cmd)

    def has_ipython_package(self):
        """Check if ipython package is installed in the remote python installation"""
        assert self.shell_info  # make sure that initialize was called already
        cmd = self.shell + self.args + [self.python_path, "-m", "pip", "show", "ipython"]
        return self._runs_successful(cmd)

    def check_output(self, cmd, **kwargs):
        """subprocess.check_output call using the shell connection
        :param cmd: command (str or list of strs) that should be executed
        :param kwargs: additional parameters that are passed to subprocess.check_output
        :return: output of executed command"""
        assert self.shell_info  # make sure that initialize was called already
        full_cmd = self.shell + self.args + self._as_list(cmd)
        return self._check_output(full_cmd, **kwargs)

    def check_output_python_module(self, module_params, **kwargs):
        """subprocess.check_output call based on python module call
        :param module_params: python module and parameters (str or list of strs) that should be executed
        :param kwargs: additional parameters that are passed to subprocess.check_output
        :return: output of executed command
        """
        assert self.shell_info  # make sure that initialize was called already
        cmd = self.shell + self.args + [self.python_path, "-m"] + self._as_list(module_params)
        return self._check_output(cmd, **kwargs)

    def check_output_python_code(self, python_code, **kwargs):
        """subprocess.check_output call running the provided python code
        :param python_code: code that should be executed
        :param kwargs: additional parameters that are passed to subprocess.check_output
        :return: output of executed command
        """
        assert self.shell_info  # make sure that initialize was called already
        assert "input" not in kwargs                # must not be specified
        assert "universal_newlines" not in kwargs   # must not be specified
        cmd = self.shell + self.args + [self.python_path]
        return self._check_output(cmd, universal_newlines=True, input=python_code, **kwargs)


    def cmd_start(self, cmd, env=None, output_file=None):
        """starts command into background and return remote pid
        :param cmd: command (str or list of strs) that should be started
        :param env: dictionary of environment variable that should be set before starting the process
        :param output_file: stdout and stderr will be redirected to the (remote) file
        :return: pid of started process
        """
        # join commands into a single parameter. otherwise
        assert self.shell_info  # make sure that initialize was called already
        if not self.is_linux:
            # for windows shells we need to split program and arguments into a list
            if isinstance(cmd, str):
                paramlist = shlex.split(cmd)
            else:
                paramlist = self._as_list(cmd)
        else:
            paramlist = self._as_list(cmd)

        return self._get_pid(self._cmd_send("start", paramlist, env=env, output_file=output_file))

    def cmd_start_python_module(self, module_params, env=None, output_file=None):
        """start python module into background and return remote pid
        :param module_params: python module and parameters (str or list of strs) that should be executed
        :param env: dictionary of environment variable that should be set before starting the process
        :param output_file: stdout and stderr will be redirected to the (remote) file
        :return: pid of started process
        """
        assert self.shell_info  # make sure that initialize was called already
        paramlist = [self.python_path, "-m"] + self._as_list(module_params)
        return self._get_pid(self._cmd_send("start", paramlist, env=env, output_file=output_file))

    def cmd_start_python_code(self, python_code, env=None, output_file=None):
        """start python with provided code into background and return remote pid
        :param python_code: code that should be executed
        :param env: dictionary of environment variable that should be set before starting the process
        :param output_file: stdout and stderr will be redirected to the (remote) file
        :return: pid of started process
        """
        assert self.shell_info  # make sure that initialize was called already
        # encoding the python code as base64 stream and decoding on the other side, remove any complicated
        # quoting strategy. We do not check the length of the code, which could exceed the shell parameter
        # string limit. Since the limit is typically > 2k, little code snippets should not cause any problems.
        encoded = base64.b64encode(python_code.encode())
        py_cmd = f'import base64;exec(base64.b64decode({encoded}).decode())'
        if not self.is_linux:
            py_cmd = '"'+py_cmd+'"'
        paramlist = [self.python_path, "-c", py_cmd]
        return self._get_pid(self._cmd_send("start", paramlist, env=env, output_file=output_file))

    def cmd_running(self, pid):
        """check if given (remote) pid is running"""
        assert self.shell_info  # make sure that initialize was called already
        output = self._cmd_send("running", pid)
        # check output
        if "__running=1__" in output:
            return True
        elif "__running=0__" in output:
            return False
        else:
            raise Exception(f"Unexpected output ({output}) returned from by the running shell command")

    def cmd_kill(self, pid, sig=None):
        """kill (remote) process with the given pid"""
        assert self.shell_info  # make sure that initialize was called already
        if sig:
            self._cmd_send("kill", pid, sig=int(sig))
        else:
            self._cmd_send("kill", pid)

    def cmd_mkdir(self, p):
        """make directory recursively"""
        assert self.shell_info  # make sure that initialize was called already
        self._cmd_send("mkdir", p)

    def cmd_rmdir(self, p):
        """remove directory recursively"""
        assert self.shell_info  # make sure that initialize was called already
        self._cmd_send("rmdir", p)

    def cmd_exists(self, p):
        """check if file/path exists"""
        assert self.shell_info  # make sure that initialize was called already
        output = self._cmd_send("exists", p)
        # check output
        if "__exists=1__" in output:
            return True
        elif "__exists=0__" in output:
            return False
        else:
            raise Exception(f"Unexpected output ({output}) returned from by the exists shell command")

    def cmd_remove(self, p):
        """delete remote file"""
        assert self.shell_info  # make sure that initialize was called already
        output = self._cmd_send("remove", p)

# test some test code, which can be removed later on
# import sys
# sender = ShellCommandSend(["cmd.exe"], ["/C"], sys.executable, send_receiver_class=1)
# sender = ShellCommandSend(["/usr/bin/bash"], ["-c"], sys.executable, send_receiver_class=1)
# pid = sender.cmd_start_python_code( "print('hallo johannes')", output_file="output.txt" )