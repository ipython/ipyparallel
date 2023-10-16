#!/usr/bin/env python
"""
Shell helper application for OS independent shell commands

Currently the following command are supported:
* start:   starts a process into background and returns its pid
* running: check if a process with a given pid is running
* kill:    kill a process with a given pid
* mkdir:   creates directory recursively (no error if directory already exists)
* rmdir:   remove directory recursively
* exists:  checks if a file/directory exists
* remove:  removes a file
"""

from subprocess import check_output, run, CalledProcessError
from argparse import ArgumentParser
import sys, re, os
import inspect
import shlex


class ShellCommandReceive:
    """
    Wrapper for receiving and performing ssh shell commands

    All supported shell commands have a cmd_ prefix. When adding new functions make sure that the is an
    equivalent in the ShellCommandSend class. When a command failed a non zero exit code will be returned.
    Hence, the ShellCommandSend class always uses subprocess.check_output for assessing if the command was
    successfull. Some command require information to be returned (cmd_exists, cmd_start, cmd_running) which
    is written to stdout in the following form: __<key>=<value>__
    """
    def __init__(self):
        self.debugging = False
        pass

    def cmd_start(self, start_cmd, env=None, output_file=None):
        if env:
            # add provided entries to environment
            if isinstance(env,str):
                env_dict = eval(env)
                assert(isinstance(env_dict, dict) is True)
                env = env_dict

            assert(isinstance(env, dict) is True)   # make sure that env is a dictionary

            # update environment
            for key, value in env.items():
                if value is not None and value != '':
                    os.environ[key] = str(value)
                else:
                    # unset entry if needed
                    if key in os.environ:
                        del os.environ[key]

        if self.debugging:
            with open("start_cmd.txt", "w") as f:
                f.write(start_cmd)

        if os.name == "nt":
            from subprocess import Popen
            from subprocess import CREATE_NEW_CONSOLE
            from subprocess import CREATE_BREAKAWAY_FROM_JOB

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

            p = Popen(start_cmd, **pkwargs)

            print(f'__remote_pid={p.pid}__')
        else:
            if output_file:
                nohup_start = f"exec nohup {start_cmd} >{output_file} 2>&1 </dev/null & echo __remote_pid=$!__"
            else:
                nohup_start = f"exec nohup {start_cmd} >/dev/null 2>&1 </dev/null & echo __remote_pid=$!__"
            os.system(nohup_start)


    def cmd_running(self, pid):
        if os.name == "nt":
            # taken from https://stackoverflow.com/questions/568271/how-to-check-if-there-exists-a-process-with-a-given-pid-in-python
            import ctypes
            PROCESS_QUERY_INFROMATION = 0x1000 # if actually PROCESS_QUERY_LIMITED_INFORMATION
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
        if os.name == "nt":
            # os.kill doesn't work reliable under windows. also see
            # https://stackoverflow.com/questions/28551180/how-to-kill-subprocess-python-in-windows

            # solution using taskill
            #import subprocess
            #subprocess.call(['taskkill', '/F', '/T', '/PID',  str(pid)])  # /T kills all child processes as well

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
        os.makedirs(path, exist_ok=True)    # we allow that the directory already exists

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
        Wrapper for sending shell commands in generic and OS independent form

        The class is designed to perform shell commands behind a ssh connection. Nevertheless, it can be
        used for send commands to different local shell as well, which is useful for testing. Since
        the concept uses this python package (class ShellCommandReceive) for performing the commands,
        it is necessary that a valid python installation (python_path) is provided. Calling the check
        commands this can be evaluated. Furthermore, get_shell_info can be called to retrieve OS and
        shell information independent of a valid python installation.

        The actual shell commands have a cmd_ prefix. When adding new functions make sure that the is an
        equivalent in the ShellCommandReceive class.

        The sender class supports two modes for performing the actual shell command. The standard way by starting
        the ipyparallel.cluster.shellcmd module requires the current code on the 'other side' of the shell
        available. When developing or debugging it is more convenient to activate the 'use_code_sending' flag. This
        transfers the ShellCommandReceive class directly to 'other side'. It should also be mentioned that the
        code sending option is much faster in executing the code.
    """
    package_name = "ipyparallel.cluster.shellcmd"    # package name for send the command
    output_template = re.compile(r"__([a-z][a-z0-9_]+)=([a-z0-9\-\.]+)__", re.IGNORECASE)
    receiver_code = inspect.getsource(ShellCommandReceive)
    _python_chars_map =  str.maketrans( {"\\": "\\\\", "'": "\\'"} )

    def __init__(self, shell, args, python_path, use_code_sending = False):
        self.shell = shell
        self.args = args
        self.python_path = python_path
        self.is_linux = None     # changed if get_remote_shell_info is called
        self.is_powershell = None
        self.join_params = True  # join all cmd params into a single param. does NOT work with windows cmd
        self.use_code_sending = use_code_sending    # should be activated when developing...
        self.debugging = False  # for outputs to file for easier debugging
        #if "ssh" in shell:
        #    self.join_params = False    #just for testing

    def _check_output(self, cmd):
        return check_output(cmd).decode('utf8', 'replace')

    def _runs_successful(self, cmd):
        try:
            check_output(cmd)
        except CalledProcessError as e:
            return False
        return True

    def _as_list(self, cmd):
        if isinstance(cmd, str):
            return [cmd]
        elif isinstance(cmd, list):
            return cmd
        else:
            raise Exception("Unknown command type")

    def _format_for_python(self, param):
        assert(isinstance(param, str) is True)
        if param.isnumeric():
            return param
        else:
            return f"'{param.translate(self._python_chars_map)}'"

    def _powershell_quote(self, param):
        if '"' in param or "'" in param or " " in param:
            # we need to replace single and double quotes be two double quotes, but if we are inside a string,
            # we need to prepend a backslash to the double quote. Otherwise it will get removed
            quoted = ""
            in_string = False
            for idx, c in enumerate(param):
                prev_c = None if idx == 0 else param[idx-1]
                next_c = None if idx == len(param)-1 else param[idx+1]
                if c == '"' and prev_c != "\\":
                    in_string = not in_string
                    quoted += '"'*2
                    continue
                if c == "'":
                    if in_string:
                        quoted += '\\"'*2
                    else:
                        quoted += '"'*2
                    continue
                quoted += c
            return "'"+quoted+"'"
        else:
            return param

    def _send_cmd(self, paramlist):
        if not self.use_code_sending:
            # send command through the corresponding package call
            if self.is_linux:
                paramlist = [shlex.quote(p) for p in paramlist]
            else:
                if self.is_powershell:
                    paramlist = [self._powershell_quote(p) for p in paramlist]
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
        else:
            # in code sending mode it is not required that the ipyparallel.cluster.shellcmd
            # exists (or is update to date) on the 'other' side of the shell. This is particular
            # useful when doing further development without copying the adapted file before each
            # test run
            py_cmd = f"import sys, os\n{self.receiver_code}\nShellCommandReceive().cmd_{paramlist[0]}("
            skip = False
            for idx in range(1, len(paramlist)):
                if skip:
                    skip = False
                    continue
                if idx > 1:
                    py_cmd += ", "
                if paramlist[idx][0:2] == "--":
                    py_cmd += f"{paramlist[idx][2:]}={self._format_for_python(paramlist[idx+1])}"
                    skip = True
                else:
                    py_cmd += self._format_for_python(paramlist[idx])
            py_cmd += ")\n"
            cmd = self.shell + self.args + [self.python_path]
            return check_output(cmd, universal_newlines=True, input=py_cmd)

    def get_shell_info(self):
        """
        get shell information by sending an echo command that works on all OS and shells

        :return: (str, str): string of system and shell
        """

        #  example outputs on
        #   windows-powershell: OS-WIN-CMD=%OS%;OS-WIN-PW=Windows_NT;OS-LINUX=;SHELL=
        #   windows-cmd       : "OS-WIN-CMD=Windows_NT;OS-WIN-PW=$env:OS;OS-LINUX=$OSTYPE;SHELL=$SHELL"
        #   ubuntu-bash       : OS-WIN-CMD=Windows_NT;OS-WIN-PW=:OS;OS-LINUX=linux-gnu;SHELL=/bin/bash
        #
        cmd = self.shell + self.args + ['echo "OS-WIN-CMD=%OS%;OS-WIN-PW=$env:OS;OS-LINUX=$OSTYPE;SHELL=$SHELL"']
        try:
            output = self._check_output(cmd)
        except CalledProcessError:
            raise Exception("Unable to get remote shell information. Are the ssh connection data correct?")
        entries = output.strip().strip('\\"').strip('"').split(";")
        # filter output non valid entries: contains =$ or =% or has no value assign (.....=)
        valid_entries = list(filter(lambda e: not ("=$" in e or "=%" in e or e[-1] == '='), entries))
        system = shell = None

        # currently we do not check if double entries are found
        for e in valid_entries:
            key, val = e.split("=")
            if key == "OS-WIN-CMD":
                system = val
                shell = "cmd.exe"
                self.is_powershell = False
                self.is_linux = False
                self.join_params = False    # disable joining, since it does not work for windows cmd.exe
            elif key == "OS-WIN-PW":
                system = val
                shell = "powershell.exe"
                self.is_powershell = True
                self.is_linux = False
            elif key == "OS-LINUX":
                system = val
                self.is_powershell = False
                self.is_linux = True
            elif key == "SHELL":
                shell = val

        return (system, shell)

    def check_python(self, python_path=None):
        """Check if remote python can be started"""
        if not python_path:
            python_path = self.python_path
        cmd = self.shell + self.args + [ python_path, '--version']
        return self._runs_successful(cmd)

    def check_ipython_package(self):
        """Check if ipython package is installed in the remote python installation"""
        cmd = self.shell + self.args + [self.python_path, "-m", "pip", "show", "ipython"]
        return self._runs_successful(cmd)

    def check_output(self, cmd):
        """generic subprocess.check_output call but using the ssh connection"""
        full_cmd = self.shell + self.args + self._as_list(cmd)
        return self._check_output(full_cmd)

    def check_output_python(self, cmd, contains_python_call=False):
        """generic subprocess.check_output python call using the ssh connection"""
        if not contains_python_call:
            fullcmd = self.shell + self.args + [self.python_path] + self._as_list(cmd)
        else:
            fullcmd = self.shell + self.args + self._as_list(cmd)
        return self._check_output(fullcmd)

    def cmd_start(self, cmd, env={}, output_file=None, log=None):
        """start cmd into background and return remote pid"""
        paramlist = ["start", cmd]
        if env and len(env) > 0:
            paramlist.extend(["--env", str(env)])
        if output_file:
            paramlist.extend(["--output_file", output_file])

        output = self._send_cmd(paramlist)
        # need to extract pid value
        values = dict(self.output_template.findall(output))
        if 'remote_pid' in values:
            return int(values['remote_pid'])
        else:
            raise RuntimeError(f"Failed to get pid for {cmd}: {output}")

    def cmd_running(self, pid):
        output = self._send_cmd(["running", str(pid)])
        # check output
        if "__running=1__" in output:
            return True
        elif "__running=0__" in output:
            return False
        else:
            raise Exception(f"Unexpected output ({output}) returned from by the running shell command")

    def cmd_kill(self, pid, sig=None):
        """kill remote process with the given pid"""
        if sig:
            self._send_cmd(["kill", str(pid), "--sig", str(int(sig))])
        else:
            self._send_cmd(["kill", str(pid)])

    def cmd_mkdir(self, p):
        """make directory recursively"""
        self._send_cmd(["mkdir", p])

    def cmd_rmdir(self, p):
        """remove directory recursively"""
        self._send_cmd(["rmdir", p])

    def cmd_exists(self, p):
        """check if file/path exists"""
        output = self._send_cmd(["exists", p])
        # check output
        if "__exists=1__" in output:
            return True
        elif "__exists=0__" in output:
            return False
        else:
            raise Exception(f"Unexpected output ({output}) returned from by the exists shell command")

    def cmd_remove(self, p):
        """delete remote file"""
        output = self._send_cmd(["remove", p])

def main():
    parser = ArgumentParser(description='Perform some standard shell command in a platform independent way')
    parser.add_argument('--debug', action='store_true', help='append command line parameters to \'sys_arg.txt\'')
    subparsers = parser.add_subparsers(dest='cmd', help='sub-command help')

    # create the parser for the "a" command
    parser_start = subparsers.add_parser('start', help='start a process into background')
    parser_start.add_argument('start_cmd', help='command that  help')
    parser_start.add_argument('--env', help='optional environment dictionary')
    parser_start.add_argument('--output_file', help='optional output redirection (for stdout and stderr)')

    parser_running = subparsers.add_parser('running', help='check if a process is running')
    parser_running.add_argument('pid', type=int, help='pid of process that should be checked')

    parser_kill = subparsers.add_parser('kill', help='kill a process')
    parser_kill.add_argument('pid', type=int, help='pid of process that should be killed')
    parser_kill.add_argument('--sig', type=int, help='signals to send')

    parser_mkdir = subparsers.add_parser('mkdir', help='create directory recursively')
    parser_mkdir.add_argument('path', help='directory path to be created')

    parser_rmdir = subparsers.add_parser('rmdir', help='remove directory recursively')
    parser_rmdir.add_argument('path', help='directory path to be removed')

    parser_exists = subparsers.add_parser('exists', help='checks if a file/directory exists')
    parser_exists.add_argument('path', help='path to check')

    parser_remove = subparsers.add_parser('remove', help='removes a file')
    parser_remove.add_argument('path', help='path to remove')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()
    cmd = args.__dict__.pop('cmd')

    if args.debug:
        with open("sys_arg.txt", "a") as f:
            f.write(f"'{__file__}' started in '{os.getcwd()}':\n")
            for idx, arg in enumerate(sys.argv[1:]):
                f.write(f"\t{idx}:{arg}$\n")
    del args.debug

    recevier = ShellCommandReceive()
    if cmd == "start":
        recevier.cmd_start(**vars(args))
    elif cmd == "running":
        recevier.cmd_running(**vars(args))
    elif cmd == "kill":
        recevier.cmd_kill(**vars(args))
    elif cmd == "mkdir":
        recevier.cmd_mkdir(**vars(args))
    elif cmd == "rmdir":
        recevier.cmd_rmdir(**vars(args))
    elif cmd == "exists":
        recevier.cmd_exists(**vars(args))
    elif cmd == "remove":
        recevier.cmd_remove(**vars(args))


if __name__ == '__main__':
    main()
