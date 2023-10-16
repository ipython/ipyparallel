"""test shell command classes"""

import pytest
import sys, os
import time, signal

from ipyparallel.cluster import shellcmd
from subprocess import run

@pytest.fixture
def setup_shellcmd_senders():

    senders = []    # each entry is tuple of the shellcmd object and the output file prefix (only needed for wsl to make sure )
    if os.name == 'nt':
        cmd = (shellcmd.ShellCommandSend(["cmd.exe"], ["/C"], sys.executable),  None)
        cmd_cs = (shellcmd.ShellCommandSend(["cmd.exe"], ["/C"], sys.executable, use_code_sending=1), None)
        ps = (shellcmd.ShellCommandSend(["powershell.exe"], ["-Command"], sys.executable), None)
        ps_cs = (shellcmd.ShellCommandSend(["powershell.exe"], ["-Command"], sys.executable, use_code_sending=1), None)
        ssh = (shellcmd.ShellCommandSend(["ssh"], ["-p", "2222", "ciuser@localhost"], "python", use_code_sending=1), None)
        if run(["where", "wsl"]).returncode == 0:
            #if wsl was found we can add a bash test as well (assuming that python3 is also installed)
            bash = (shellcmd.ShellCommandSend(["bash"], ["-c"], "python3", use_code_sending=1), "~/")   # use wsl to test with bash
        #senders = [cmd, cmd_cs, ps, ps_cs, ssh, bash]
        senders = [cmd_cs, ps_cs, ssh]
    else:
        # under linux we could also test more shells
        bash = (shellcmd.ShellCommandSend(["/usr/bin/bash"], ["-c"], "python3"), None)
        bash_cs = (shellcmd.ShellCommandSend(["/usr/bin/bash"], ["-c"], "python3", use_code_sending=1), None)
        ssh = (shellcmd.ShellCommandSend(["ssh"], ["-p", "2222", "ciuser@localhost"], "python3", use_code_sending=1), None)
        senders = [bash, bash_cs, ssh]
    return senders

@pytest.fixture
def shellcmd_test_cmd():
    """returns a command that runs for 5 seconds"""
    test_command = {}
    test_command["Windows"] = "ping -n 5 127.0.0.1"
    test_command["Linux"] = "ping -c 5 127.0.0.1"
    return test_command

def test_all_shellcmds(setup_shellcmd_senders, shellcmd_test_cmd):

    def read_via_shell(shell, filename):
        # small helper function to read a file via shell commands
        if shell.is_linux:
            content = shell.check_output(f"cat {filename}").strip()
        else:
            content = shell.check_output(f"type {filename}").strip()
        content = content.replace("\r\n", "\n") #correct line endings for windows
        return content.split("\n")

    # go through all senders for testing
    for sender, prefix in setup_shellcmd_senders:
        assert_prefix = f"shell={sender.shell[0]} (code sending={sender.use_code_sending})"

        info = sender.get_shell_info()
        assert len(info) == 2 and info[0] and info[1], f"{assert_prefix}: invalid shell info return ({info})"

        if sender.is_linux:
            test_cmd = shellcmd_test_cmd["Linux"]
        else:
            test_cmd = shellcmd_test_cmd["Windows"]

        python_ok = sender.check_python()
        assert python_ok is True, f"{assert_prefix}: python not found"

        test_dir = "shellcmd_test"
        test_file = "testfile.txt"

        # perform basic file/directory operations
        sender.cmd_mkdir(test_dir)
        assert sender.cmd_exists(test_dir) is True, f"{assert_prefix}: test directory '{test_dir}' was not created"

        # create a simple text file with one line (works on all platforms)
        fullpath = test_dir+'/'+test_file
        sender.check_output(f'echo "test-line" > {fullpath}')

        assert sender.cmd_exists(fullpath) is True, f"{assert_prefix}: test file '{fullpath}' was not created"

        sender.cmd_remove(fullpath)
        assert sender.cmd_exists(fullpath) is False, f"{assert_prefix}: test file '{fullpath}' was not removed"

        sender.cmd_rmdir(test_dir)
        assert sender.cmd_exists(test_dir) is False , f"{assert_prefix}: test directory '{test_dir}' was not removed"

        # do start operation test
        redirect_output_file = "output.txt"
        pid = sender.cmd_start(test_cmd, output_file=redirect_output_file)
        assert pid > 0 , f"{assert_prefix}: start command returned invalid pid={pid}"
        assert sender.cmd_running(pid) is True, f"{assert_prefix}: pid={pid} is not running"

        sender.cmd_kill(pid, signal.SIGTERM)

        assert sender.cmd_running(pid) is False, f"{assert_prefix}: pid={pid} is still running"
        assert sender.cmd_exists(redirect_output_file) is True, f"{assert_prefix}: output file '{redirect_output_file}' was not created"
        sender.cmd_remove(redirect_output_file)

        # do environment setting test
        # we have a dictionary of three environment entries, where the first one is empty
        env_dict = {"IPP_CLUSTER_ID": "",
                    "IPP_PROFILE_DIR": r"~/.ipython/profile_ssh",
                    "IPP_CONNECTION_INFO": {"ssh": "", "interface": "tcp://*", "registration": 60691, "control": 60692, "mux": 60693, "task": 60694, "iopub": 60695, "hb_ping": 60696, "hb_pong": 60697, "broadcast": [60698, 60699], "key": "169b682b-337c645951e7d47723061090", "curve_serverkey": "null", "location": "host", "pack": "json", "unpack": "json", "signature_scheme": "hmac-sha256"},
                    }
        # we use a small python script to output provided environment setting
        python_cmd = "import os;"
        python_cmd += "print('IPP_CLUSTER_ID set =', 'IPP_CLUSTER_ID' in os.environ);"
        python_cmd += "print('IPP_PROFILE_DIR =',os.environ['IPP_PROFILE_DIR']);"
        python_cmd += "print('IPP_CONNECTION_INFO =',os.environ['IPP_CONNECTION_INFO'])"

        output_file = "stdout.txt"
        if prefix:
            output_file = prefix + output_file
        pid = sender.cmd_start(f'{sender.python_path} -c "{python_cmd}"', env=env_dict, output_file=output_file)

        counter = 0
        max_counter = 5
        while sender.cmd_running(pid) or counter > max_counter:
            time.sleep(1)   #sleep for 1 second to make sure that command has finished
            counter += 1

        assert sender.cmd_running(pid) is False, f"{assert_prefix} env-test: pid={pid} is still running after {max_counter}s"

        output_lines = read_via_shell(sender, output_file)
        # check lines if they correspond to original dictionary (env_dict)
        assert len(output_lines) == 3, f"{assert_prefix}: env-test: 3 lines expected but {len(output_lines)} returned"
        assert output_lines[0] == 'IPP_CLUSTER_ID set = False'
        for line in output_lines[1:]:
            key, value = line.split(" = ")
            assert key in env_dict, f"{assert_prefix}: env-test: key '{key}' not in env_dict ({env_dict})"
            if isinstance(env_dict[key], dict): # convert to dictionary if needed
                value = eval(value)
            assert env_dict[key] == value, f"{assert_prefix}: env-test: values do not match '{env_dict[key]}' != '{value}'"



