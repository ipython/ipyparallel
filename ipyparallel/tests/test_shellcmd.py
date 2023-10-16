"""test shell command tools"""
import pickle
from collections import namedtuple

import pytest
import sys, os
import time

from ipyparallel.cluster import shellcmd

@pytest.fixture
def setup_shellcmd_senders():
    senders = []
    if os.name == 'nt':
        cmd = shellcmd.ShellCommandSend(["cmd.exe"], ["/C"], sys.executable, use_code_sending=1)
        ps = shellcmd.ShellCommandSend(["powershell.exe"], ["-Command"], sys.executable)
        senders = [ps] #, cmd ps]
    return senders

@pytest.fixture
def shellcmd_test_cmd():
    """returns a command that runs for 5 seconds"""
    if os.name == 'nt':
        test_command ="ping -n 5 127.0.0.1"
    else:
        test_command ="ping -c 5 127.0.0.1"
    return test_command


def test_all_shellcmds(setup_shellcmd_senders, shellcmd_test_cmd):

    for sender in setup_shellcmd_senders:
        info = sender.get_shell_info()
        assert len(info) == 2 and info[0] and info[1]

        python_ok = sender.check_python()
        assert python_ok==True

        test_dir = "shellcmd_test"
        test_script = "testfile.txt"

        # perform basic file/directory operations
        sender.cmd_mkdir(test_dir)
        assert sender.cmd_exists(test_dir) is True  # make sure that test_dir was created

        # create a simple text file with one line
        fullpath = os.path.join(test_dir,test_script)
        with open(fullpath, "w") as f:
            f.write("test line\n")

        assert sender.cmd_exists(fullpath) is True  # make sure that test file was created

        sender.cmd_remove(fullpath)
        assert sender.cmd_exists(fullpath) is False  # make sure that test file was deleted

        sender.cmd_rmdir(test_dir)
        assert sender.cmd_exists(test_dir) is False  # make sure that test_dir was removed

        # do process operation test
        redirect_output_file="output.txt"
        pid = sender.cmd_start(shellcmd_test_cmd, output_file=redirect_output_file)
        assert pid > 0
        assert sender.cmd_running(pid) is True  # make sure that process with pid is running

        #time.sleep(7)  # ping should run for 5 seconds
        sender.cmd_kill(pid)

        assert sender.cmd_running(pid) is False  # make sure that process with pid is not running any more
        assert sender.cmd_exists(redirect_output_file) is True  # make sure that output file was created
        sender.cmd_remove(redirect_output_file)



