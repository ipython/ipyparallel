#!/usr/bin/env python
"""
Commandline interface to OS independent shell commands

Currently the following command are supported:
* start:   starts a process into background and returns its pid
* running: check if a process with a given pid is running
* kill:    kill a process with a given pid
* mkdir:   creates directory recursively (no error if directory already exists)
* rmdir:   remove directory recursively
* exists:  checks if a file/directory exists
* remove:  removes a file
"""

import os
import sys
from argparse import ArgumentParser

from .cluster.shellcmd import ShellCommandReceive


def main():
    parser = ArgumentParser(
        description='Perform some standard shell command in a platform independent way'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='append command line parameters to \'sys_arg.txt\'',
    )
    subparsers = parser.add_subparsers(dest='cmd', help='sub-command help')

    # create the parser for the "a" command
    parser_start = subparsers.add_parser(
        'start', help='start a process into background'
    )
    parser_start.add_argument('--env', help='optional environment dictionary')
    parser_start.add_argument(
        '--output_file', help='optional output redirection (for stdout and stderr)'
    )
    parser_start.add_argument('start_cmd', nargs='+', help='command that help')

    parser_running = subparsers.add_parser(
        'running', help='check if a process is running'
    )
    parser_running.add_argument(
        'pid', type=int, help='pid of process that should be checked'
    )

    parser_kill = subparsers.add_parser('kill', help='kill a process')
    parser_kill.add_argument(
        'pid', type=int, help='pid of process that should be killed'
    )
    parser_kill.add_argument('--sig', type=int, help='signals to send')

    parser_mkdir = subparsers.add_parser('mkdir', help='create directory recursively')
    parser_mkdir.add_argument('path', help='directory path to be created')

    parser_rmdir = subparsers.add_parser('rmdir', help='remove directory recursively')
    parser_rmdir.add_argument('path', help='directory path to be removed')

    parser_exists = subparsers.add_parser(
        'exists', help='checks if a file/directory exists'
    )
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

    receiver = ShellCommandReceive()
    with receiver as r:
        recevier_method = getattr(r, f"cmd_{cmd}")
        recevier_method(**vars(args))


if __name__ == '__main__':
    main()
