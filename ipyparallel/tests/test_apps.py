"""Test CLI application behavior"""
import sys
from subprocess import check_output

import nose.tools as nt

import ipyparallel


def _get_output(cmd):
    out = check_output([sys.executable, '-m', 'ipyparallel.cluster', '--version'])
    if isinstance(out, bytes):
        out = out.decode('utf8', 'replace')
    return out

def test_version():
    for submod in ['cluster', 'engine', 'controller']:
        out = _get_output([sys.executable, '-m', 'ipyparallel.%s' % submod, '--version'])
        assert out.strip() == ipyparallel.__version__
    