# coding: utf-8
"""The IPython ZMQ-based parallel computing interface."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.


import os
import warnings

import zmq

from traitlets.config.configurable import MultipleInstanceError


from ._version import version_info, __version__
from .serialize import *
from .client.asyncresult import *
from .client.client import Client
from .client.remotefunction import *
from .client.view import *
from .controller.dependency import *
from .error import *
from .util import interactive

#-----------------------------------------------------------------------------
# Functions
#-----------------------------------------------------------------------------


def bind_kernel(**kwargs):
    """Bind an Engine's Kernel to be used as a full IPython kernel.
    
    This allows a running Engine to be used simultaneously as a full IPython kernel
    with the QtConsole or other frontends.
    
    This function returns immediately.
    """
    from ipykernel.kernelapp import IPKernelApp
    from ipyparallel.apps.ipengineapp import IPEngineApp
    
    # first check for IPKernelApp, in which case this should be a no-op
    # because there is already a bound kernel
    if IPKernelApp.initialized() and isinstance(IPKernelApp._instance, IPKernelApp):
        return
    
    if IPEngineApp.initialized():
        try:
            app = IPEngineApp.instance()
        except MultipleInstanceError:
            pass
        else:
            return app.bind_kernel(**kwargs)

    raise RuntimeError("bind_kernel be called from an IPEngineApp instance")


def register_joblib_backend(name='ipyparallel', make_default=False):
    """Register the default ipyparallel backend for joblib."""
    from .joblib import register
    return register(name=name, make_default=make_default)


# nbextension installation (requires notebook ≥ 4.2)
def _jupyter_server_extension_paths():
    return [{
        'module': 'ipyparallel.nbextension'
    }]

def _jupyter_nbextension_paths():
    return [{
        'section': 'tree',
        'src': 'nbextension/static',
        'dest': 'ipyparallel',
        'require': 'ipyparallel/main',
    }]

