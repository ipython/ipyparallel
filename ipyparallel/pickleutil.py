"""Pickle utilities for IPython parallel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from ipython_kernel.pickleutil import CannedObject, can_map
from .controller.dependency import dependent

def _uncan_dependent_hook(dep, g=None):
    dep.check_dependency()
    
def can_dependent(obj):
    return CannedObject(obj, keys=('f', 'df'), hook=_uncan_dependent_hook)

def enable():
    can_map[dependent] = can_dependent
