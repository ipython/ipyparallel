"""Utilities to enable code objects to be pickled.

Any process that import this module will be able to pickle code objects.  This
includes the func_code attribute of any function.  Once unpickled, new
functions can be built using new.function(code, globals()).  Eventually
we need to automate all of this so that functions themselves can be pickled.

Reference: A. Tremols, P Cogolo, "Python Cookbook," p 302-305
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import copyreg
import inspect
import sys
import types


def code_ctor(*args):
    return types.CodeType(*args)


# map CodeType constructor args to code co_ attribute names
# (they _almost_ all match, and new ones probably will)
_code_attr_map = {
    "codestring": "code",
    "constants": "consts",
}
# pass every supported arg to the code constructor
# this should be more forward-compatible
# (broken on pypy: https://github.com/ipython/ipyparallel/issues/845)
if sys.version_info >= (3, 10) and not hasattr(sys, "pypy_version_info"):
    _code_attr_names = tuple(
        _code_attr_map.get(name, name)
        for name, param in inspect.signature(types.CodeType).parameters.items()
        if param.POSITIONAL_ONLY or param.POSITIONAL_OR_KEYWORD
    )
else:
    # can't inspect types.CodeType on Python < 3.10
    _code_attr_names = [
        "argcount",
        "kwonlyargcount",
        "nlocals",
        "stacksize",
        "flags",
        "code",
        "consts",
        "names",
        "varnames",
        "filename",
        "name",
        "firstlineno",
        "lnotab",
        "freevars",
        "cellvars",
    ]
    if hasattr(types.CodeType, "co_posonlyargcount"):
        _code_attr_names.insert(1, "posonlyargcount")

    _code_attr_names = tuple(_code_attr_names)


def reduce_code(obj):
    """codeobject reducer"""
    return code_ctor, tuple(getattr(obj, f'co_{name}') for name in _code_attr_names)


copyreg.pickle(types.CodeType, reduce_code)
