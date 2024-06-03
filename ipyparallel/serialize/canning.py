"""Pickle-related utilities.."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import copy
import functools
import pickle
import sys
from types import FunctionType

from traitlets import import_item
from traitlets.log import get_logger

from . import codeutil  # noqa This registers a hook when it's imported


def _get_cell_type(a=None):
    """the type of a closure cell doesn't seem to be importable,
    so just create one
    """

    def inner():
        return a

    return type(inner.__closure__[0])


cell_type = _get_cell_type()

# -------------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------------


def interactive(f):
    """decorator for making functions appear as interactively defined.
    This results in the function being linked to the user_ns as globals()
    instead of the module globals().
    """

    # build new FunctionType, so it can have the right globals
    # interactive functions never have closures, that's kind of the point
    if isinstance(f, FunctionType):
        mainmod = __import__('__main__')
        f = FunctionType(
            f.__code__,
            mainmod.__dict__,
            f.__name__,
            f.__defaults__,
        )
    # associate with __main__ for uncanning
    f.__module__ = '__main__'
    return f


def use_dill():
    """use dill to expand serialization support

    adds support for object methods and closures to serialization.
    """
    import dill

    from . import serialize

    serialize.pickle = dill

    # disable special function handling, let dill take care of it
    can_map.pop(FunctionType, None)


def use_cloudpickle():
    """use cloudpickle to expand serialization support

    adds support for object methods and closures to serialization.
    """
    import cloudpickle

    from . import serialize

    serialize.pickle = cloudpickle

    # disable special function handling, let cloudpickle take care of it
    can_map.pop(FunctionType, None)


def use_pickle():
    """revert to using stdlib pickle

    Reverts custom serialization enabled by use_dill|cloudpickle.
    """

    from . import serialize

    serialize.pickle = pickle

    # restore special function handling
    can_map[FunctionType] = _original_can_map[FunctionType]


# -------------------------------------------------------------------------------
# Classes
# -------------------------------------------------------------------------------


class CannedObject:
    def __init__(self, obj, keys=[], hook=None):
        """can an object for safe pickling

        Parameters
        ----------
        obj
            The object to be canned
        keys : list (optional)
            list of attribute names that will be explicitly canned / uncanned
        hook : callable (optional)
            An optional extra callable,
            which can do additional processing of the uncanned object.
        large data may be offloaded into the buffers list,
        used for zero-copy transfers.
        """
        self.keys = keys
        self.obj = copy.copy(obj)
        self.hook = can(hook)
        for key in keys:
            setattr(self.obj, key, can(getattr(obj, key)))

        self.buffers = []

    def get_object(self, g=None):
        if g is None:
            g = {}
        obj = self.obj
        for key in self.keys:
            setattr(obj, key, uncan(getattr(obj, key), g))

        if self.hook:
            self.hook = uncan(self.hook, g)
            self.hook(obj, g)
        return self.obj


class Reference(CannedObject):
    """object for wrapping a remote reference by name."""

    def __init__(self, name):
        if not isinstance(name, str):
            raise TypeError("illegal name: %r" % name)
        self.name = name
        self.buffers = []

    def __repr__(self):
        return "<Reference: %r>" % self.name

    def get_object(self, g=None):
        if g is None:
            g = {}

        return eval(self.name, g)


class CannedCell(CannedObject):
    """Can a closure cell"""

    def __init__(self, cell):
        self.cell_contents = can(cell.cell_contents)

    def get_object(self, g=None):
        cell_contents = uncan(self.cell_contents, g)

        def inner():
            return cell_contents

        return inner.__closure__[0]


class CannedFunction(CannedObject):
    def __init__(self, f):
        self._check_type(f)
        self.code = f.__code__
        if f.__defaults__:
            self.defaults = [can(fd) for fd in f.__defaults__]
        else:
            self.defaults = None

        if f.__kwdefaults__:
            self.kwdefaults = can_dict(f.__kwdefaults__)
        else:
            self.kwdefaults = None

        if f.__annotations__:
            self.annotations = can_dict(f.__annotations__)
        else:
            self.annotations = None

        closure = f.__closure__
        if closure:
            self.closure = tuple(can(cell) for cell in closure)
        else:
            self.closure = None

        self.module = f.__module__ or '__main__'
        self.__name__ = f.__name__
        self.buffers = []

    def _check_type(self, obj):
        assert isinstance(obj, FunctionType), "Not a function type"

    def get_object(self, g=None):
        # try to load function back into its module:
        if not self.module.startswith('__'):
            __import__(self.module)
            g = sys.modules[self.module].__dict__

        if g is None:
            g = {}
        if self.defaults:
            defaults = tuple(uncan(cfd, g) for cfd in self.defaults)
        else:
            defaults = None

        if self.kwdefaults:
            kwdefaults = uncan_dict(self.kwdefaults)
        else:
            kwdefaults = None
        if self.annotations:
            annotations = uncan_dict(self.annotations)
        else:
            annotations = {}

        if self.closure:
            closure = tuple(uncan(cell, g) for cell in self.closure)
        else:
            closure = None
        newFunc = FunctionType(self.code, g, self.__name__, defaults, closure)
        if kwdefaults:
            newFunc.__kwdefaults__ = kwdefaults
        if annotations:
            newFunc.__annotations__ = annotations
        return newFunc


class CannedPartial(CannedObject):
    def __init__(self, f):
        self._check_type(f)
        self.func = can(f.func)
        self.args = [can(a) for a in f.args]
        self.keywords = {k: can(v) for k, v in f.keywords.items()}
        self.buffers = []
        self.arg_buffer_counts = []
        self.keyword_buffer_counts = {}
        # consolidate buffers
        for canned_arg in self.args:
            if not isinstance(canned_arg, CannedObject):
                self.arg_buffer_counts.append(0)
                continue
            self.arg_buffer_counts.append(len(canned_arg.buffers))
            self.buffers.extend(canned_arg.buffers)
            canned_arg.buffers = []
        for key in sorted(self.keywords):
            canned_kwarg = self.keywords[key]
            if not isinstance(canned_kwarg, CannedObject):
                continue
            self.keyword_buffer_counts[key] = len(canned_kwarg.buffers)
            self.buffers.extend(canned_kwarg.buffers)
            canned_kwarg.buffers = []

    def _check_type(self, obj):
        if not isinstance(obj, functools.partial):
            raise ValueError("Not a functools.partial: %r" % obj)

    def get_object(self, g=None):
        if g is None:
            g = {}
        if self.buffers:
            # reconstitute buffers
            for canned_arg, buf_count in zip(self.args, self.arg_buffer_counts):
                if not buf_count:
                    continue
                canned_arg.buffers = self.buffers[:buf_count]
                self.buffers = self.buffers[buf_count:]
            for key in sorted(self.keyword_buffer_counts):
                buf_count = self.keyword_buffer_counts[key]
                canned_kwarg = self.keywords[key]
                canned_kwarg.buffers = self.buffers[:buf_count]
                self.buffers = self.buffers[buf_count:]
            assert len(self.buffers) == 0

        args = [uncan(a, g) for a in self.args]
        keywords = {k: uncan(v, g) for k, v in self.keywords.items()}
        func = uncan(self.func, g)
        return functools.partial(func, *args, **keywords)


class CannedClass(CannedObject):
    def __init__(self, cls):
        self._check_type(cls)
        self.name = cls.__name__
        self.old_style = not isinstance(cls, type)
        self._canned_dict = {}
        for k, v in cls.__dict__.items():
            if k not in ('__weakref__', '__dict__'):
                self._canned_dict[k] = can(v)
        if self.old_style:
            mro = []
        else:
            mro = cls.mro()

        self.parents = [can(c) for c in mro[1:]]
        self.buffers = []

    def _check_type(self, obj):
        assert isinstance(obj, type), "Not a class type"

    def get_object(self, g=None):
        parents = tuple(uncan(p, g) for p in self.parents)
        return type(self.name, parents, uncan_dict(self._canned_dict, g=g))


class CannedArray(CannedObject):
    def __init__(self, obj):
        from numpy import ascontiguousarray

        self.shape = obj.shape
        self.dtype = obj.dtype.descr if obj.dtype.fields else obj.dtype.str
        self.pickled = False
        if sum(obj.shape) == 0:
            self.pickled = True
        elif obj.dtype == 'O':
            # can't handle object dtype with buffer approach
            self.pickled = True
        elif obj.dtype.fields and any(
            dt == 'O' for dt, sz in obj.dtype.fields.values()
        ):
            self.pickled = True
        if self.pickled:
            # just pickle it
            from . import serialize

            self.buffers = [serialize.pickle.dumps(obj, serialize.PICKLE_PROTOCOL)]
        else:
            # ensure contiguous
            obj = ascontiguousarray(obj, dtype=None)
            self.buffers = [memoryview(obj)]

    def get_object(self, g=None):
        from numpy import frombuffer

        data = self.buffers[0]
        if self.pickled:
            from . import serialize

            # we just pickled it
            return serialize.pickle.loads(data)
        else:
            return frombuffer(data, dtype=self.dtype).reshape(self.shape)


class CannedBytes(CannedObject):
    def __init__(self, obj):
        self.buffers = [obj]

    def get_object(self, g=None):
        data = self.buffers[0]
        return self.wrap(data)

    @staticmethod
    def wrap(data):
        if isinstance(data, bytes):
            return data
        else:
            return memoryview(data).tobytes()


class CannedMemoryView(CannedBytes):
    wrap = memoryview


CannedBuffer = CannedMemoryView
# -------------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------------


def _import_mapping(mapping, original=None):
    """import any string-keys in a type mapping"""
    log = get_logger()
    log.debug("Importing canning map")
    for key, value in list(mapping.items()):
        if isinstance(key, str):
            try:
                cls = import_item(key)
            except Exception:
                if original and key not in original:
                    # only message on user-added classes
                    log.error("canning class not importable: %r", key, exc_info=True)
                mapping.pop(key)
            else:
                mapping[cls] = mapping.pop(key)


def istype(obj, check):
    """like isinstance(obj, check), but strict

    This won't catch subclasses.
    """
    if isinstance(check, tuple):
        for cls in check:
            if type(obj) is cls:
                return True
        return False
    else:
        return type(obj) is check


def can(obj):
    """prepare an object for pickling"""

    import_needed = False

    for cls, canner in can_map.items():
        if isinstance(cls, str):
            import_needed = True
            break
        elif istype(obj, cls):
            return canner(obj)

    if import_needed:
        # perform can_map imports, then try again
        # this will usually only happen once
        _import_mapping(can_map, _original_can_map)
        return can(obj)

    return obj


def can_class(obj):
    if isinstance(obj, type) and obj.__module__ == '__main__':
        return CannedClass(obj)
    else:
        return obj


def can_dict(obj):
    """can the *values* of a dict"""
    if istype(obj, dict):
        newobj = {}
        for k, v in obj.items():
            newobj[k] = can(v)
        return newobj
    else:
        return obj


sequence_types = (list, tuple, set)


def can_sequence(obj):
    """can the elements of a sequence"""
    if istype(obj, sequence_types):
        t = type(obj)
        return t([can(i) for i in obj])
    else:
        return obj


def uncan(obj, g=None):
    """invert canning"""

    import_needed = False
    for cls, uncanner in uncan_map.items():
        if isinstance(cls, str):
            import_needed = True
            break
        elif isinstance(obj, cls):
            return uncanner(obj, g)

    if import_needed:
        # perform uncan_map imports, then try again
        # this will usually only happen once
        _import_mapping(uncan_map, _original_uncan_map)
        return uncan(obj, g)

    return obj


def uncan_dict(obj, g=None):
    if istype(obj, dict):
        newobj = {}
        for k, v in obj.items():
            newobj[k] = uncan(v, g)
        return newobj
    else:
        return obj


def uncan_sequence(obj, g=None):
    if istype(obj, sequence_types):
        t = type(obj)
        return t([uncan(i, g) for i in obj])
    else:
        return obj


def _uncan_dependent_hook(dep, g=None):
    dep.check_dependency()


def can_dependent(obj):
    return CannedObject(obj, keys=('f', 'df'), hook=_uncan_dependent_hook)


# -------------------------------------------------------------------------------
# API dictionaries
# -------------------------------------------------------------------------------

# These dicts can be extended for custom serialization of new objects

can_map = {
    'numpy.ndarray': CannedArray,
    FunctionType: CannedFunction,
    functools.partial: CannedPartial,
    bytes: CannedBytes,
    memoryview: CannedMemoryView,
    cell_type: CannedCell,
    type: can_class,
    'ipyparallel.dependent': can_dependent,
}

uncan_map = {
    CannedObject: lambda obj, g: obj.get_object(g),
    dict: uncan_dict,
}

# for use in _import_mapping:
_original_can_map = can_map.copy()
_original_uncan_map = uncan_map.copy()
