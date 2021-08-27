# coding: utf-8
"""Some generic utilities for dealing with classes, urls, and serialization."""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import asyncio
import functools
import inspect
import logging
import os
import re
import shlex
import socket
import stat
import sys
import warnings
from datetime import datetime
from datetime import timezone
from functools import lru_cache
from signal import SIGABRT
from signal import SIGINT
from signal import signal
from signal import SIGTERM
from types import FunctionType

import traitlets
import zmq
from dateutil.parser import parse as dateutil_parse
from dateutil.tz import tzlocal
from IPython import get_ipython
from IPython.core.profiledir import ProfileDir
from IPython.core.profiledir import ProfileDirError
from IPython.paths import get_ipython_dir
from jupyter_client.localinterfaces import is_public_ip
from jupyter_client.localinterfaces import localhost
from jupyter_client.localinterfaces import public_ips
from tornado.ioloop import IOLoop
from traitlets.log import get_logger
from zmq.log import handlers

utc = timezone.utc


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


class Namespace(dict):
    """Subclass of dict for attribute access to keys."""

    def __getattr__(self, key):
        """getattr aliased to getitem"""
        if key in self:
            return self[key]
        else:
            raise NameError(key)

    def __setattr__(self, key, value):
        """setattr aliased to setitem, with strict"""
        if hasattr(dict, key):
            raise KeyError("Cannot override dict keys %r" % key)
        self[key] = value


class ReverseDict(dict):
    """simple double-keyed subset of dict methods."""

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self._reverse = dict()
        for key, value in self.items():
            self._reverse[value] = key

    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self._reverse[key]

    def __setitem__(self, key, value):
        if key in self._reverse:
            raise KeyError("Can't have key %r on both sides!" % key)
        dict.__setitem__(self, key, value)
        self._reverse[value] = key

    def pop(self, key):
        value = dict.pop(self, key)
        self._reverse.pop(value)
        return value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------


def log_errors(f):
    """decorator to log unhandled exceptions raised in a method.

    For use wrapping on_recv callbacks, so that exceptions
    do not cause the stream to be closed.
    """

    @functools.wraps(f)
    def logs_errors(self, *args, **kwargs):
        try:
            result = f(self, *args, **kwargs)
        except Exception as e:
            self.log.exception(f"Uncaught exception in {f}: {e}")
            return

        if inspect.isawaitable(result):
            # if it's async, schedule logging for when the future resolves
            future = asyncio.ensure_future(result)

            def _log_error(future):
                if future.exception():
                    self.log.error(f"Uncaught exception in {f}: {future.exception()}")

            future.add_done_callback(_log_error)

    return logs_errors


def is_url(url):
    """boolean check for whether a string is a zmq url"""
    if '://' not in url:
        return False
    proto, addr = url.split('://', 1)
    if proto.lower() not in ['tcp', 'pgm', 'epgm', 'ipc', 'inproc']:
        return False
    return True


def validate_url(url):
    """validate a url for zeromq"""
    if not isinstance(url, str):
        raise TypeError("url must be a string, not %r" % type(url))
    url = url.lower()

    proto_addr = url.split('://')
    assert len(proto_addr) == 2, 'Invalid url: %r' % url
    proto, addr = proto_addr
    assert proto in ['tcp', 'pgm', 'epgm', 'ipc', 'inproc'], (
        "Invalid protocol: %r" % proto
    )

    # domain pattern adapted from http://www.regexlib.com/REDetails.aspx?regexp_id=391
    # author: Remi Sabourin
    pat = re.compile(
        r'^([\w\d]([\w\d\-]{0,61}[\w\d])?\.)*[\w\d]([\w\d\-]{0,61}[\w\d])?$'
    )

    if proto == 'tcp':
        lis = addr.split(':')
        assert len(lis) == 2, 'Invalid url: %r' % url
        addr, s_port = lis
        try:
            port = int(s_port)
        except ValueError:
            raise AssertionError("Invalid port %r in url: %r" % (port, url))

        assert addr == '*' or pat.match(addr) is not None, 'Invalid url: %r' % url

    else:
        # only validate tcp urls currently
        pass

    return True


def validate_url_container(container):
    """validate a potentially nested collection of urls."""
    if isinstance(container, str):
        url = container
        return validate_url(url)
    elif isinstance(container, dict):
        container = container.values()

    for element in container:
        validate_url_container(element)


def split_url(url):
    """split a zmq url (tcp://ip:port) into ('tcp','ip','port')."""
    proto_addr = url.split('://')
    assert len(proto_addr) == 2, 'Invalid url: %r' % url
    proto, addr = proto_addr
    lis = addr.split(':')
    assert len(lis) == 2, 'Invalid url: %r' % url
    addr, s_port = lis
    return proto, addr, s_port


def is_ip(location):
    """Is a location an ip?

    It could be a hostname.
    """
    return bool(re.match(location, r'(\d+\.){3}\d+'))


@lru_cache()
def ip_for_host(host):
    """Get the ip address for a host

    If no ips can be found for the host,
    the host is returned unmodified.
    """
    try:
        return socket.gethostbyname_ex(host)[2][0]
    except Exception as e:
        warnings.warn(
            "IPython could not determine IPs for %s: %s" % (host, e), RuntimeWarning
        )
        return host


def disambiguate_ip_address(ip, location=None):
    """turn multi-ip interfaces '0.0.0.0' and '*' into a connectable address

    Explicit IP addresses are returned unmodified.

    Parameters
    ----------
    ip : IP address
        An IP address, or the special values 0.0.0.0, or *
    location : IP address or hostname, optional
        A public IP of the target machine, or its hostname.
        If location is an IP of the current machine,
        localhost will be returned,
        otherwise location will be returned.
    """
    if ip in {'0.0.0.0', '*'}:
        if not location:
            # unspecified location, localhost is the only choice
            return localhost()
        elif not is_ip(location):
            if location == socket.gethostname():
                # hostname matches, use localhost
                return localhost()
            else:
                # hostname doesn't match, but the machine can have a few names.
                location = ip_for_host(location)

        if is_public_ip(location):
            # location is a public IP on this machine, use localhost
            ip = localhost()
        elif not public_ips():
            # this machine's public IPs cannot be determined,
            # assume `location` is not this machine
            warnings.warn("IPython could not determine public IPs", RuntimeWarning)
            ip = location
        else:
            # location is not this machine, do not use loopback
            ip = location
    return ip


def disambiguate_url(url, location=None):
    """turn multi-ip interfaces '0.0.0.0' and '*' into connectable
    ones, based on the location (default interpretation is localhost).

    This is for zeromq urls, such as ``tcp://*:10101``.
    """
    try:
        proto, ip, port = split_url(url)
    except AssertionError:
        # probably not tcp url; could be ipc, etc.
        return url

    ip = disambiguate_ip_address(ip, location)

    return "%s://%s:%s" % (proto, ip, port)


# --------------------------------------------------------------------------
# helpers for implementing old MEC API via view.apply
# --------------------------------------------------------------------------


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


def _push(**ns):
    """helper method for implementing `client.push` via `client.apply`"""
    user_ns = get_ipython().user_global_ns
    tmp = '_IP_PUSH_TMP_'
    while tmp in user_ns:
        tmp = tmp + '_'
    try:
        for name, value in ns.items():
            user_ns[tmp] = value
            exec("%s = %s" % (name, tmp), user_ns)
    finally:
        user_ns.pop(tmp, None)


def _pull(keys):
    """helper method for implementing `client.pull` via `client.apply`"""
    user_ns = get_ipython().user_global_ns
    if isinstance(keys, (list, tuple, set)):
        return [eval(key, user_ns) for key in keys]
    else:
        return eval(keys, user_ns)


def _execute(code):
    """helper method for implementing `client.execute` via `client.apply`"""
    user_ns = get_ipython().user_global_ns
    exec(code, user_ns)


# --------------------------------------------------------------------------
# extra process management utilities
# --------------------------------------------------------------------------

_random_ports = set()


def select_random_ports(n):
    """Selects and return n random ports that are available."""
    ports = []
    for i in range(n):
        sock = socket.socket()
        sock.bind(('', 0))
        while sock.getsockname()[1] in _random_ports:
            sock.close()
            sock = socket.socket()
            sock.bind(('', 0))
        ports.append(sock)
    for i, sock in enumerate(ports):
        port = sock.getsockname()[1]
        sock.close()
        ports[i] = port
        _random_ports.add(port)
    return ports


def signal_children(children):
    """Relay interupt/term signals to children, for more solid process cleanup."""

    def terminate_children(sig, frame):
        log = get_logger()
        log.critical("Got signal %i, terminating children..." % sig)
        for child in children:
            child.terminate()

        sys.exit(sig != SIGINT)
        # sys.exit(sig)

    for sig in (SIGINT, SIGABRT, SIGTERM):
        signal(sig, terminate_children)


def generate_exec_key(keyfile):
    import uuid

    newkey = str(uuid.uuid4())
    with open(keyfile, 'w') as f:
        # f.write('ipython-key ')
        f.write(newkey + '\n')
    # set user-only RW permissions (0600)
    # this will have no effect on Windows
    os.chmod(keyfile, stat.S_IRUSR | stat.S_IWUSR)


def integer_loglevel(loglevel):
    try:
        loglevel = int(loglevel)
    except ValueError:
        if isinstance(loglevel, str):
            loglevel = getattr(logging, loglevel)
    return loglevel


def connect_logger(logname, context, iface, root="ip", loglevel=logging.DEBUG):
    logger = logging.getLogger(logname)
    if any([isinstance(h, handlers.PUBHandler) for h in logger.handlers]):
        # don't add a second PUBHandler
        return
    loglevel = integer_loglevel(loglevel)
    lsock = context.socket(zmq.PUB)
    lsock.connect(iface)
    handler = handlers.PUBHandler(lsock)
    handler.setLevel(loglevel)
    handler.root_topic = root
    logger.addHandler(handler)
    logger.setLevel(loglevel)
    return logger


def connect_engine_logger(context, iface, engine, loglevel=logging.DEBUG):
    from ipyparallel.engine.log import EnginePUBHandler

    logger = logging.getLogger()
    if any([isinstance(h, handlers.PUBHandler) for h in logger.handlers]):
        # don't add a second PUBHandler
        return
    loglevel = integer_loglevel(loglevel)
    lsock = context.socket(zmq.PUB)
    lsock.connect(iface)
    handler = EnginePUBHandler(engine, lsock)
    handler.setLevel(loglevel)
    logger.addHandler(handler)
    logger.setLevel(loglevel)
    return logger


def local_logger(logname, loglevel=logging.DEBUG):
    loglevel = integer_loglevel(loglevel)
    logger = logging.getLogger(logname)
    if any([isinstance(h, logging.StreamHandler) for h in logger.handlers]):
        # don't add a second StreamHandler
        return
    handler = logging.StreamHandler()
    handler.setLevel(loglevel)
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs).03d [%(levelname)1.1s %(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(loglevel)
    return logger


def set_hwm(sock, hwm=0):
    """set zmq High Water Mark on a socket

    in a way that always works for various pyzmq / libzmq versions.
    """
    import zmq

    for key in ('HWM', 'SNDHWM', 'RCVHWM'):
        opt = getattr(zmq, key, None)
        if opt is None:
            continue
        try:
            sock.setsockopt(opt, hwm)
        except zmq.ZMQError:
            pass


def int_keys(dikt):
    """Rekey a dict that has been forced to cast number keys to str for JSON

    where there should be ints.
    """
    for k in list(dikt):
        if isinstance(k, str):
            nk = None
            try:
                nk = int(k)
            except ValueError:
                try:
                    nk = float(k)
                except ValueError:
                    continue
            if nk in dikt:
                raise KeyError("already have key %r" % nk)
            dikt[nk] = dikt.pop(k)
    return dikt


def become_dask_worker(address, nanny=False, **kwargs):
    """Task function for becoming a dask.distributed Worker

    Parameters
    ----------
    address : str
        The URL of the dask Scheduler.
    **kwargs
        Any additional keyword arguments will be passed to the Worker constructor.
    """
    shell = get_ipython()
    kernel = shell.kernel
    if getattr(kernel, 'dask_worker', None) is not None:
        kernel.log.info("Dask worker is already running.")
        return
    from distributed import Worker, Nanny

    if nanny:
        w = Nanny(address, **kwargs)
    else:
        w = Worker(address, **kwargs)
    shell.user_ns['dask_worker'] = shell.user_ns[
        'distributed_worker'
    ] = kernel.distributed_worker = w
    kernel.io_loop.add_callback(w.start)


def stop_distributed_worker():
    """Task function for stopping the the distributed worker on an engine."""
    shell = get_ipython()
    kernel = shell.kernel
    if getattr(kernel, 'distributed_worker', None) is None:
        kernel.log.info("Distributed worker already stopped.")
        return
    w = kernel.distributed_worker
    kernel.distributed_worker = None
    if shell.user_ns.get('distributed_worker', None) is w:
        shell.user_ns.pop('distributed_worker', None)
    IOLoop.current().add_callback(lambda: w.terminate(None))


def ensure_timezone(dt):
    """Ensure a datetime object has a timezone

    If it doesn't have one, attach the local timezone.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tzlocal())
    else:
        return dt


# extract_dates forward-port from jupyter_client 5.0
# timestamp formats
ISO8601 = "%Y-%m-%dT%H:%M:%S.%f"
ISO8601_PAT = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d{1,6})?(Z|([\+\-]\d{2}:?\d{2}))?$"
)


def _ensure_tzinfo(dt):
    """Ensure a datetime object has tzinfo

    If no tzinfo is present, add tzlocal
    """
    if not dt.tzinfo:
        # No more naïve datetime objects!
        warnings.warn(
            u"Interpreting naïve datetime as local %s. Please add timezone info to timestamps."
            % dt,
            DeprecationWarning,
            stacklevel=4,
        )
        dt = dt.replace(tzinfo=tzlocal())
    return dt


def _parse_date(s):
    """parse an ISO8601 date string

    If it is None or not a valid ISO8601 timestamp,
    it will be returned unmodified.
    Otherwise, it will return a datetime object.
    """
    if s is None:
        return s
    m = ISO8601_PAT.match(s)
    if m:
        dt = dateutil_parse(s)
        return _ensure_tzinfo(dt)
    return s


def extract_dates(obj):
    """extract ISO8601 dates from unpacked JSON"""
    if isinstance(obj, dict):
        new_obj = {}  # don't clobber
        for k, v in obj.items():
            new_obj[k] = extract_dates(v)
        obj = new_obj
    elif isinstance(obj, (list, tuple)):
        obj = [extract_dates(o) for o in obj]
    elif isinstance(obj, str):
        obj = _parse_date(obj)
    return obj


def compare_datetimes(a, b):
    """Compare two datetime objects

    If one has a timezone and the other doesn't,
    treat the naïve datetime as local time to avoid errors.

    Returns the timedelta
    """
    if a.tzinfo is None and b.tzinfo is not None:
        a = a.replace(tzinfo=tzlocal())
    elif a.tzinfo is not None and b.tzinfo is None:
        b = b.replace(tzinfo=tzlocal())
    return a - b


def utcnow():
    """Timezone-aware UTC timestamp"""
    return datetime.utcnow().replace(tzinfo=utc)


def _patch_jupyter_client_dates():
    """Monkeypatch jupyter_client.extract_dates to be nondestructive wrt timezone info"""
    import jupyter_client
    from distutils.version import LooseVersion as V

    if V(jupyter_client.__version__) < V('5.0'):
        from jupyter_client import session

        if hasattr(session, '_save_extract_dates'):
            return
        session._save_extract_dates = session.extract_dates
        session.extract_dates = extract_dates


# FIXME: remove patch when we require jupyter_client 5.0
_patch_jupyter_client_dates()


def progress(*args, widget=None, **kwargs):
    """Create a tqdm progress bar

    If `widget` is None, autodetects if IPython widgets should be used,
    otherwise use basic tqdm.
    """
    if widget is None:
        # auto widget if in a kernel
        ip = get_ipython()
        if ip is not None and getattr(ip, 'kernel', None) is not None:
            try:
                import ipywidgets  # noqa
            except ImportError:
                widget = False
            else:
                widget = True
        else:
            widget = False
    if widget:
        import tqdm.notebook

        f = tqdm.notebook.tqdm_notebook
    else:
        import tqdm

        kwargs.setdefault("file", sys.stdout)
        f = tqdm.tqdm
    return f(*args, **kwargs)


def abbreviate_profile_dir(profile_dir):
    """Abbreviate IPython profile directory if in $IPYTHONDIR"""
    profile_prefix = os.path.join(get_ipython_dir(), "profile_")
    if profile_dir.startswith(profile_prefix):
        # use just the profile name if it's in $IPYTHONDIR
        return profile_dir[len(profile_prefix) :]
    else:
        return profile_dir


def _all_profile_dirs():
    """List all IPython profile directories"""
    profile_dirs = []
    with os.scandir(get_ipython_dir()) as paths:
        for path in paths:
            if path.is_dir() and path.name.startswith('profile_'):
                profile_dirs.append(path.path)
    return profile_dirs


def _default_profile_dir(profile=None):
    """Locate the default IPython profile directory

    Priorities:

    - named profile, if specified
    - current IPython profile, if run inside IPython
    - $IPYTHONDIR/profile_default

    Returns absolute profile directory path,
    ensuring it exists
    """
    if not profile:
        ip = get_ipython()
        if ip is not None:
            return ip.profile_dir.location
    ipython_dir = get_ipython_dir()
    profile = profile or 'default'
    try:
        pd = ProfileDir.find_profile_dir_by_name(ipython_dir, name=profile)
    except ProfileDirError:
        pd = ProfileDir.create_profile_dir_by_name(ipython_dir, name=profile)
    return pd.location


def _locate_profiles(profiles=None):
    """Locate one or more IPython profiles by name"""
    ipython_dir = get_ipython_dir()
    return [
        ProfileDir.find_profile_dir_by_name(ipython_dir, name=profile).location
        for profile in profiles
    ]


def shlex_join(cmd):
    """Backport shlex.join to Python < 3.8"""
    return ' '.join(shlex.quote(s) for s in cmd)


_traitlet_annotations = {
    traitlets.Bool: bool,
    traitlets.Integer: int,
    traitlets.Float: float,
    traitlets.List: list,
    traitlets.Dict: dict,
    traitlets.Set: set,
    traitlets.Unicode: str,
    traitlets.Tuple: tuple,
}


class _TraitAnnotation:
    """Trait annotation for a trait type"""

    def __init__(self, trait_type):
        self.trait_type = trait_type

    def __repr__(self):
        return self.trait_type.__name__


def _trait_annotation(trait_type):
    """Return an annotation for a trait"""
    if trait_type in _traitlet_annotations:
        return _traitlet_annotations[trait_type]
    else:
        annotation = _traitlet_annotations[trait_type] = _TraitAnnotation(trait_type)
        return annotation


def _traitlet_signature(cls):
    """Add traitlet-based signature to a class"""
    parameters = []
    for name, trait in cls.class_traits().items():
        if name.startswith("_"):
            # omit private traits
            continue
        if hasattr(trait, 'default'):
            # traitlets 5
            default = trait.default()
        else:
            default = trait.default_value
        if default is traitlets.Undefined:
            default = None

        annotation = _trait_annotation(trait.__class__)

        parameters.append(
            inspect.Parameter(
                name=name,
                kind=inspect.Parameter.KEYWORD_ONLY,
                annotation=annotation,
                default=default,
            )
        )
    cls.__signature__ = inspect.Signature(parameters)
    return cls
