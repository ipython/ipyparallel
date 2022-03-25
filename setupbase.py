# coding: utf-8

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

"""
This file originates from the 'jupyter-packaging' package, and
contains a set of useful utilities for including npm packages
within a Python package.
"""
from collections import defaultdict
from pathlib import Path
import io
import logging
import os
import functools
import pipes
import re
import shlex
import subprocess
import sys
from shutil import which
from typing import Callable, List, Optional, Tuple, Union

try:
    from deprecation import deprecated
except ImportError:
    # shim deprecated to allow setuptools to find the version string in this file
    deprecated = lambda *args, **kwargs: lambda *args, **kwargs: None

if Path("MANIFEST").exists():
    Path("MANIFEST").unlink()

from packaging.version import VERSION_PATTERN
from setuptools import Command
from setuptools.command.build_py import build_py

try:
    from setuptools.config import StaticModule
except ImportError:
    # setuptools>=61.0.0
    from setuptools.config.expand import StaticModule

from setuptools.command.sdist import sdist
from setuptools.command.develop import develop
from setuptools.command.bdist_egg import bdist_egg

try:
    from wheel.bdist_wheel import bdist_wheel
except ImportError:  # pragma: no cover
    bdist_wheel = None

if sys.platform == "win32":  # pragma: no cover
    from subprocess import list2cmdline
else:

    def list2cmdline(cmd_list):
        return " ".join(map(pipes.quote, cmd_list))


__version__ = "0.12.0"

# ---------------------------------------------------------------------------
# Top Level Variables
# ---------------------------------------------------------------------------

SEPARATORS = os.sep if os.altsep is None else os.sep + os.altsep
VERSION_REGEX = re.compile(
    r"^\s*" + VERSION_PATTERN + r"\s*$", re.VERBOSE | re.IGNORECASE
)


log = logging.getLogger(__name__)

if "--skip-npm" in sys.argv:
    print("Skipping npm install as requested.")
    skip_npm = True
    sys.argv.remove("--skip-npm")
else:
    skip_npm = False


# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------


def wrap_installers(
    pre_develop=None,
    pre_dist=None,
    post_develop=None,
    post_dist=None,
    ensured_targets=None,
    skip_if_exists=None,
):
    """Make a setuptools cmdclass that calls a prebuild function before installing.

    Parameters
    ----------
    pre_develop: function
        The function to call prior to the develop command.
    pre_dist: function
        The function to call prior to the sdist and wheel commands
    post_develop: function
        The function to call after the develop command.
    post_dist: function
        The function to call after the sdist and wheel commands.
    ensured_targets: list
        A list of local file paths that should exist when the dist commands are run
    skip_if_exists: list
        A list of local files whose presence causes the prebuild to skip

    Notes
    -----
    For any function given, creates a new `setuptools` command that can be run separately,
    e.g. `python setup.py pre_develop`.

    Returns
    -------
    A cmdclass dictionary for setup args.
    """
    cmdclass = {}

    def _make_command(name, func):
        class _Wrapped(BaseCommand):
            def run(self):
                func()

        _Wrapped.__name__ = name
        func.__name__ = name
        cmdclass[name] = _Wrapped

    for name in ["pre_develop", "post_develop", "pre_dist", "post_dist"]:
        if locals()[name]:
            _make_command(name, locals()[name])

    cmdclass["ensure_targets"] = ensure_targets(ensured_targets or [])

    skips = skip_if_exists or []
    should_skip = skips and all(Path(path).exists() for path in skips)

    def _make_wrapper(klass, pre_build, post_build):
        class _Wrapped(klass):
            def run(self):
                if pre_build and not should_skip:
                    self.run_command(pre_build.__name__)
                if klass != develop:
                    self.run_command("ensure_targets")
                klass.run(self)
                if post_build and not should_skip:
                    self.run_command(post_build.__name__)

        cmdclass[klass.__name__] = _Wrapped

    if pre_develop or post_develop:
        _make_wrapper(develop, pre_develop, post_develop)

    if pre_dist or post_dist or ensured_targets:
        _make_wrapper(sdist, pre_dist, post_dist)
        if bdist_wheel:
            _make_wrapper(bdist_wheel, pre_dist, post_dist)

    return cmdclass


def npm_builder(
    path=None, build_dir=None, source_dir=None, build_cmd="build", force=False, npm=None
):
    """Build function factory for managing an npm installation.

    Note: The function is a no-op if the `--skip-npm` cli flag is used.

    Parameters
    ----------
    path: str, optional
        The base path of the node package. Defaults to the current directory.
    build_dir: str, optional
        The target build directory.  If this and source_dir are given,
        the JavaScript will only be build if necessary.
    source_dir: str, optional
        The source code directory.
    build_cmd: str, optional
        The npm command to build assets to the build_dir.
    npm: str or list, optional.
        The npm executable name, or a tuple of ['node', executable].

    Returns
    -------
    A build function to use with `wrap_installers`
    """

    def builder():
        if skip_npm:
            log.info("Skipping npm-installation")
            return

        node_package = Path(path or Path.cwd().resolve())

        is_yarn = (node_package / "yarn.lock").exists()
        if is_yarn and not which("yarn"):
            log.warn("yarn not found, ignoring yarn.lock file")
            is_yarn = False

        npm_cmd = npm

        if npm is None:
            if is_yarn:
                npm_cmd = ["yarn"]
            else:
                npm_cmd = ["npm"]
        elif isinstance(npm, str):
            npm_cmd = [npm]

        if not which(npm_cmd[0]):
            log.error(
                "`{0}` unavailable.  If you're running this command "
                "using sudo, make sure `{0}` is available to sudo".format(npm_cmd[0])
            )
            return

        if build_dir and source_dir and not force:
            should_build = is_stale(build_dir, source_dir)
        else:
            should_build = True

        if should_build:
            log.info(
                "Installing build dependencies with npm.  This may " "take a while..."
            )
            run(npm_cmd + ["install"], cwd=node_package)
            if build_cmd:
                run(npm_cmd + ["run", build_cmd], cwd=node_package)

    return builder


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------


def get_data_files(data_specs, *, top=None, exclude=None):
    """Expand data file specs into valid data files metadata.

    Parameters
    ----------
    data_files_spec: list
        A list of (path, dname, pattern) tuples where the path is the
        `data_files` install path, dname is the source directory, and the
        pattern is a glob pattern.
    top: str, optional
        The top directory
    exclude: func, optional
        Function used to test whether to exclude a file

    Returns
    -------
    A valid list of data_files items.
    """
    return _get_data_files(data_specs, None, top=top, exclude=exclude)


def get_version(fpath: Union[str, Path], name: str = "__version__") -> str:
    """Get the version of the package from the given file by extracting the given `name`."""
    fpath = Path(fpath)
    # Try to get it from a static import first
    try:
        module = StaticModule(fpath.as_posix().replace("/", ".").replace(".py", ""))
        return getattr(module, name)
    except Exception as e:
        pass

    path = fpath.resolve()
    version_ns = {}
    with io.open(path, encoding="utf8") as f:
        exec(f.read(), {}, version_ns)
    return version_ns[name]


def run(cmd, **kwargs):
    """Echo a command before running it."""
    log.info("> " + list2cmdline(cmd))
    kwargs.setdefault("shell", os.name == "nt")
    if not isinstance(cmd, (list, tuple)):
        cmd = shlex.split(cmd, posix=os.name != "nt")
    if not Path(cmd[0]).is_absolute():
        # If a command is not an absolute path find it first.
        cmd_path = which(cmd[0])
        if not cmd_path:
            raise ValueError(
                f"Aborting. Could not find cmd ({cmd[0]}) in path. "
                "If command is not expected to be in user's path, "
                "use an absolute path."
            )
        cmd[0] = cmd_path
    return subprocess.check_call(cmd, **kwargs)


def is_stale(target: Union[str, Path], source: Union[str, Path]) -> bool:
    """Test whether the target file/directory is stale based on the source
    file/directory.
    """
    if not Path(target).exists():
        return True
    target_mtime = recursive_mtime(target) or 0
    return compare_recursive_mtime(source, cutoff=target_mtime)


class BaseCommand(Command):
    """Empty command because Command needs subclasses to override too much"""

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def get_inputs(self):
        return []

    def get_outputs(self):
        return []


def combine_commands(*commands):
    """Return a Command that combines several commands."""

    class CombinedCommand(BaseCommand):
        def initialize_options(self):
            self.commands = []
            for C in commands:
                self.commands.append(C(self.distribution))
            for c in self.commands:
                c.initialize_options()

        def finalize_options(self):
            for c in self.commands:
                c.finalize_options()

        def run(self):
            for c in self.commands:
                c.run()

    return CombinedCommand


def compare_recursive_mtime(
    path: Union[str, Path], cutoff: float, newest: bool = True
) -> bool:
    """Compare the newest/oldest mtime for all files in a directory.

    Cutoff should be another mtime to be compared against. If an mtime that is
    newer/older than the cutoff is found it will return True.
    E.g. if newest=True, and a file in path is newer than the cutoff, it will
    return True.
    """
    path = Path(path)
    if path.is_file():
        mt = mtime(path)
        if newest:
            if mt > cutoff:
                return True
        elif mt < cutoff:
            return True
    for dirname, _, filenames in os.walk(str(path), topdown=False):
        for filename in filenames:
            mt = mtime(Path(dirname) / filename)
            if newest:  # Put outside of loop?
                if mt > cutoff:
                    return True
            elif mt < cutoff:
                return True
    return False


def recursive_mtime(path: Union[str, Path], newest: bool = True) -> float:
    """Gets the newest/oldest mtime for all files in a directory."""
    path = Path(path)
    if path.is_file():
        return mtime(path)
    current_extreme = None
    for dirname, _, filenames in os.walk(str(path), topdown=False):
        for filename in filenames:
            mt = mtime(Path(dirname) / filename)
            if newest:  # Put outside of loop?
                if mt >= (current_extreme or mt):
                    current_extreme = mt
            elif mt <= (current_extreme or mt):
                current_extreme = mt
    return current_extreme


def mtime(path: Union[str, Path]) -> float:
    """shorthand for mtime"""
    return Path(path).stat().st_mtime


def skip_if_exists(paths, CommandClass):
    """Skip a command if list of paths exists."""

    def should_skip():
        return all(Path(path).exists() for path in paths)

    class SkipIfExistCommand(Command):
        def initialize_options(self):
            if not should_skip():
                self.command = CommandClass(self.distribution)
                self.command.initialize_options()
            else:
                self.command = None

        def finalize_options(self):
            if self.command is not None:
                self.command.finalize_options()

        def run(self):
            if self.command is not None:
                self.command.run()

    return SkipIfExistCommand


def ensure_targets(targets):
    """Return a Command that checks that certain files exist.

    Raises a ValueError if any of the files are missing.

    Note: The check is skipped if the `--skip-npm` flag is used.
    """

    class TargetsCheck(BaseCommand):
        def run(self):
            if skip_npm:
                log.info("Skipping target checks")
                return
            missing = [t for t in targets if not os.path.exists(t)]
            if missing:
                raise ValueError(("missing files: %s" % missing))

    return TargetsCheck


# ---------------------------------------------------------------------------
# Deprecated Functions
# ---------------------------------------------------------------------------


@deprecated(
    deprecated_in="0.11",
    removed_in="2.0",
    current_version=__version__,
    details="Parse the version info as described in `get_version_info` docstring",
)
def get_version_info(version_str):
    """DEPRECATED: Get a version info tuple given a version string

    Use something like the following instead:

    ```
    import re

    # Version string must appear intact for tbump versioning
    __version__ = '1.4.0.dev0'

    # Build up version_info tuple for backwards compatibility
    pattern = r'(?P<major>/d+).(?P<minor>/d+).(?P<patch>/d+)(?P<rest>.*)'
    match = re.match(pattern, __version__)
    parts = [int(match[part]) for part in ['major', 'minor', 'patch']]
    if match['rest']:
    parts.append(match['rest'])
    version_info = tuple(parts)
    ```
    """
    match = VERSION_REGEX.match(version_str)
    if not match:
        raise ValueError(f'Invalid version "{version_str}"')
    release = match["release"]
    version_info = [int(p) for p in release.split(".")]
    if release != version_str:
        version_info.append(version_str[len(release) :])
    return tuple(version_info)


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `BaseCommand` directly instead",
)
def command_for_func(func):
    """Create a command that calls the given function."""

    class FuncCommand(BaseCommand):
        def run(self):
            func()
            update_package_data(self.distribution)

    return FuncCommand


@deprecated(
    deprecated_in="0.7",
    removed_in="1.0",
    current_version=__version__,
    details="Use `setuptools` `python_requires` instead",
)
def ensure_python(specs):
    """Given a list of range specifiers for python, ensure compatibility."""
    if sys.version_info >= (3, 10):
        raise RuntimeError(
            "ensure_python is deprecated and not compatible with Python 3.10+"
        )
    if not isinstance(specs, (list, tuple)):
        specs = [specs]
    v = sys.version_info
    part = "%s.%s" % (v.major, v.minor)
    for spec in specs:
        if part == spec:
            return
        try:
            if eval(part + spec):
                return
        except SyntaxError:
            pass
    raise ValueError("Python version %s unsupported" % part)


@deprecated(
    deprecated_in="0.7",
    removed_in="1.0",
    current_version=__version__,
    details="Use `setuptools.find_packages` instead",
)
def find_packages(top):
    """
    Find all of the packages.
    """
    from setuptools import find_packages as fp

    return fp(top)


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `use_package_data=True` and `MANIFEST.in` instead",
)
def update_package_data(distribution):
    """update build_py options to get package_data changes"""
    build_py = distribution.get_command_obj("build_py")
    build_py.finalize_options()


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Not needed",
)
class bdist_egg_disabled(bdist_egg):
    """Disabled version of bdist_egg

    Prevents setup.py install performing setuptools' default easy_install,
    which it should never ever do.
    """

    def run(self):
        sys.exit(
            "Aborting implicit building of eggs. Use `pip install .` "
            " to install from source."
        )


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details=""""
Use `wrap_installers` to handle prebuild steps in cmdclass.
Use `get_data_files` to handle data files.
Use `include_package_data=True` and `MANIFEST.in` for package data.
""",
)
def create_cmdclass(
    prerelease_cmd=None, package_data_spec=None, data_files_spec=None, exclude=None
):
    """Create a command class with the given optional prerelease class.

    Parameters
    ----------
    prerelease_cmd: (name, Command) tuple, optional
        The command to run before releasing.
    package_data_spec: dict, optional
        A dictionary whose keys are the dotted package names and
        whose values are a list of glob patterns.
    data_files_spec: list, optional
        A list of (path, dname, pattern) tuples where the path is the
        `data_files` install path, dname is the source directory, and the
        pattern is a glob pattern.
    exclude: function
        A function which takes a string filename and returns True if the
        file should be excluded from package data and data files, False otherwise.

    Notes
    -----
    We use specs so that we can find the files *after* the build
    command has run.

    The package data glob patterns should be relative paths from the package
    folder containing the __init__.py file, which is given as the package
    name.
    e.g. `dict(foo=['bar/*', 'baz/**'])`

    The data files directories should be absolute paths or relative paths
    from the root directory of the repository.  Data files are specified
    differently from `package_data` because we need a separate path entry
    for each nested folder in `data_files`, and this makes it easier to
    parse.
    e.g. `('share/foo/bar', 'pkgname/bizz, '*')`
    """
    wrapped = [prerelease_cmd] if prerelease_cmd else []
    if package_data_spec or data_files_spec:
        wrapped.append("handle_files")

    wrapper = functools.partial(_wrap_command, wrapped)
    handle_files = _get_file_handler(package_data_spec, data_files_spec, exclude)
    develop_handler = _get_develop_handler()

    if "bdist_egg" in sys.argv:
        egg = wrapper(bdist_egg, strict=True)
    else:
        egg = bdist_egg_disabled

    is_repo = os.path.exists(".git")

    cmdclass = dict(
        build_py=wrapper(build_py, strict=is_repo),
        bdist_egg=egg,
        sdist=wrapper(sdist, strict=True),
        handle_files=handle_files,
    )

    if bdist_wheel:
        cmdclass["bdist_wheel"] = wrapper(bdist_wheel, strict=True)

    cmdclass["develop"] = wrapper(develop_handler, strict=True)
    return cmdclass


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `npm_builder` and `wrap_installers`",
)
def install_npm(
    path=None, build_dir=None, source_dir=None, build_cmd="build", force=False, npm=None
):
    """Return a Command for managing an npm installation.

    Note: The command is skipped if the `--skip-npm` flag is used.

    Parameters
    ----------
    path: str, optional
        The base path of the node package. Defaults to the current directory.
    build_dir: str, optional
        The target build directory.  If this and source_dir are given,
        the JavaScript will only be build if necessary.
    source_dir: str, optional
        The source code directory.
    build_cmd: str, optional
        The npm command to build assets to the build_dir.
    npm: str or list, optional.
        The npm executable name, or a tuple of ['node', executable].
    """
    builder = npm_builder(
        path=path,
        build_dir=build_dir,
        source_dir=source_dir,
        build_cmd=build_cmd,
        force=force,
        npm=npm,
    )

    class NPM(BaseCommand):
        description = "install package.json dependencies using npm"

        def run(self):
            builder()

    return NPM


# ---------------------------------------------------------------------------
# Private Functions
# ---------------------------------------------------------------------------


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `npm_builder` and `wrap_installers`",
)
def _wrap_command(cmds, cls, strict=True):
    """Wrap a setup command

    Parameters
    ----------
    cmds: list(str)
        The names of the other commands to run prior to the command.
    strict: boolean, optional
        Whether to raise errors when a pre-command fails.
    """

    class WrappedCommand(cls):
        def run(self):
            if not getattr(self, "uninstall", None):
                try:
                    [self.run_command(cmd) for cmd in cmds]
                except Exception:
                    if strict:
                        raise
                    else:
                        pass
            # update package data
            update_package_data(self.distribution)

            result = cls.run(self)
            return result

    return WrappedCommand


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `npm_builder` and `wrap_installers`",
)
def _get_file_handler(package_data_spec, data_files_spec, exclude=None):
    """Get a package_data and data_files handler command."""

    class FileHandler(BaseCommand):
        def run(self):
            package_data = self.distribution.package_data
            package_spec = package_data_spec or dict()

            for (key, patterns) in package_spec.items():
                files = _get_package_data(key, patterns)
                if exclude is not None:
                    files = [f for f in files if not exclude(f)]
                package_data[key] = files

            self.distribution.data_files = _get_data_files(
                data_files_spec, self.distribution.data_files, exclude=exclude
            )

    return FileHandler


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `npm_builder` and `wrap_installers`",
)
def _get_develop_handler():
    """Get a handler for the develop command"""

    class _develop(develop):
        def install_for_development(self):
            self.finalize_options()
            super(_develop, self).install_for_development()
            self.run_command("handle_files")
            prefix = Path(self.install_base or self.prefix or sys.prefix)
            for target_dir, filepaths in self.distribution.data_files:
                for filepath in filepaths:
                    filename = Path(filepath).name
                    target = prefix / target_dir / filename
                    self.mkpath(str(target.parent))
                    self.copy_file(str(filepath), str(target))

    return _develop


def _glob_pjoin(*parts: List[Union[str, Path]]) -> str:
    """Join paths for glob processing"""
    if str(parts[0]) in (".", ""):
        parts = parts[1:]
    return Path().joinpath(*parts).as_posix()


def _get_data_files(
    data_specs: List[Tuple[str, str, str]],
    existing: List[Tuple[str, str]],
    *,
    top: Optional[Union[str, Path]] = None,
    exclude: Callable[[str], bool] = None,
):
    """Expand data file specs into valid data files metadata.

    Parameters
    ----------
    data_specs: list of tuples
        See [create_cmdclass] for description.
    existing: list of tuples
        The existing distribution data_files metadata.
    top: str, optional
        The top directory
    exclude: func, optional
        Function used to test whether to exclude a file

    Returns
    -------
    A valid list of data_files items.
    """
    if top is None:
        top = Path.cwd().resolve()
    else:
        top = Path(top)

    # Extract the existing data files into a staging object.
    file_data = defaultdict(list)
    for (path, files) in existing or []:
        file_data[path] = files

    # Extract the files and assign them to the proper data
    # files path.
    for (path, dname, pattern) in data_specs or []:
        dname = Path(dname)
        if dname.is_absolute():
            dname = dname.relative_to(top)

        dname = dname.as_posix().rstrip("/")
        offset = 0 if dname in (".", "") else len(dname) + 1
        files = _get_files(_glob_pjoin(dname, pattern), top=top)

        for fname in files:
            # Normalize the path.
            root = str(Path(fname).parent)
            full_path = _glob_pjoin(path, root[offset:])
            full_path.rstrip("/")

            if exclude is not None and exclude(fname):
                continue
            file_data[full_path].append(fname)

    # Construct the data files spec.
    data_files = []
    for (path, files) in file_data.items():
        data_files.append((path, files))
    return data_files


def _get_files(
    file_patterns: Union[str, List[str]], top: Union[str, Path] = None
) -> List[str]:
    """Expand file patterns to a list of paths.

    Parameters
    -----------
    file_patterns: list or str
        A list of glob patterns for the data file locations.
        The globs can be recursive if they include a `**`.
        They should be relative paths from the top directory or
        absolute paths.
    top: str
        the directory to consider for data files

    Note:
    Files in `node_modules` are ignored.
    """
    if top is None:
        top = Path.cwd().resolve()
    else:
        top = Path(top)

    if not isinstance(file_patterns, (list, tuple)):
        file_patterns = [file_patterns]

    for i, p in enumerate(file_patterns):
        p = Path(p)
        if p.is_absolute():
            file_patterns[i] = str(p.relative_to(top))

    matchers = [_compile_pattern(p) for p in file_patterns]

    files = set()

    for root, dirnames, filenames in os.walk(str(top)):
        # Don't recurse into node_modules
        if "node_modules" in dirnames:
            dirnames.remove("node_modules")
        for m in matchers:
            for filename in filenames:
                fn = Path(_glob_pjoin(root, filename)).relative_to(top)
                fn = fn.as_posix()
                if m(fn):
                    files.add(fn)

    return list(files)


@deprecated(
    deprecated_in="0.8",
    removed_in="1.0",
    current_version=__version__,
    details="Use `npm_builder` and `wrap_installers`",
)
def _get_package_data(
    root: Union[str, Path], file_patterns: Union[str, List[str]] = None
) -> List[str]:
    """Expand file patterns to a list of `package_data` paths.

    Parameters
    -----------
    root: str
        The relative path to the package root from the current dir.
    file_patterns: list or str, optional
        A list of glob patterns for the data file locations.
        The globs can be recursive if they include a `**`.
        They should be relative paths from the root or
        absolute paths.  If not given, all files will be used.

    Note:
    Files in `node_modules` are ignored.
    """
    if file_patterns is None:
        file_patterns = ["*"]
    return _get_files(file_patterns, _glob_pjoin(Path.cwd().resolve(), root))


def _compile_pattern(pat: str, ignore_case=True) -> Callable:
    """Translate and compile a glob pattern to a regular expression matcher."""
    if isinstance(pat, bytes):
        pat_str = pat.decode("ISO-8859-1")
        res_str = _translate_glob(pat_str)
        res = res_str.encode("ISO-8859-1")
    else:
        res = _translate_glob(pat)
    flags = re.IGNORECASE if ignore_case else 0
    return re.compile(res, flags=flags).match


def _iexplode_path(path):
    """Iterate over all the parts of a path.

    Splits path recursively with os.path.split().
    """
    (head, tail) = os.path.split(str(path))
    if not head or (not tail and head == path):
        if head:
            yield head
        if tail or not head:
            yield tail
        return
    for p in _iexplode_path(head):
        yield p
    yield tail


def _translate_glob(pat):
    """Translate a glob PATTERN to a regular expression."""
    translated_parts = []
    for part in _iexplode_path(pat):
        translated_parts.append(_translate_glob_part(part))
    os_sep_class = "[%s]" % re.escape(SEPARATORS)
    res = _join_translated(translated_parts, os_sep_class)
    return "(?ms){res}\\Z".format(res=res)


def _join_translated(translated_parts, os_sep_class):
    """Join translated glob pattern parts.

    This is different from a simple join, as care need to be taken
    to allow ** to match ZERO or more directories.
    """
    res = ""
    for part in translated_parts[:-1]:
        if part == ".*":
            # drop separator, since it is optional
            # (** matches ZERO or more dirs)
            res += part
        else:
            res += part + os_sep_class

    if translated_parts[-1] == ".*":
        # Final part is **
        res += ".+"
        # Follow stdlib/git convention of matching all sub files/directories:
        res += "({os_sep_class}?.*)?".format(os_sep_class=os_sep_class)
    else:
        res += translated_parts[-1]
    return res


def _translate_glob_part(pat):
    """Translate a glob PATTERN PART to a regular expression."""
    # Code modified from Python 3 standard lib fnmatch:
    if pat == "**":
        return ".*"
    i, n = 0, len(pat)
    res = []
    while i < n:
        c = pat[i]
        i = i + 1
        if c == "*":
            # Match anything but path separators:
            res.append("[^%s]*" % SEPARATORS)
        elif c == "?":
            res.append("[^%s]?" % SEPARATORS)
        elif c == "[":
            j = i
            if j < n and pat[j] == "!":
                j = j + 1
            if j < n and pat[j] == "]":
                j = j + 1
            while j < n and pat[j] != "]":
                j = j + 1
            if j >= n:
                res.append("\\[")
            else:
                stuff = pat[i:j].replace("\\", "\\\\")
                i = j + 1
                if stuff[0] == "!":
                    stuff = "^" + stuff[1:]
                elif stuff[0] == "^":
                    stuff = "\\" + stuff
                res.append("[%s]" % stuff)
        else:
            res.append(re.escape(c))
    return "".join(res)
