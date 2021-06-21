"""Deprecated import for ipyparallel.cluster.launchers"""
import warnings

from ipyparallel.cluster.launcher import *  # noqa

warnings.warn(
    f"{__name__} is deprecated in ipyparallel 7. Use ipyparallel.cluster.launcher.",
    DeprecationWarning,
)
