"""Jupyter server extension(s)"""
import warnings


def load_jupyter_server_extension(app):
    warnings.warn(
        "Using ipyparallel.nbextension as server extension is deprecated in IPython Parallel 7.0. Use top-level ipyparallel.",
        DeprecationWarning,
    )
    from ipyparallel import _load_jupyter_server_extension

    return _load_jupyter_server_extension(app)
