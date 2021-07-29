"""Place to put the base Handler"""
import warnings
from functools import lru_cache

_APIHandler = None


@lru_cache()
def _guess_api_handler():
    """Fallback to guess API handler by availability"""
    try:
        from notebook.base.handlers import APIHandler
    except ImportError:
        from jupyter_server.base.handlers import APIHandler
    global _APIHandler
    _APIHandler = APIHandler
    return _APIHandler


def get_api_handler(app=None):
    """Get the base APIHandler class to use

    Inferred from app class (either jupyter_server or notebook app)
    """
    global _APIHandler
    if _APIHandler is not None:
        return _APIHandler
    if app is None:
        warnings.warn(
            "Guessing base APIHandler class. Specify an app to ensure the right APIHandler is used.",
            stacklevel=2,
        )
        return _guess_api_handler()

    top_modules = {cls.__module__.split(".", 1)[0] for cls in app.__class__.mro()}
    if "jupyter_server" in top_modules:
        from jupyter_server.base.handlers import APIHandler

        _APIHandler = APIHandler
        return APIHandler
    if "notebook" in top_modules:
        from notebook.base.handlers import APIHandler

        _APIHandler = APIHandler
        return APIHandler

    warnings.warn(f"Failed to detect base APIHandler class for {app}.", stacklevel=2)
    return _guess_api_handler()
