"""Install the IPython clusters tab in the Jupyter notebook dashboard

Only applicable for notebook < 7
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from jupyter_core.paths import jupyter_config_dir
from notebook.services.config import ConfigManager as FrontendConfigManager
from traitlets.config.manager import BaseJSONConfigManager


def install_extensions(enable=True, user=False):
    """Register ipyparallel clusters tab as notebook extensions

    Toggle with enable=True/False.
    """
    import notebook

    from ipyparallel.util import _v

    if _v(notebook.__version__) < _v('4.2'):
        return _install_extension_nb41(enable)

    from notebook.nbextensions import (
        disable_nbextension,
        enable_nbextension,
        install_nbextension_python,
    )
    from notebook.serverextensions import toggle_serverextension_python

    toggle_serverextension_python('ipyparallel', user=user)
    install_nbextension_python('ipyparallel', user=user)
    if enable:
        enable_nbextension('tree', 'ipyparallel/main', user=user)
    else:
        disable_nbextension('tree', 'ipyparallel/main')


def _install_extension_nb41(enable=True):
    """deprecated, pre-4.2 implementation of installing notebook extension"""
    # server-side
    server = BaseJSONConfigManager(config_dir=jupyter_config_dir())
    server_cfg = server.get('jupyter_notebook_config')
    app_cfg = server_cfg.get('NotebookApp', {})
    server_extensions = app_cfg.get('server_extensions', [])
    server_ext = 'ipyparallel.nbextension'
    server_changed = False
    if enable and server_ext not in server_extensions:
        server_extensions.append(server_ext)
        server_changed = True
    elif (not enable) and server_ext in server_extensions:
        server_extensions.remove(server_ext)
        server_changed = True
    if server_changed:
        server.update(
            'jupyter_notebook_config',
            {
                'NotebookApp': {
                    'server_extensions': server_extensions,
                }
            },
        )

    # frontend config (*way* easier because it's a dict)
    frontend = FrontendConfigManager()
    frontend.update(
        'tree',
        {
            'load_extensions': {
                'ipyparallel/main': enable or None,
            }
        },
    )


install_server_extension = install_extensions
