"""Install the IPython clusters tab in the Jupyter notebook dashboard"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from traitlets.config.manager import BaseJSONConfigManager
from notebook.services.config import ConfigManager as FrontendConfigManager


def install_server_extension(enable=True):
    """Register ipyparallel clusters tab as a notebook server extension
    
    Toggle with enable=True/False.
    """
    # server-side
    server = BaseJSONConfigManager()
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
        server.update('jupyter_notebook_config', {
            'NotebookApp': {
                'server_extensions': server_extensions,
            }
        })
    
    # frontend config (*way* easier because it's a dict)
    frontend = FrontendConfigManager()
    frontend.update('tree', {
        'load_extensions': {
            'ipyparallel/main': enable or None,
        }
    })

