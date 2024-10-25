"""Install the IPython clusters tab in the Jupyter notebook dashboard

Only applicable for notebook < 7
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.


def install_extensions(enable=True, user=False):
    """Register ipyparallel clusters tab as notebook extensions

    Toggle with enable=True/False.
    """
    import notebook  # noqa

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


install_server_extension = install_extensions
