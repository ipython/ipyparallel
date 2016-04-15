"""Tornado handlers for IPython cluster web service."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import json
import os
import sys

from tornado import web

from notebook.utils import url_path_join as ujoin
from notebook.base.handlers import IPythonHandler
from notebook.nbextensions import install_nbextension

from .clustermanager import ClusterManager


static = os.path.join(os.path.dirname(__file__), 'static')


class ClusterHandler(IPythonHandler):
    @property
    def cluster_manager(self):
        return self.settings['cluster_manager']


class MainClusterHandler(ClusterHandler):

    @web.authenticated
    def get(self):
        self.finish(json.dumps(self.cluster_manager.list_profiles()))


class ClusterProfileHandler(ClusterHandler):

    @web.authenticated
    def get(self, profile):
        self.finish(json.dumps(self.cluster_manager.profile_info(profile)))


class ClusterActionHandler(ClusterHandler):

    @web.authenticated
    def post(self, profile, action):
        cm = self.cluster_manager
        if action == 'start':
            n = self.get_argument('n', default=None)
            if not n:
                data = cm.start_cluster(profile)
            else:
                data = cm.start_cluster(profile, int(n))
        if action == 'stop':
            data = cm.stop_cluster(profile)
        self.finish(json.dumps(data))


#-----------------------------------------------------------------------------
# URL to handler mappings
#-----------------------------------------------------------------------------


_cluster_action_regex = r"(?P<action>start|stop)"
_profile_regex = r"(?P<profile>[^\/]+)" # there is almost no text that is invalid

default_handlers = [
    (r"/clusters", MainClusterHandler),
    (r"/clusters/%s/%s" % (_profile_regex, _cluster_action_regex), ClusterActionHandler),
    (r"/clusters/%s" % _profile_regex, ClusterProfileHandler),
]


def load_jupyter_server_extension(nbapp):
    """Load the nbserver extension"""
    from distutils.version import LooseVersion as V
    import notebook
    
    nbapp.log.info("Loading IPython parallel extension")
    webapp = nbapp.web_app
    webapp.settings['cluster_manager'] = ClusterManager(parent=nbapp)
    
    if V(notebook.__version__) < V('4.2'):
        windows = sys.platform.startswith('win')
        install_nbextension(static, destination='ipyparallel', symlink=not windows, user=True)
        cfgm = nbapp.config_manager
        cfgm.update('tree', {
            'load_extensions': {
                'ipyparallel/main': True,
            }
        })
    base_url = webapp.settings['base_url']
    webapp.add_handlers(".*$", [
        (ujoin(base_url, pat), handler)
        for pat, handler in default_handlers
    ])
