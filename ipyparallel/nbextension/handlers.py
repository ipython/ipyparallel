"""Tornado handlers for IPython cluster web service."""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import json
import os
import sys

from notebook.base.handlers import APIHandler
from notebook.utils import url_path_join as ujoin
from tornado import web

from ..cluster import ClusterManager
from ..util import abbreviate_profile_dir


static = os.path.join(os.path.dirname(__file__), 'static')


class ClusterHandler(APIHandler):
    @property
    def cluster_manager(self):
        return self.settings['cluster_manager']

    def cluster_model(self, cluster):
        """Create a JSONable cluster model"""
        d = cluster.to_dict()
        profile_dir = d['cluster']['profile_dir']
        # provide abbreviated profile info
        d['cluster']['profile'] = abbreviate_profile_dir(profile_dir)
        return d


class ClusterListHandler(ClusterHandler):
    """List and create new clusters

    GET /clusters : list current clusters
    POST / clusters (JSON body) : create a new cluster"""

    @web.authenticated
    def get(self):
        # currently reloads everything from disk. Is that what we want?
        clusters = self.cluster_manager.load_clusters()
        self.finish(
            {key: self.cluster_model(cluster) for key, cluster in clusters.items()}
        )

    @web.authenticated
    def post(self):
        body = self.get_json_body() or {}
        # profile
        # cluster_id
        cluster_id, cluster = self.cluster_manager.new_cluster(**body)
        self.write(json.dumps({}))


class ClusterActionHandler(ClusterHandler):
    """Actions on a single cluster

    GET: read single cluster model
    POST: start
    PATCH: engines?
    DELETE: stop
    """

    def get_cluster(self, cluster_key):
        try:
            return self.cluster_manager.get_cluster(cluster_key)
        except KeyError:
            raise web.HTTPError(404, f"No such cluster: {cluster_key}")

    @web.authenticated
    async def post(self, cluster_key):
        cluster = self.get_cluster(cluster_key)
        n = self.get_argument('n', default=None)
        await cluster.start_cluster(n=n)
        self.write(json.dumps(self.cluster_model(cluster)))

    @web.authenticated
    async def get(self, cluster_key):
        cluster = self.get_cluster(cluster_key)
        self.write(json.dumps(self.cluster_model(cluster)))

    @web.authenticated
    async def delete(self, cluster_key):
        cluster = self.get_cluster(cluster_key)
        await cluster.stop_cluster()
        self.cluster_manager.remove_cluster(cluster_key)
        self.write(json.dumps(self.cluster_model(cluster)))


# -----------------------------------------------------------------------------
# URL to handler mappings
# -----------------------------------------------------------------------------


_cluster_action_regex = r"(?P<action>start|stop|create)"
_cluster_key_regex = (
    r"(?P<cluster_key>[^\/]+)"  # there is almost no text that is invalid
)

default_handlers = [
    (r"/clusters", ClusterListHandler),
    (
        rf"/clusters/{_cluster_key_regex}",
        ClusterActionHandler,
    ),
]


def load_jupyter_server_extension(nbapp):
    """Load the nbserver extension"""
    import notebook

    nbapp.log.info("Loading IPython parallel extension")
    webapp = nbapp.web_app
    webapp.settings['cluster_manager'] = ClusterManager(parent=nbapp)

    base_url = webapp.settings['base_url']
    webapp.add_handlers(
        ".*$", [(ujoin(base_url, pat), handler) for pat, handler in default_handlers]
    )
