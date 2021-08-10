"""Tornado handlers for IPython cluster web service."""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import json
import os

from jupyter_server.utils import url_path_join as ujoin
from tornado import web

from ..cluster import ClusterManager
from ..util import abbreviate_profile_dir
from .base import get_api_handler


static = os.path.join(os.path.dirname(__file__), 'static')

APIHandler = get_api_handler()


class ClusterHandler(APIHandler):
    @property
    def cluster_manager(self):
        return self.settings['cluster_manager']

    def cluster_model(self, key, cluster):
        """Create a JSONable cluster model

        Adds additional fields to make things easier on the client
        """
        d = cluster.to_dict()
        profile_dir = d['cluster']['profile_dir']
        # provide abbreviated profile info
        d['cluster']['profile'] = abbreviate_profile_dir(profile_dir)

        # top-level fields, added for easier client-side logic
        # add 'id' key, since typescript is bad at key-value pairs
        # so we return a list instead of a dict
        d['id'] = key
        # add total engine count
        d['engines']['n'] = sum(es.get('n', 0) for es in d['engines']['sets'].values())
        # add cluster file
        d['cluster_file'] = cluster.cluster_file
        return d


class ClusterListHandler(ClusterHandler):
    """List and create new clusters

    GET /clusters : list current clusters
    POST / clusters (JSON body) : create a new cluster"""

    @web.authenticated
    def get(self):
        # currently reloads everything from disk. Is that what we want?
        clusters = self.cluster_manager.load_clusters(init_default_clusters=True)

        def sort_key(model):
            """Sort clusters

            order:
            - running
            - default profile
            - default cluster id
            """
            running = True if model.get("controller") else False
            profile = model['cluster']['profile']
            cluster_id = model['cluster']['cluster_id']
            default_profile = profile == 'default'
            default_cluster = cluster_id == ''

            return (
                not running,
                not default_profile,
                not default_cluster,
                profile,
                cluster_id,
            )

        self.write(
            json.dumps(
                sorted(
                    [
                        self.cluster_model(key, cluster)
                        for key, cluster in clusters.items()
                    ],
                    key=sort_key,
                )
            )
        )

    @web.authenticated
    def post(self):
        body = self.get_json_body() or {}
        self.log.info(f"Creating cluster with {body}")
        # clear null values
        for key in ('profile', 'cluster_id', 'n'):
            if key in body and body[key] in {None, ''}:
                body.pop(key)
        if 'profile' in body and os.path.sep in body['profile']:
            # if it looks like a path, use profile_dir
            body['profile_dir'] = body.pop('profile')

        if (
            'profile' in body
            and 'cluster_id' not in body
            and f"{body['profile']}:" not in self.cluster_mananger.clusters
        ):
            # if no cluster exists for a profile,
            # default for no cluster id instead of random
            body["cluster_id"] = ""

        cluster_id, cluster = self.cluster_manager.new_cluster(**body)
        self.write(json.dumps(self.cluster_model(cluster_id, cluster)))


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
        body = self.get_json_body() or {}
        n = body.get("n", None)
        if n is not None and not isinstance(n, int):
            raise web.HTTPError(400, f"n must be an integer, not {n!r}")
        await cluster.start_cluster(n=n)
        self.write(json.dumps(self.cluster_model(cluster_key, cluster)))

    @web.authenticated
    async def get(self, cluster_key):
        cluster = self.get_cluster(cluster_key)
        self.write(json.dumps(self.cluster_model(cluster_key, cluster)))

    @web.authenticated
    async def delete(self, cluster_key):
        cluster = self.get_cluster(cluster_key)
        await cluster.stop_cluster()
        self.cluster_manager.remove_cluster(cluster_key)
        self.set_status(204)


# URL to handler mappings


_cluster_key_regex = (
    r"(?P<cluster_key>[^\/]+)"  # there is almost no text that is invalid
)

default_handlers = [
    (r"/ipyparallel/clusters", ClusterListHandler),
    (
        rf"/ipyparallel/clusters/{_cluster_key_regex}",
        ClusterActionHandler,
    ),
]


def load_jupyter_server_extension(nbapp):
    """Load the nbserver extension"""

    nbapp.log.info("Loading IPython parallel extension")
    webapp = nbapp.web_app
    webapp.settings['cluster_manager'] = ClusterManager(parent=nbapp)

    base_url = webapp.settings['base_url']
    webapp.add_handlers(
        ".*$", [(ujoin(base_url, pat), handler) for pat, handler in default_handlers]
    )
