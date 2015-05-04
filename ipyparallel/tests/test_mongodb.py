"""Tests for mongodb backend"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import os

from unittest import TestCase

from nose import SkipTest

from . import test_db

c = None

def setup():
    global c

    try:
        from pymongo import Connection
    except ImportError:
        raise SkipTest()

    conn_kwargs = {}
    if 'DB_IP' in os.environ:
        conn_kwargs['host'] = os.environ['DB_IP']
    if 'DBA_MONGODB_ADMIN_URI' in os.environ:
        # On ShiningPanda, we need a username and password to connect. They are
        # passed in a mongodb:// URI.
        conn_kwargs['host'] = os.environ['DBA_MONGODB_ADMIN_URI']
    if 'DB_PORT' in os.environ:
        conn_kwargs['port'] = int(os.environ['DB_PORT'])
    
    try:
        c = Connection(**conn_kwargs)
    except Exception:
        c=None

def teardown(self):
    if c is not None:
        c.drop_database('iptestdb')

class TestMongoBackend(test_db.TaskDBTest, TestCase):
    """MongoDB backend tests"""

    def create_db(self):
        from ipyparallel.controller.mongodb import MongoDB
        try:
            return MongoDB(database='iptestdb', _connection=c)
        except Exception:
            raise SkipTest("Couldn't connect to mongodb")

