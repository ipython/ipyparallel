"""Tests for mongodb backend"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import os

from unittest import TestCase
import pytest

from . import test_db

c = None

@pytest.fixture(scope='module')
def mongo_conn(request):
    global c
    try:
        from pymongo import MongoClient
    except ImportError:
        pytest.skip("Requires mongodb")

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
        c = MongoClient(**conn_kwargs)
    except Exception:
        c = None
    if c is not None:
        request.addfinalizer(lambda : c.drop_database('iptestdb'))
    return c


@pytest.mark.usefixture('mongo_conn')
class TestMongoBackend(test_db.TaskDBTest, TestCase):
    """MongoDB backend tests"""

    def create_db(self):
        try:
            from ipyparallel.controller.mongodb import MongoDB
            return MongoDB(database='iptestdb', _connection=c)
        except Exception:
            pytest.skip("Couldn't connect to mongodb")

