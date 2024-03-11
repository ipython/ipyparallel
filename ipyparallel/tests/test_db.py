"""Tests for db backends"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta
from unittest import TestCase

import pytest
from jupyter_client.session import Session

from ipyparallel import util
from ipyparallel.controller.dictdb import DictDB
from ipyparallel.controller.hub import init_record
from ipyparallel.controller.sqlitedb import SQLiteDB
from ipyparallel.util import utc


class TaskDBTest:
    def setUp(self):
        util._disable_session_extract_dates()
        self.session = Session()
        self.db = self.create_db()
        self.load_records(16)

    def create_db(self):
        raise NotImplementedError

    def load_records(self, n=1, buffer_size=100):
        """load n records for testing"""
        # sleep 1/10 s, to ensure timestamp is different to previous calls
        time.sleep(0.01)
        msg_ids = []
        for i in range(n):
            msg = self.session.msg('apply_request', content=dict(a=5))
            msg['buffers'] = [os.urandom(buffer_size)]
            rec = init_record(msg)
            msg_id = msg['header']['msg_id']
            msg_ids.append(msg_id)
            self.db.add_record(msg_id, rec)
        return msg_ids

    def test_add_record(self):
        before = self.db.get_history()
        self.load_records(5)
        after = self.db.get_history()
        assert len(after) == len(before) + 5
        assert after[:-5] == before

    def test_drop_record(self):
        msg_id = self.load_records()[-1]
        rec = self.db.get_record(msg_id)
        self.db.drop_record(msg_id)
        with pytest.raises(KeyError):
            self.db.get_record(msg_id)

    def _round_to_millisecond(self, dt):
        """necessary because mongodb rounds microseconds"""
        micro = dt.microsecond
        extra = int(str(micro)[-3:])
        return dt - timedelta(microseconds=extra)

    def test_update_record(self):
        now = self._round_to_millisecond(util.utcnow())
        msg_id = self.db.get_history()[-1]
        rec1 = self.db.get_record(msg_id)
        data = {'stdout': 'hello there', 'completed': now}
        self.db.update_record(msg_id, data)
        rec2 = self.db.get_record(msg_id)
        assert rec2['stdout'] == 'hello there'
        assert rec2['completed'] == now
        rec1.update(data)
        assert rec1 == rec2

    # def test_update_record_bad(self):
    #     """test updating nonexistant records"""
    #     msg_id = str(uuid.uuid4())
    #     data = {'stdout': 'hello there'}
    #     self.assertRaises(KeyError, self.db.update_record, msg_id, data)

    def test_find_records_dt(self):
        """test finding records by date"""
        hist = self.db.get_history()
        middle = self.db.get_record(hist[len(hist) // 2])
        tic = middle['submitted']
        before = self.db.find_records({'submitted': {'$lt': tic}})
        after = self.db.find_records({'submitted': {'$gte': tic}})
        assert len(before) + len(after) == len(hist)
        for b in before:
            assert b['submitted'] < tic
        for a in after:
            assert a['submitted'] >= tic
        same = self.db.find_records({'submitted': tic})
        for s in same:
            assert s['submitted'] == tic

    def test_find_records_keys(self):
        """test extracting subset of record keys"""
        found = self.db.find_records(
            {'msg_id': {'$ne': ''}}, keys=['submitted', 'completed']
        )
        for rec in found:
            assert set(rec.keys()), {'msg_id', 'submitted' == 'completed'}

    def test_find_records_msg_id(self):
        """ensure msg_id is always in found records"""
        found = self.db.find_records(
            {'msg_id': {'$ne': ''}}, keys=['submitted', 'completed']
        )
        for rec in found:
            assert 'msg_id' in rec.keys()
        found = self.db.find_records({'msg_id': {'$ne': ''}}, keys=['submitted'])
        for rec in found:
            assert 'msg_id' in rec.keys()
        found = self.db.find_records({'msg_id': {'$ne': ''}}, keys=['msg_id'])
        for rec in found:
            assert 'msg_id' in rec.keys()

    def test_find_records_in(self):
        """test finding records with '$in','$nin' operators"""
        hist = self.db.get_history()
        even = hist[::2]
        odd = hist[1::2]
        recs = self.db.find_records({'msg_id': {'$in': even}})
        found = [r['msg_id'] for r in recs]
        assert set(even) == set(found)
        recs = self.db.find_records({'msg_id': {'$nin': even}})
        found = [r['msg_id'] for r in recs]
        assert set(odd) == set(found)

    def test_get_history(self):
        msg_ids = self.db.get_history()
        latest = datetime(1984, 1, 1).replace(tzinfo=utc)
        for msg_id in msg_ids:
            rec = self.db.get_record(msg_id)
            newt = rec['submitted']
            assert newt >= latest
            latest = newt
        msg_id = self.load_records(1)[-1]
        assert self.db.get_history()[-1] == msg_id

    def test_datetime(self):
        """get/set timestamps with datetime objects"""
        msg_id = self.db.get_history()[-1]
        rec = self.db.get_record(msg_id)
        assert isinstance(rec['submitted'], datetime)
        self.db.update_record(msg_id, dict(completed=util.utcnow()))
        rec = self.db.get_record(msg_id)
        assert isinstance(rec['completed'], datetime)

    def test_drop_matching(self):
        msg_ids = self.load_records(10)
        query = {'msg_id': {'$in': msg_ids}}
        self.db.drop_matching_records(query)
        recs = self.db.find_records(query)
        assert len(recs) == 0

    def test_null(self):
        """test None comparison queries"""
        msg_ids = self.load_records(10)

        query = {'msg_id': None}
        recs = self.db.find_records(query)
        assert len(recs) == 0

        query = {'msg_id': {'$ne': None}}
        recs = self.db.find_records(query)
        assert len(recs) >= 10

    def test_pop_safe_get(self):
        """editing query results shouldn't affect record [get]"""
        msg_id = self.db.get_history()[-1]
        rec = self.db.get_record(msg_id)
        rec.pop('buffers')
        rec['garbage'] = 'hello'
        rec['header']['msg_id'] = 'fubar'
        rec2 = self.db.get_record(msg_id)
        assert 'buffers' in rec2
        assert 'garbage' not in rec2
        assert rec2['header']['msg_id'] == msg_id

    def test_pop_safe_find(self):
        """editing query results shouldn't affect record [find]"""
        msg_id = self.db.get_history()[-1]
        rec = self.db.find_records({'msg_id': msg_id})[0]
        rec.pop('buffers')
        rec['garbage'] = 'hello'
        rec['header']['msg_id'] = 'fubar'
        rec2 = self.db.find_records({'msg_id': msg_id})[0]
        assert 'buffers' in rec2
        assert 'garbage' not in rec2
        assert rec2['header']['msg_id'] == msg_id

    def test_pop_safe_find_keys(self):
        """editing query results shouldn't affect record [find+keys]"""
        msg_id = self.db.get_history()[-1]
        rec = self.db.find_records({'msg_id': msg_id}, keys=['buffers', 'header'])[0]
        rec.pop('buffers')
        rec['garbage'] = 'hello'
        rec['header']['msg_id'] = 'fubar'
        rec2 = self.db.find_records({'msg_id': msg_id})[0]
        assert 'buffers' in rec2
        assert 'garbage' not in rec2
        assert rec2['header']['msg_id'] == msg_id


class TestDictBackend(TaskDBTest, TestCase):
    def create_db(self):
        return DictDB()

    def test_cull_count(self):
        self.db = self.create_db()  # skip the load-records init from setUp
        self.db.record_limit = 20
        self.db.cull_fraction = 0.2
        self.load_records(20)
        assert len(self.db.get_history()) == 20
        self.load_records(1)
        # 0.2 * 20 = 4, 21 - 4 = 17
        assert len(self.db.get_history()) == 17
        self.load_records(3)
        assert len(self.db.get_history()) == 20
        self.load_records(1)
        assert len(self.db.get_history()) == 17

        for i in range(25):
            self.load_records(1)
            assert len(self.db.get_history()) >= 17
            assert len(self.db.get_history()) <= 20

    def test_cull_size(self):
        self.db = self.create_db()  # skip the load-records init from setUp
        self.db.size_limit = 1000
        self.db.cull_fraction = 0.2
        self.load_records(100, buffer_size=10)
        assert len(self.db.get_history()) == 100
        self.load_records(1, buffer_size=0)
        assert len(self.db.get_history()) == 101
        self.load_records(1, buffer_size=1)
        # 0.2 * 100 = 20, 101 - 20 = 81
        assert len(self.db.get_history()) == 81

    def test_cull_size_drop(self):
        """dropping records updates tracked buffer size"""
        self.db = self.create_db()  # skip the load-records init from setUp
        self.db.size_limit = 1000
        self.db.cull_fraction = 0.2
        self.load_records(100, buffer_size=10)
        assert len(self.db.get_history()) == 100
        self.db.drop_record(self.db.get_history()[-1])
        assert len(self.db.get_history()) == 99
        self.load_records(1, buffer_size=5)
        assert len(self.db.get_history()) == 100
        self.load_records(1, buffer_size=5)
        assert len(self.db.get_history()) == 101
        self.load_records(1, buffer_size=1)
        assert len(self.db.get_history()) == 81

    def test_cull_size_update(self):
        """updating records updates tracked buffer size"""
        self.db = self.create_db()  # skip the load-records init from setUp
        self.db.size_limit = 1000
        self.db.cull_fraction = 0.2
        self.load_records(100, buffer_size=10)
        assert len(self.db.get_history()) == 100
        msg_id = self.db.get_history()[-1]
        self.db.update_record(msg_id, dict(result_buffers=[os.urandom(10)], buffers=[]))
        assert len(self.db.get_history()) == 100
        self.db.update_record(msg_id, dict(result_buffers=[os.urandom(11)], buffers=[]))
        assert len(self.db.get_history()) == 79


class TestSQLiteBackend(TaskDBTest, TestCase):
    def setUp(self):
        tmp_file = tempfile.NamedTemporaryFile(suffix='.db')
        self.temp_db = tmp_file.name
        tmp_file.close()
        super().setUp()

    def create_db(self):
        location, fname = os.path.split(self.temp_db)
        log = logging.getLogger('test')
        log.setLevel(logging.CRITICAL)
        return SQLiteDB(location=location, filename=fname, log=log)

    def tearDown(self):
        self.db.close()
        try:
            os.remove(self.temp_db)
        except Exception:
            pass
