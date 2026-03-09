"""Tests for task label functionality"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import logging
import os
from unittest import TestCase

import pytest

import ipyparallel as ipp
from ipyparallel.cluster.launcher import LocalControllerLauncher


def speudo_wait(t):
    import time

    tic = time.time()
    print(f"waiting for {t}s...")
    # time.sleep(t)  # do NOT wait for t seconds to speed up tests
    print("done")
    return time.time() - tic


class TaskLabelTest:
    def setUp(self):
        self.cluster = ipp.Cluster(
            n=2, log_level=10, controller=self.get_controller_launcher()
        )
        self.cluster.start_cluster_sync()

        self.rc = self.cluster.connect_client_sync()
        self.rc.wait_for_engines(n=2)

    def get_controller_launcher(self):
        raise NotImplementedError

    def tearDown(self):
        self.cluster.stop_engines()
        self.cluster.stop_controller()
        # self.cluster.close()

    def run_tasks(self, view):
        ar_list = []
        # use context to set label
        with view.temp_flags(label="mylabel_map"):
            ar_list.append(view.map_async(speudo_wait, [1.1, 1.2, 1.3, 1.4, 1.5]))
        # use set_flags to set label
        ar_list.extend(
            [
                view.set_flags(label=f"mylabel_apply_{i:02}").apply_async(
                    speudo_wait, 2 + i / 10
                )
                for i in range(5)
            ]
        )
        view.wait(ar_list)

        # build list of used labels
        map_labels = ["mylabel_map"]
        apply_labels = []
        for i in range(5):
            apply_labels.append(f"mylabel_apply_{i:02}")
        return map_labels, apply_labels

    def check_labels(self, labels):
        # query database
        data = self.rc.db_query({'label': {"$nin": [""]}}, keys=['msg_id', 'label'])
        for d in data:
            msg_id = d['msg_id']
            label = d['label']
            assert label in labels
            labels.remove(label)

        assert len(labels) == 0

    def clear_db(self):
        self.rc.purge_everything()

    def test_balanced_view(self):
        bview = self.rc.load_balanced_view()
        map_labels, apply_labels = self.run_tasks(bview)
        labels = map_labels * 5 + apply_labels
        self.check_labels(labels)
        self.clear_db()

    def test_direct_view(self):
        dview = self.rc[:]
        map_labels, apply_labels = self.run_tasks(dview)
        labels = map_labels * 2 + apply_labels * 2
        self.check_labels(labels)
        self.clear_db()


class TestLabelDictDB(TaskLabelTest, TestCase):
    def get_controller_launcher(self):
        class dictDB(LocalControllerLauncher):
            controller_args = ["--dictdb"]

        return dictDB


class TestLabelSqliteDB(TaskLabelTest, TestCase):
    def get_controller_launcher(self):
        class sqliteDB(LocalControllerLauncher):
            controller_args = ["--sqlitedb"]

        return sqliteDB


class TestLabelMongoDB(TaskLabelTest, TestCase):
    def get_controller_launcher(self):
        class mongoDB(LocalControllerLauncher):
            database = "mongodb-label-test"  # use this database label for testing
            controller_args = ["--mongodb", f"--MongoDB.database={database}"]

        try:
            from pymongo import MongoClient

            c = MongoClient(serverSelectionTimeoutMS=2000)
            servinfo = c.server_info()  # checks if mongo server is reachable using the default connection parameter

            # make sure that test database is empty
            db = c[mongoDB.database]
            records = db.get_collection("task_records")
            records.delete_many({})
        except (ImportError, Exception):
            pytest.skip("Requires mongodb")

        return mongoDB
