import unittest
from unittest.mock import Mock, MagicMock, patch, ANY
import dragon
import multiprocessing as mp
import time
import json
import os
from threading import get_ident
import sqlite3
from pickle import dumps

from dragon.globalservices.node import get_list, query
from dragon.telemetry.tsdb_server import TSDBApplication, tsdb


class TestDragonTelemetryTSDBServer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.options = {
            "bind": "%s:%s" % ("localhost", "4243"),
            "workers": 1,
            "start_event": mp.Event(),
            "shutdown_event": mp.Event(),
            "daemon": True,
            "loglevel": "critical",
            "tmdb_dir": "/tmp",
        }

    def setUp(self):
        pass

    @patch("multiprocessing.cpu_count")
    def test_load_config(self, mock_workers):
        mock_flask_app = MagicMock()

        tsdb_app = TSDBApplication(mock_flask_app, self.options)

        with patch.object(tsdb_app.cfg, "set") as mock_set:
            tsdb_app.load_config()

        self.assertEqual(tsdb_app.application, mock_flask_app)
        self.assertEqual(tsdb_app.options, self.options)

        mock_set.assert_any_call("daemon", True)
        mock_set.assert_any_call("workers", 1)
        mock_set.assert_any_call("loglevel", "critical")
        mock_set.assert_any_call("bind", "localhost:4243")

    def test_load_app(self):
        mock_flask_app = MagicMock()

        tsdb_app = TSDBApplication(mock_flask_app, self.options)

        self.assertEqual(tsdb_app.load(), mock_flask_app)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
