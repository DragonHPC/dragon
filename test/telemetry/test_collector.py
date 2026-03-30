import unittest
from unittest.mock import Mock, MagicMock, patch, ANY
import dragon
import multiprocessing as mp
import time
import json
import os
from threading import get_ident

from dragon.globalservices.node import get_list, query
from dragon.telemetry.collector import Collector

os.environ["DRAGON_DEFAULT_TMDB_WNDW"] = "10"


class TestDragonTelemetryCollector(unittest.TestCase):
    def setUp(self):
        num_nodes = len(get_list())
        self.collector_start_event = mp.Event()
        self.collector_shutdown_event = mp.Event()
        telemetry_cfg = {}
        self.collector = Collector(telemetry_cfg)

    def test_init(self):
        telemetry_cfg = {}
        collector = Collector(telemetry_cfg)
        assert isinstance(collector.data, dict)

    def test_startup_shutdown(self):
        collector_proc = mp.Process(
            target=self.collector.collect, args=[self.collector_start_event, self.collector_shutdown_event]
        )
        collector_proc.start()
        time.sleep(5)
        self.collector_start_event.set()
        time.sleep(10)
        self.collector_shutdown_event.set()
        collector_proc.join()

    @patch("dragon.telemetry.collector.requests")
    @patch("multiprocessing.Event")
    def test_collect_success(self, mock_request, mock_event):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"success": 1, "failed": 0, "errors": ""}
        mock_request.post.side_effect = mock_response
        is_set = [False, False, True]
        mock_event.is_set.side_effect = is_set

        self.collector_start_event.set()
        self.collector.collect(self.collector_start_event, mock_event)

        self.assertEqual(len(mock_event.is_set.call_args_list), len(is_set))

    @patch("dragon.telemetry.collector.requests")
    @patch("multiprocessing.Event")
    def test_collect_tmdb_wndw(self, mock_request, mock_event):
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"success": 1, "failed": 0, "errors": ""}
        mock_request.post.side_effect = mock_response

        our_start_time = int(time.time())

        def mock_is_set():
            if int(time.time()) - our_start_time > 10:
                return True
            return False

        mock_event.is_set = MagicMock(side_effect=mock_is_set)

        self.collector_start_event.set()
        self.collector.tmdb_window = 5
        self.collector.collect(self.collector_start_event, mock_event)
        mock_event.post.assert_any_call("http://localhost:4243/api/tsdb_cleanup", json=ANY)

    def tearDown(self):
        pass


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
