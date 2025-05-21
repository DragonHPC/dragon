import unittest
from unittest.mock import patch
import dragon
import multiprocessing as mp
import time
import json
import os
from threading import get_ident

from dragon.globalservices.node import get_list, query
from dragon.telemetry.aggregator_app import app

from telemetry.telemetry_data import AGGREGATOR_FUNCTIONS, BASE_GRAFANA_QUERY, BASE_DRAGON_SERVER_RESPONSE, BASE_DRAGON_SERVER_SUGGEST_RESPONSE


class TestDragonTelemetryAggregatorApp(unittest.TestCase):

    def setUp(self):
        node_list = get_list()
        node_descr_list = [query(h_uid) for h_uid in node_list]
        node_list = [descr.host_name for descr in node_descr_list]
        queue_dict = {}
        for host in node_list:
            queue_dict[host] = mp.Queue()
        self.context = app.app_context()
        self.context.push()
        self.client = app.test_client()
        app.config["queue_dict"] = queue_dict
        app.config["return_queue"] = mp.Queue()
        app.config["result_dict"] = {}

    def test_get_aggregators(self):
        response = self.client.get("/api/aggregators")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.get_data(as_text=True)), AGGREGATOR_FUNCTIONS)

    @patch("dragon.telemetry.aggregator_app.time")
    def test_post_query(self, mock_time):

        fix_time = time.time()
        mock_time.time.return_value = fix_time
        query_from_grafana = BASE_GRAFANA_QUERY
        req_uid = str(int(fix_time * 100)) + "_" + str(get_ident())

        # Assume collectors and dragon servers do their work and send data to return queue
        dragon_server_response = BASE_DRAGON_SERVER_RESPONSE
        dragon_server_response["req_id"] = req_uid
        app.config["return_queue"].put(dragon_server_response)

        response = self.client.post("/api/query", json=query_from_grafana)

        self.assertEqual(response.status_code, 200)

    @patch("dragon.telemetry.aggregator_app.time")
    def test_get_suggest(self, mock_time):
        dragon_server_response = BASE_DRAGON_SERVER_SUGGEST_RESPONSE
        fix_time = time.time()
        mock_time.time.return_value = fix_time
        req_uid = str(int(fix_time * 100)) + "_" + str(os.getpid())
        dragon_server_response["req_id"] = req_uid
        app.config["return_queue"].put(dragon_server_response)
        response = self.client.get("/api/suggest")

        response_data = json.loads(response.get_data(as_text=True))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response_data), 4)
        assert isinstance(response_data, list)

    @patch("dragon.telemetry.aggregator_app.time")
    def test_get_suggest_with_query(self, mock_time):
        fix_time = time.time()
        mock_time.time.return_value = fix_time
        uid = str(int(fix_time * 100)) + "_" + str(os.getpid())
        dragon_server_response = BASE_DRAGON_SERVER_SUGGEST_RESPONSE
        dragon_server_response["req_id"] = uid
        app.config["return_queue"].put(dragon_server_response)
        response = self.client.get("/api/suggest?q=test")
        response_data = json.loads(response.get_data())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response_data, ["dragon_telemetry_test_data"])
        self.assertEqual(len(response_data), 1)

        assert isinstance(response_data, list)

    def test_get_telemetery_shutdown(self):
        response = self.client.get("/api/telemetry_shutdown")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.get_data())["shutdown_begin"])

    def tearDown(self):
        self.context.pop()


if __name__ == "__main__":
    unittest.main()
