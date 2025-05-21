import unittest
import dragon
import multiprocessing as mp
import queue
import cloudpickle

from dragon.utils import set_local_kv, B64
from dragon.telemetry.analysis import AnalysisClient, AnalysisServer, LS_TAS_KEY
from dragon.telemetry.telemetry_head import _register_with_local_services

from telemetry.telemetry_data import SAMPLE_DATA, BASE_GRAFANA_QUERY, BASE_CLIENT_QUERY, BASE_DRAGON_SERVER_RESPONSE

class TestDragonTelemetryAnalysisServer(unittest.TestCase):
    def setUp(self):
        self.server_queue = mp.Queue()
        self.client_queue = mp.Queue()
        self.as_discovery = mp.Queue()
        self.ds_server_queue = mp.Queue()
        self.shutdown_event = mp.Event()
        self.as_server = AnalysisServer(
            {"test-hostname": self.ds_server_queue},
            self.server_queue,
            self.as_discovery,
            self.shutdown_event,
            1,
        )
        self.as_server_proc = mp.Process(target=self.as_server.run)
        self.as_server_proc.start()

    def tearDown(self):
        if self.as_server_proc.is_alive():
            self.shutdown_event.set()
        self.as_server_proc.join()

    def test_analysis_server_shutdown(self):
        self.assertTrue(self.as_server_proc.is_alive())
        self.assertTrue(not self.shutdown_event.is_set())
        self.shutdown_event.set()
        self.as_server_proc.join()
        self.assertTrue(not self.as_server_proc.is_alive())

    def test_analysis_server_discovery(self):
        # we only put one in the queue as part of setUp
        queue_str = self.as_discovery.get()
        with self.assertRaises(queue.Empty):
            _ = self.as_discovery.get(timeout=1)
        self.assertIsInstance(queue_str, str)

    def test_analysis_server_good_msg(self):
        # we only put one in the queue as part of setUp
        queue_str = self.as_discovery.get()
        server_input_queue = cloudpickle.loads(B64.str_to_bytes(queue_str))
        server_input_queue.put((BASE_CLIENT_QUERY, self.client_queue))
        ds_server_message = self.ds_server_queue.get()
        self.server_queue.put(BASE_DRAGON_SERVER_RESPONSE)
        # type is either query or suggest at the moment
        self.assertIn("type", ds_server_message)
        self.assertIn("start", ds_server_message)
        self.assertIsInstance(ds_server_message["start"], str)
        self.assertEqual(ds_server_message["start"], str(BASE_CLIENT_QUERY["start_time"] * 1000))
        self.assertIn("req_id", ds_server_message)
        self.assertIn("queries", ds_server_message)
        self.assertEqual(len(ds_server_message["queries"]), len(BASE_CLIENT_QUERY["metrics"]))
        self.assertIn("return_queue", ds_server_message)
        self.assertEqual(ds_server_message["return_queue"], "analysis-server")
        self.assertIn("showQuery", ds_server_message)
        self.assertEqual(ds_server_message["showQuery"], False)
        response = self.client_queue.get()
        self.assertDictEqual(response[0], BASE_DRAGON_SERVER_RESPONSE)


class TestDragonTelemetryAnalysisClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_queue = mp.Queue()
        cls.client_queue = mp.Queue()
        analysis_comm_sdesc = B64.bytes_to_str(cloudpickle.dumps(cls.server_queue))
        _register_with_local_services(analysis_comm_sdesc)

    def test_analysis_client_connect(self):
        tac = AnalysisClient()
        tac.connect()
        test_msg = 42
        tac._request_conn.put(test_msg)
        sq_result = self.server_queue.get()
        self.assertEqual(sq_result, test_msg)

    def test_analysis_client_good_msg(self):
        # we only put one in the queue as part of setUp
        tac = AnalysisClient()
        tac.connect()
        tac._data_queue.put([BASE_DRAGON_SERVER_RESPONSE])
        tac.get_data(BASE_CLIENT_QUERY["metrics"][0], BASE_CLIENT_QUERY["start_time"])
        output_query, client_queue = self.server_queue.get()
        (BASE_CLIENT_QUERY, self.client_queue)
        self.assertEqual(output_query, BASE_CLIENT_QUERY)
        test_msg = 42
        tac._data_queue.put(test_msg)
        cq_result = client_queue.get()
        self.assertEqual(cq_result, test_msg)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
