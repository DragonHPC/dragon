import unittest
import dragon
import multiprocessing as mp
import queue
import cloudpickle

from dragon.utils import set_local_kv, B64
from dragon.telemetry.analysis import AnalysisClient, AnalysisServer, LS_TAS_KEY
from dragon.telemetry.telemetry_head import _register_with_local_services

# sample data put into DB, please use existing start and end times if you add data
SAMPLE_DATA = {
    "load_average": {"1722010526": 0.58, "1722010527": 0.58, "1722010528": 0.62},
    "used_RAM": {"1722010526": 9.3, "1722010527": 9.3, "1722010528": 9.3},
    "cpu_percent": {"1722010526": 0.8, "1722010527": 1.6, "1722010528": 0.8},
}

# sample query that works with the above data
BASE_GRAFANA_QUERY = {
    "start": 1722010526,
    "queries": [{"metric": "load_average", "aggregator": "sum", "downsample": "100ms-avg", "tags": {"host": "*"}}],
    "msResolution": False,
    "globalAnnotations": True,
    "showQuery": True,
    "end": 1722010528,
    "return_queue": "aggregator",
}


BASE_CLIENT_QUERY = {"type": "query", "start_time": 1722010526, "metrics": ["load_average"]}

BASE_DRAGON_SERVER_RESPONSE = {
    "start": 1708966449000,
    "queries": [
        {
            "metric": "cpu_percent",
            "aggregator": "sum",
            "downsample": "100ms-avg",
            "tags": {"host": "*"},
            "index": 0,
        }
    ],
    "msResolution": False,
    "globalAnnotations": True,
    "showQuery": True,
    "end": 1701789807277,
    "req_id": 0,
    "type": "query",
    "host": "test-hostname",
    "result": [
        {
            "query": {
                "metric": "cpu_percent",
                "aggregator": "sum",
                "downsample": "100ms-avg",
                "tags": {"host": "*"},
                "index": 0,
            },
            "metric": "cpu_percent",
            "tags": {"host": "pinoak0034"},
            "aggregateTags": [],
            "dps": {
                "1721860573": 0.5,
                "1721860574": 0.0,
                "1721860575": 0.0,
                "1721860576": 0.8,
                "1721860577": 0.8,
                "1721860578": 0.8,
                "1721860579": 0.8,
                "1721860580": 0.8,
                "1721860581": 0.8,
                "1721860582": 0.8,
                "1721860583": 0.7,
                "1721860584": 0.8,
                "1721860585": 0.8,
                "1721860586": 0.8,
                "1721860587": 0.8,
                "1721860588": 0.8,
                "1721860589": 0.8,
                "1721860590": 0.8,
                "1721860591": 1.0,
                "1721860592": 0.8,
                "1721860593": 0.8,
                "1721860594": 0.8,
                "1721860595": 0.8,
                "1721860596": 56.7,
                "1721860597": 99.5,
                "1721860598": 15.0,
                "1721860599": 0.8,
                "1721860600": 0.8,
                "1721860601": 0.8,
                "1721860602": 0.8,
                "1721860603": 0.9,
                "1721860604": 0.8,
                "1721860605": 0.8,
                "1721860606": 0.8,
                "1721860607": 0.8,
                "1721860608": 0.8,
                "1721860609": 0.8,
                "1721860610": 0.9,
                "1721860611": 0.8,
                "1721860612": 0.8,
                "1721860613": 0.8,
                "1721860614": 0.8,
                "1721860615": 0.8,
                "1721860616": 0.9,
                "1721860617": 0.8,
                "1721860618": 0.8,
                "1721860619": 0.8,
                "1721860620": 0.8,
                "1721860621": 0.8,
                "1721860622": 0.8,
                "1721860623": 0.8,
                "1721860624": 0.8,
                "1721860625": 0.9,
                "1721860626": 0.8,
                "1721860627": 0.8,
                "1721860628": 0.8,
                "1721860629": 0.8,
                "1721860630": 0.8,
                "1721860631": 0.8,
                "1721860632": 0.8,
                "1721860633": 0.8,
                "1721860634": 0.8,
                "1721860635": 0.9,
                "1721860636": 0.8,
                "1721860637": 0.8,
                "1721860638": 0.9,
                "1721860639": 0.8,
                "1721860640": 0.8,
                "1721860641": 0.8,
                "1721860642": 0.8,
                "1721860643": 0.8,
                "1721860644": 0.8,
                "1721860645": 0.8,
                "1721860646": 0.8,
                "1721860647": 0.8,
                "1721860648": 0.8,
                "1721860649": 0.8,
                "1721860650": 0.8,
                "1721860651": 0.8,
                "1721860652": 0.8,
                "1721860653": 0.8,
            },
        }
    ],
}


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
