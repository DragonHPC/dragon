import unittest
import dragon
import multiprocessing as mp
import queue
import cloudpickle
import time
import threading
import os
import sqlite3
import copy
import glob

from dragon.utils import set_local_kv, B64
from dragon.telemetry.analysis import (
    AnalysisClient,
    AnalysisServer,
    LS_TAS_KEY,
    CollectorDetectorGroup,
    Detector,
    CollectorGroup,
)
from dragon.telemetry.telemetry_head import _register_with_local_services
from dragon.data import DDict
from telemetry.telemetry_data import (
    BASE_CLIENT_QUERY,
    BASE_DRAGON_SERVER_RESPONSE,
    BASE_GRAFANA_QUERY,
)

from dragon.telemetry.dragon_server import DragonServer
from dragon.telemetry.tsdb_server import tsdb


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
        tac.get_data(
            BASE_CLIENT_QUERY["metrics"][0],
            tags=BASE_CLIENT_QUERY["tags"][0],
            start_time=BASE_CLIENT_QUERY["start_time"],
        )
        output_query, client_queue = self.server_queue.get()
        self.assertEqual(output_query, BASE_CLIENT_QUERY)
        test_msg = 42
        tac._data_queue.put(test_msg)
        cq_result = client_queue.get()
        self.assertEqual(cq_result, test_msg)


class SampleDetector(Detector):

    def __init__(
        self,
        test_queue: mp.Queue = None,
        test_ddict: DDict = None,
    ):
        super().__init__(
            metric_name=BASE_CLIENT_QUERY["metrics"][0],
            sacred_nodes=["test_node0001", "test_node0002"],
            restartable_ddicts=[test_ddict],
            analysis_args=(
                42,
                test_queue,
            ),
        )

    def analyze_data(self, data: list, analysis_args: tuple) -> tuple:

        test_float, test_queue = analysis_args

        # test the analysis args are correctly passed
        if test_float != 42:
            raise ValueError("Invalid analysis args")

        test_queue.put(data)

        return True, ["test_node0002", "test_node0003"]

    # patching trigger restart to test the restartable ddicts but not actually restart the runtime
    @staticmethod
    def trigger_restart(nodes_to_remove: list, sacred_nodes: list, restartable_ddicts: list) -> None:
        test_queue = restartable_ddicts[0]["test_queue"]
        for dd in restartable_ddicts:
            dd.destroy(allow_restart=True)
        set_to_remove = set(nodes_to_remove)
        set_sacred_nodes = set(sacred_nodes)
        removing = list(set_to_remove - set_sacred_nodes)
        test_queue.put(removing)


class TestDragonTelemetryDetector(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.server_queue = mp.Queue()
        cls.test_queue = mp.Queue()
        cls.test_ddict = DDict(n_nodes=1, managers_per_node=1, total_mem=10 * 1024 * 1024)
        analysis_comm_sdesc = B64.bytes_to_str(cloudpickle.dumps(cls.server_queue))
        _register_with_local_services(analysis_comm_sdesc)

    @classmethod
    def tearDownClass(cls):
        # need to cleanup ddict that was destroyed with restart_allowed=True
        for filename in glob.glob("/dev/shm/_dragon_dragon_dict_pool*"):
            try:
                os.remove(filename)
            except:
                pass

    def thread_stop_helper(self, queue, event):
        while not event.is_set():
            queue.put([BASE_DRAGON_SERVER_RESPONSE])

    def test_analysis_detector_good_path(self):

        self.test_ddict["test"] = "I have friends everywhere"
        self.test_ddict["test_queue"] = self.test_queue
        detector = SampleDetector(test_queue=self.test_queue, test_ddict=self.test_ddict)
        # get the first request from the detectors analysis client(tac)
        start_time = time.time()
        detector.start()
        request, tac_return_queue = self.server_queue.get()
        self.assertEqual(request["metrics"], BASE_CLIENT_QUERY["metrics"])
        self.assertEqual(request["tags"], ["host"])
        self.assertTrue(
            int(request["start_time"]) >= int(start_time),
            msg="{} is not greater than {}".format(int(request["start_time"]), int(start_time)),
        )
        self.assertEqual(request["type"], BASE_CLIENT_QUERY["type"])
        # check the query
        # return a response to the tac. this will be handed to the analyze_data method above
        tac_return_queue.put([BASE_DRAGON_SERVER_RESPONSE])
        # check that the data is correctly passed to the analyze_data method
        data = self.test_queue.get()
        self.assertEqual(data[0]["dps"], BASE_DRAGON_SERVER_RESPONSE["result"][0]["dps"])
        # check that the patched restart trigger is passed the correct data and gets most of the correct stuff
        removed_node = self.test_queue.get()
        self.assertEqual(removed_node, ["test_node0003"])
        th = threading.Thread(target=self.thread_stop_helper, args=(tac_return_queue, detector._watcher_end_event))
        th.start()
        detector.stop()
        th.join()
        self.test_ddict.destroy()


class SampleCollectorGroup(CollectorGroup):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def collector_test(collector_args):

        test_counter = collector_args[0]
        with test_counter.get_lock():
            test_counter.value += 1

        return 8


class TestDragonTelemetryCollectorGroup(unittest.TestCase):

    def setUp(self):
        user = os.environ.get("USER", str(os.getuid()))
        self.filename = "/tmp/ts_" + user + "_" + os.uname().nodename + ".db"
        os.environ["DRAGON_TELEMETRY_LEVEL"] = "1"
        self.tsdb_start_event = mp.Event()
        self.tsdb_end_event = mp.Event()
        self.counter = mp.Value("i", 0)
        self.collector_args = (self.counter,)
        self.telem_cfg = {}
        self.ds = DragonServer(
            input_queue=None,
            return_queue_dict=None,
            shutdown_event=self.tsdb_end_event,
            telemetry_config=self.telem_cfg,
        )
        self.collector_group = SampleCollectorGroup(per_gpu=False, collector_args=self.collector_args)

    def tearDown(self):
        try:
            os.remove(self.filename)
        except:
            pass

    @staticmethod
    def tsdb_thread_helper(start_event, end_event, telem_cfg):
        tsdb(start_event=start_event, shutdown_event=end_event, telemetry_cfg=telem_cfg)

    def check_db(self, counter, start_time):
        # This function should check the database to ensure that the collector group has put data in it
        # For simplicity, we will just check the counter value
        self.assertTrue(counter.value > 1, msg="Collector group did not increment the counter as expected.")
        connection = sqlite3.connect(self.filename)
        cursor = connection.cursor()
        query_from_grafana = copy.deepcopy(BASE_GRAFANA_QUERY)
        uid = str(int(time.time() * 100)) + "_" + str(10)
        query_from_grafana["queries"][0]["metric"] = self.collector_group.metric_name
        query_from_grafana["req_id"] = uid
        query_from_grafana["type"] = "query"
        query_from_grafana["start"] = str(start_time * 1000)
        query_from_grafana["end"] = str((start_time + 300) * 1000)
        res = self.ds.query(query_from_grafana, connection, cursor)
        self.assertIsInstance(res, list)
        self.assertTrue(len(res[0]["dps"]) == counter.value, msg="No data returned from the database.")
        self.assertTrue(
            all([val == 8 for val in res[0]["dps"].values()]), msg="Data in the database does not match expected data."
        )

    def test_analysis_collector_group_good_path(self):

        # start tsdb server to handle putting data in from the collector group
        # this has to be a separate process because the tsdb server needs to be run in the main thread for signals it relies on to work. See error produced for more details.
        tsdb_proc = mp.Process(
            target=self.tsdb_thread_helper,
            args=(
                self.tsdb_start_event,
                self.tsdb_end_event,
                self.telem_cfg,
            ),
        )
        tsdb_proc.start()
        start_time = int(time.time())
        self.collector_group.start()
        # sleep for a bit so that collector group can collect some data
        time.sleep(3 * self.collector_group.collection_rate)
        self.collector_group.stop()
        self.check_db(self.counter, start_time)
        self.ds.shutdown_telemetry_server()
        tsdb_proc.terminate()
        tsdb_proc.join()


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
