import unittest
from unittest.mock import patch, MagicMock
import dragon
import multiprocessing as mp
import time
import json
import os
import socket
from threading import get_ident
import sqlite3
import pickle
import copy

from dragon.globalservices.node import get_list, query
from dragon.telemetry.dragon_server import DragonServer
from dragon.infrastructure.policy import Policy

from telemetry.telemetry_data import SAMPLE_DATA, BASE_GRAFANA_QUERY

class TestDragonTelemetryDragonServer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.input_queue = mp.Queue()
        cls.return_queue_aggregator = mp.Queue()
        cls.return_queue_as = mp.Queue()
        cls.shutdown_event = mp.Event()
        cls.return_queue_dict = {"aggregator": cls.return_queue_aggregator, "analysis-server": cls.return_queue_as}
        cls.telemetry_cfg = {}
        cls.ds = DragonServer(
            input_queue=cls.input_queue,
            return_queue_dict=cls.return_queue_dict,
            shutdown_event=cls.shutdown_event,
            telemetry_config=cls.telemetry_cfg,
        )
        cls.ds_proc = mp.Process(target=cls.ds._listen)
        cls.ds_proc.start()
        user = os.environ.get("USER", str(os.getuid()))
        cls.filename = "/tmp/ts_" + user + "_" + os.uname().nodename + ".db"
        cls.add_data_to_DB()
        cls.add_shutdown_event_to_DB()

    @classmethod
    def tearDownClass(cls):
        cls.shutdown_event.set()
        cls.ds_proc.join()
        # os.remove(cls.filename)

    @classmethod
    def add_shutdown_event_to_DB(cls):
        connection = sqlite3.connect(cls.filename)
        cursor = connection.cursor()
        sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
        cursor.execute(sql_create_flags)
        connection.commit()
        sql_insert_flag_event = "INSERT OR REPLACE INTO flags VALUES (1, ?)"
        cursor.execute(sql_insert_flag_event, [pickle.dumps(cls.shutdown_event)])
        connection.commit()

    @classmethod
    def add_data_to_DB(cls):
        connection = sqlite3.connect(cls.filename)
        cursor = connection.cursor()
        sql_create_metrics = "CREATE TABLE IF NOT EXISTS datapoints (metric text, timestamp text, value real, tags json)"
        cursor.execute(sql_create_metrics)
        connection.commit()
        sql_insert = "INSERT INTO datapoints VALUES (?,?,?,?)"
        for k, v in SAMPLE_DATA.items():
            for t, d in v.items():
                cursor.execute(sql_insert, [k, t, d, json.dumps(None)])
        connection.commit()
        connection.close()

    def test_dragon_server_shutdown(self):
        self.assertTrue(self.ds_proc.is_alive())
        self.assertTrue(not self.shutdown_event.is_set())
        self.shutdown_event.set()
        self.ds_proc.join()
        self.assertTrue(not self.ds_proc.is_alive())

    def test_dragon_server_data_response(self):
        query_from_grafana = copy.deepcopy(BASE_GRAFANA_QUERY)
        for key in list(SAMPLE_DATA.keys()):
            uid = str(int(time.time() * 100)) + "_" + str(get_ident())
            query_from_grafana["queries"][0]["metric"] = key
            query_from_grafana["req_id"] = uid
            query_from_grafana["type"] = "query"
            self.input_queue.put(query_from_grafana)
            response = self.return_queue_aggregator.get(timeout=5)
            self.assertEqual(SAMPLE_DATA[key], response["result"][0]["dps"])

    def test_dragon_server_bad_metric(self):
        query_from_grafana = copy.deepcopy(BASE_GRAFANA_QUERY)
        query_from_grafana["queries"][0]["metric"] = "bad_metric_name"
        uid = str(int(time.time() * 100)) + "_" + str(get_ident())
        query_from_grafana["req_id"] = uid
        query_from_grafana["type"] = "query"
        self.input_queue.put(query_from_grafana)
        response = self.return_queue_aggregator.get()
        self.assertEqual(0, len(response["result"]))

    def test_dragon_server_bad_times(self):
        # times after metrics
        query_from_grafana = copy.deepcopy(BASE_GRAFANA_QUERY)
        query_from_grafana["start"] = "1722010529000"
        uid = str(int(time.time() * 100)) + "_" + str(get_ident())
        query_from_grafana["req_id"] = uid
        query_from_grafana["type"] = "query"
        self.input_queue.put(query_from_grafana)
        response = self.return_queue_aggregator.get()
        self.assertFalse(response["result"])

    @patch("dragon.telemetry.dragon_server.subprocess.Popen")
    def test_db_dump(self, mock_popen):
        # Mock the process object and its communicate method
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"stdout", b"stderr")
        mock_process.returncode = 0
        mock_popen.return_value = mock_process

        # Set necessary config
        self.ds.telem_cfg = {"dump_node": "test_node", "dump_dir": "/test/dir"}
        
        # Call the method
        result = self.ds.db_dump()

        # Assertions
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["message"], "Database dumped successfully.")
        
        # Verify rsync command structure
        args, _ = mock_popen.call_args
        cmd = args[0]
        self.assertEqual(cmd[0], "rsync")
        self.assertEqual(cmd[1], "--mkpath")
        # Check if destination contains the node and path
        self.assertIn("test_node:/test/dir/telemetry/", cmd[3])

    @patch("dragon.telemetry.dragon_server.subprocess.Popen")
    def test_db_dump_failure(self, mock_popen):
        # Mock the process object for failure
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"rsync error")
        mock_process.returncode = 1
        mock_popen.return_value = mock_process

        # Set necessary config
        self.ds.telem_cfg = {"dump_node": "test_node", "dump_dir": "/test/dir"}
        
        # Call the method
        result = self.ds.db_dump()

        # Assertions
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["message"], "rsync error")
    
    def test_query_mini_telemetry(self):
        # Setup DragonServer with mini_telemetry_args
        mini_args = (["node1"], "merged_db")
        ds = DragonServer(
            input_queue=mp.Queue(),
            return_queue_dict={},
            shutdown_event=mp.Event(),
            telemetry_config={},
            mini_telemetry_args=mini_args
        )

        # Setup in-memory DB with expected schema for mini telemetry
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        # Note: table_name column added
        cursor.execute("CREATE TABLE datapoints (table_name text, metric text, timestamp text, value real, tags json)")
        
        # Insert sample data
        # metric="cpu", timestamp="1000", value=1.0, tags={"host": "node1"}, table_name="node1"
        cursor.execute(
            "INSERT INTO datapoints VALUES (?, ?, ?, ?, ?)", 
            ("node1", "cpu", "1000", 1.0, json.dumps({"host": "node1"}))
        )
        conn.commit()

        # Prepare request body
        # start time is divided by 1000 in query method, so 0 -> 0. Timestamp 1000 > 0.
        request_body = {
            "start": "0",
            "queries": [
                {
                    "metric": "cpu",
                    "filters": [] 
                }
            ]
        }

        # Execute query
        result = ds.query(request_body, conn, cursor)

        # Assertions
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["metric"], "cpu")
        # In mini telemetry mode, host tag comes from table_name which is index 1 of the tuple key
        self.assertEqual(result[0]["tags"]["host"], "node1")
        self.assertEqual(result[0]["dps"]["1000"], 1.0)

if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
