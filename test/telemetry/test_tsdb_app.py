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
from dragon.telemetry.tsdb_app import app


def get_dps_of_each_row(filename):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()
    stmt = "SELECT * FROM datapoints"
    result = cursor.execute(stmt).fetchall()
    final = {}
    for row in result:
        if row[0] in final.keys():
            final[row[0]].update({row[1]: row[2]})
        else:
           final[row[0]] = {row[1]: row[2]} 
    
    return final


class TestDragonTelemetryTSDBApp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.shutdown_event = mp.Event()
        user = os.environ.get("USER", str(os.getuid()))
        cls.filename = "/tmp/ts_" + user + "_" + os.uname().nodename + ".db"
        connection = sqlite3.connect(cls.filename)
        cursor = connection.cursor()
        sql_create_metrics = "CREATE TABLE IF NOT EXISTS datapoints (metric text, timestamp text, value real, tags json)"
        cursor.execute(sql_create_metrics)
        connection.commit()
        sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
        cursor.execute(sql_create_flags)
        connection.commit()

        sql_insert = "INSERT INTO datapoints VALUES (?,?,?,?)"
        cls.sample_data = {
            "load_average": {"1722010526": 0.58, "1722010527": 0.58, "1722010528": 0.62},
            "used_RAM": {"1722010526": 9.3, "1722010527": 9.3, "1722010528": 9.3},
            "cpu_percent": {"1722010526": 0.8, "1722010527": 1.6, "1722010528": 0.8},
            "num_running_processes": {"1722010526": 21, "1722010527": 20, "1722010528": 23},
            "def_pool_utilization": {"1722010526": 0.003910064, "1722010527": 0.00093, "1722010528": 0.001234},
        }
        for k, v in cls.sample_data.items():
            for t, d in v.items():
                cursor.execute(sql_insert, [k, t, d, json.dumps(None)])
        connection.commit()

        # Shutdown event
        sql_insert_flag_event = "INSERT OR REPLACE INTO flags VALUES (1, ?)"
        cursor.execute(sql_insert_flag_event, [dumps(cls.shutdown_event)])
        connection.commit()
        connection.close()

        cls.context = app.app_context()
        cls.context.push()
        cls.client = app.test_client()

    def setUp(self):
        pass

    def test_check_is_ready(self):
        response = self.client.get("/api/ready")

        self.assertEqual(response.status_code, 200)
        self.assertEqual((json.loads(response.get_data(as_text=True)))["is_ready"], True)

    def test_add_dps_with_timestamps(self):
        pre_mock_db = get_dps_of_each_row(self.filename)
        query = {
            "dps": [
                {"metric": "load_average", "value": 16.96},
                {"metric": "used_RAM", "value": 27.1},
                {"metric": "cpu_percent", "value": 10.5},
                {"metric": "num_running_processes", "value": 25},
                {"metric": "def_pool_utilization", "value": 0.003910064697265625},
            ],
            "timestamp": 1733466126,
        }
        
        response = self.client.post("/api/metrics", json=query)
        post_mock_db = get_dps_of_each_row(self.filename)

        self.assertEqual(response.status_code, 201)
        self.assertEqual((json.loads(response.get_data(as_text=True)))["success"], len(query["dps"]))
        for k, v in post_mock_db.items():
            with self.subTest(k):
                self.assertEqual(len(v), len(pre_mock_db[k]) + 1)

    def test_add_dps_without_timestamp(self):
        pre_mock_db = get_dps_of_each_row(self.filename)
        query = {
            "dps": [
                {"metric": "load_average", "value": 16.96},
                {"metric": "used_RAM", "value": 27.1},
                {"metric": "cpu_percent", "value": 10.5},
                {"metric": "num_running_processes", "value": 25},
                {"metric": "def_pool_utilization", "value": 0.003910064697265625},
            ]
        }
        response = self.client.post("/api/metrics", json=query)
        post_mock_db = get_dps_of_each_row(self.filename)

        self.assertEqual(response.status_code, 201)
        self.assertEqual((json.loads(response.get_data(as_text=True)))["success"], len(query["dps"]))
        for k, v in post_mock_db.items():
            with self.subTest(k):
                self.assertEqual(len(v), len(pre_mock_db[k]) + 1)

    def test_add_new_metrics(self):
        pre_mock_db = get_dps_of_each_row(self.filename)
        query = {
            "dps": [
                {"metric": "new_metric", "value": 123.456},
                {"metric": "used_RAM", "value": 27.1},
                {"metric": "cpu_percent", "value": 10.5},
                {"metric": "num_running_processes", "value": 25},
                {"metric": "def_pool_utilization", "value": 0.003910064697265625},
            ]
        }
        response = self.client.post("/api/metrics", json=query)
        post_mock_db = get_dps_of_each_row(self.filename)
        self.assertEqual(response.status_code, 201)
        self.assertEqual((json.loads(response.get_data(as_text=True)))["success"], len(query["dps"]))
        self.assertEqual(len(post_mock_db.keys()), len(pre_mock_db.keys()) + 1)

    def test_set_shutdown(self):
        response = self.client.get("/api/set_shutdown")
        assert self.shutdown_event.is_set()
        self.assertEqual(json.loads(response.get_data(as_text=True))["success"], 1)
        self.assertEqual(response.status_code, 200)
        self.shutdown_event.clear()

    def test_telemetry_shutdown(self):
        response = self.client.get("/api/telemetry_shutdown")

        self.assertEqual(response.status_code, 200)
        self.assertEqual((json.loads(response.get_data(as_text=True)))["shutdown"], True)

    def test_tsdb_cleanup(self):
        pre_mock_db = get_dps_of_each_row(self.filename)
        query = {"start_time": 1722010527}
        response = self.client.post("/api/tsdb_cleanup", json=query)
        post_mock_db = get_dps_of_each_row(self.filename)
        self.assertEqual(response.status_code, 201)
        for k, v in post_mock_db.items():
            if k != "new_metric":
                with self.subTest(k):
                    self.assertEqual(len(v), len(pre_mock_db[k]) - 1)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.context.pop()
        os.remove(cls.filename)


class TestDragonTelemetryTSDBAppErrors(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.shutdown_event = mp.Event()
        user = os.environ.get("USER", str(os.getuid()))
        cls.filename = "/tmp/ts_" + user + "_" + os.uname().nodename + ".db"
        connection = sqlite3.connect(cls.filename)
        cursor = connection.cursor()
        sql_create_metrics = "CREATE TABLE IF NOT EXISTS metrics (metric varchar(50), tags varchar(30), dps json)"
        cursor.execute(sql_create_metrics)
        connection.commit()
        sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
        cursor.execute(sql_create_flags)
        connection.commit()

        sql_insert = "INSERT INTO metrics VALUES (?,?,?)"
        cls.sample_data = {"load_average": {"1722010526": 0.58, "1722010527": 0.58, "1722010528": 0.62}}
        for k, v in cls.sample_data.items():
            cursor.execute(sql_insert, [k, None, json.dumps(v)])
        connection.commit()

        # Shutdown event
        sql_insert_flag_event = "INSERT OR REPLACE INTO flags VALUES (1, ?)"
        cursor.execute(sql_insert_flag_event, [dumps(cls.shutdown_event)])
        connection.commit()
        connection.close()

        cls.context = app.app_context()
        cls.context.push()
        cls.client = app.test_client()

    def test_tsdb_cleanup_error(self):
        pass

    @unittest.skip("AICI-1625 need to resolve error handling in this test")
    @patch("dragon.telemetry.tsdb_app.get_metrics_from_db")
    def test_add_metrics_error_response(self, mock_metrics):
        mock_metrics.return_value = ["load_average"]
        query = {
            "dps": [
                {"metric": "load_average", "value": 16.96},
                {"metric": "used_RAM", "value": 27.1},
                {"metric": "cpu_percent", "value": 10.5},
                {"metric": "num_running_processes", "value": 25},
                {"metric": "def_pool_utilization", "value": 0.003910064697265625},
            ]
        }
        response = self.client.post("/api/metrics", json=query)
        res = json.loads(response.get_data(as_text=True))
        to_pass = len([i for i in query["dps"] if i["metric"] == "load_average"])
        to_fail = len([i for i in query["dps"] if i["metric"] != "load_average"])
        self.assertEqual(res["failed"], to_fail)
        self.assertEqual(res["success"], to_pass)
        self.assertEqual(len(res["errors"]), to_fail)

    def test_set_shutdown_error(self):
        pass

    @classmethod
    def tearDownClass(cls):
        cls.context.pop()
        os.remove(cls.filename)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
