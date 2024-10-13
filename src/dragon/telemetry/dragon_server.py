import dragon
from dragon.globalservices.node import get_list
from dragon.infrastructure.policy import Policy
import os
import requests
import multiprocessing as mp
import queue
import sqlite3
import time
import socket
import json
from pickle import loads
import logging

LOG = logging.getLogger(__name__)

from dragon.telemetry.tsdb_server import tsdb
from dragon.telemetry.collector import Collector


class DragonServer():
    """ Dragon Server retrieves requests from associated queue, performs queries, and returns response
    to return queue.
    """
    def __init__(self, queue_dict: dict, return_queue: object, shutdown_event: object):
        """Inititialize Dragon Server object.

        Args:
            queue_dict (dict): Dictionary for each node and its associated request queue.
            return_queue (object): Queue where response to requests is sent to
            shutdown_event (object): Used to signal shutdown
        """
        self.queue_dict = queue_dict
        self.return_queue = return_queue
        self.shutdown_event = shutdown_event
        # need to define database file etc
        self.listen_procs = []
        self.telemetry_level = int(os.getenv('DRAGON_TELEMETRY_LEVEL', 0))

    def telemetry_handler(self):
        """Starts Dragon Server on each compute node
        """
        mp.set_start_method('dragon')
        collector_start_event = mp.Event()
        try:
            with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
                tsdb_proc = mp.Process(target=tsdb, args=(collector_start_event, self.shutdown_event,))
                tsdb_proc.start()

                if self.telemetry_level > 1:
                    c = Collector()
                    collector_proc = mp.Process(target=c.collect, args=[collector_start_event, self.shutdown_event])
                    collector_proc.start()

                ds_proc = mp.Process(target=self._listen)
                ds_proc.start()

            if self.telemetry_level > 1:
                collector_proc.join()
            ds_proc.join()
            # try to shutdown telemetry server gently
            self.shutdown_aggregator_server()
            self.shutdown_telemetry_server()
            tsdb_proc.terminate()
            tsdb_proc.join()

        except Exception as e:
            LOG.warn(f"Error in DragonServer: {e}")


    def _listen(self):
        """ Check request queue for new requests
            Return response to return queue
        """
        hostname = os.uname().nodename
        tmdb_directory = str(os.getenv("DRAGON_DEFAULT_TMDB_DIR", "/tmp"))
        filename = tmdb_directory + "/ts_" + os.uname().nodename + ".db"
        receive_queue = self.queue_dict[hostname]
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()


        # Create metrics table
        sql_create_metrics = "CREATE TABLE IF NOT EXISTS metrics (metric varchar(50), dps json)"
        cursor.execute(sql_create_metrics)
        connection.commit()
        sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
        cursor.execute(sql_create_flags)
        connection.commit()
        LOG.debug(f"Listen DragonServer object on {hostname}")
        while True:
            try:
                request_body = receive_queue.get(timeout=1)
                result = {}
                request_type = request_body["type"]
                result["type"] = request_type
                if request_type == "suggest":
                    res = self.get_metrics_from_db(cursor)
                elif request_type == "query":
                    res = self.query(request_body, connection, cursor)
                else:
                    res = []
                request_body["result"] = res

                self.return_queue.put(request_body)

                if self.check_is_shutdown(cursor):
                    LOG.debug(f"Shutting down Dragon Server on {hostname}...")
                    break

            except queue.Empty as e:
                if self.shutdown_event.is_set():
                    LOG.debug(f"Shutting down Dragon Server on {hostname}...")
                    break
                time.sleep(0.1)
                continue
            except Exception as e:
                LOG.warn(f"Exception caught: {e}")

    def get_metrics_from_db(self, cursor: object) -> list:
        """ Retrieve distinct metrics from database

        Args:
            cursor (object): SQLite3 Cursor object to execute SQL statements

        Returns:
            list: distinct metrics from metrics table
        """
        sql = "SELECT DISTINCT(metric) FROM metrics"
        res = list(cursor.execute(sql))
        res = [x[0] for x in res]
        return res

    def check_is_shutdown(self, cursor: object) -> bool:
        """ Check flags table if shutdown event is set

        Args:
            cursor (object): SQLite3 Cursor object to execute SQL statements

        Returns:
            bool: True if shutdown event is set, else False
        """
        sql = "SELECT is_shutdown FROM flags LIMIT 1"
        shutdown_event = loads(list(cursor.execute(sql).fetchone())[0])
        return shutdown_event.is_set()

    def shutdown_telemetry_server(self):
        """ Signal local TSDBServer to begin shutdown sequence
        """
        shutdown_url = "http://localhost:4243/api/telemetry_shutdown"
        _ = requests.get(shutdown_url)
        return

    def shutdown_aggregator_server(self):
        """ Send request to Aggregator signaling shutdown.
        May return RequestException if Aggregator is not on that specific compute node.
        """

        shutdown_url = "http://localhost:4242/api/telemetry_shutdown"
        try:
            _ = requests.get(shutdown_url)
        except requests.exceptions.RequestException as e:
            # Aggregator isn't on this node
            pass
        return


    def query(self, request_body: dict, connection: object, cursor: object) -> dict:
        """_summary_ Query local database
        param request_body: Request JSON for query
        param connection: sqlite3 connection
        param cursor: sqlite3 cursor

        Args:
            request_body (dict): Request JSON for query
            connection (object): SQLite3 connection
            cursor (object): SQLite3 cursor

        Returns:
            dict: Result with time series data
        """
        def transform_select_result(tsdb: list) -> list:
            """Transform rows retrieved from database/table

            Args:
                tsdb (list): Rows from SQLite3 table

            Returns:
                list: Transformed rows
            """
            result = []
            for row in tsdb:
                res = {}
                res["dps"] = json.loads(row[1])
                result.append(res)
            return result

        hostname = os.uname().nodename
        result = []
        queries = request_body["queries"]

        # Grafana sends request in milliseconds. But expects response in seconds :)
        # New Info: (Milliseconds can be changed in dashboard settings!)
        start_time = int(request_body["start"])/1000

        metric_list = self.get_metrics_from_db(cursor)
        for q in queries:
            res = {}
            dps = {}
            # return query request body as part of response
            if request_body["showQuery"] == True:
                res["query"] = q
                res["query"]["index"] = queries.index(q)
            res["metric"] = q["metric"]
            res["tags"] =  {"host": hostname}
            res["aggregateTags"]= []
            if q["metric"] in metric_list:
                tsdb = cursor.execute("SELECT * FROM metrics where metric = ?", [q["metric"]]).fetchall()
                tsdb = transform_select_result(tsdb)
                for x in tsdb:
                # Filter datapoints by start time
                    dps_from_start = {k:v for k,v in x["dps"].items() if int(k) >= start_time }
                    dps.update(dps_from_start)
            res["dps"] = dps
            result.append(res)
        return result


