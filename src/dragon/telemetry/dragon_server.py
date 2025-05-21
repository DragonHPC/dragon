import dragon
from dragon.globalservices.node import get_list
from dragon.infrastructure.policy import Policy
from dragon.infrastructure.parameters import this_process
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
import stat

LOG = logging.getLogger(__name__)

from dragon.telemetry.tsdb_server import tsdb, ENFORCED_DB_PERMISSIONS
from dragon.telemetry.collector import Collector
from dragon.dlogging.util import setup_BE_logging, DragonLoggingServices as dls

log = None


class DragonServer:
    """Dragon Server retrieves requests from associated queue, performs queries, and returns response
    to return queue.
    """

    def __init__(
        self,
        input_queue: mp.Queue,
        return_queue_dict: object,
        shutdown_event: object,
        telemetry_config: object,
    ):
        """Inititialize Dragon Server object.

        Args:
            input_queue (Queue): Nodes request queue.
            return_queue (object): Queue where response to requests is sent to
            shutdown_event (object): Used to signal shutdown
            telemetry_config (object): Used to pass along configuration details
        """
        self.input_queue = input_queue
        self.return_queue_dict = return_queue_dict
        self.shutdown_event = shutdown_event
        self.listen_procs = []
        self.telem_cfg = telemetry_config
        self.telemetry_level = int(os.getenv("DRAGON_TELEMETRY_LEVEL", 0))
        self.setup_logging()

    def setup_logging(self):
        # This block turns on a client log for each client
        global log
        if log is None:
            fname = f"{dls.TELEM}_{socket.gethostname()}_ds_{str(this_process.my_puid)}.log"
            setup_BE_logging(service=dls.TELEM, fname=fname)
            log = logging.getLogger(str(dls.TELEM))

    def telemetry_handler(self):
        """Starts Dragon Server on each compute node"""
        mp.set_start_method("dragon")
        collector_start_event = mp.Event()
        try:
            with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
                tsdb_proc = mp.Process(
                    target=tsdb,
                    args=(collector_start_event, self.shutdown_event, self.telem_cfg),
                )
                tsdb_proc.start()

                if self.telemetry_level > 1:
                    c = Collector(self.telem_cfg)
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
        """Check request queue for new requests
        Return response to return queue
        """
        self.setup_logging()
        hostname = os.uname().nodename
        user = os.environ.get("USER", str(os.getuid()))
        tmdb_directory = str(self.telem_cfg.get("default_tmdb_directory", "/tmp"))
        filename = os.path.join(tmdb_directory, "ts_" + user + "_" + os.uname().nodename + ".db")
        log.debug(f"opening db at ")
        connection = sqlite3.connect(filename)
        cursor = connection.cursor()

        # Create metrics table
        sql_create_metrics = "CREATE TABLE IF NOT EXISTS datapoints (metric text, timestamp text, value real, tags json)"
        cursor.execute(sql_create_metrics)
        connection.commit()
        sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
        cursor.execute(sql_create_flags)
        connection.commit()
        log.debug(f"Listen DragonServer object on {hostname}")
        db_permissions = stat.S_IMODE(os.stat(filename).st_mode)
        if db_permissions != ENFORCED_DB_PERMISSIONS:
            os.chmod(filename, ENFORCED_DB_PERMISSIONS)
        while not self.shutdown_event.is_set():
            try:
                request_body = self.input_queue.get(timeout=1)
                log.debug(f"{request_body=}")
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

                log.debug("putting response into return queue")
                self.return_queue_dict[request_body["return_queue"]].put(request_body)

            except queue.Empty as e:
                continue
            except Exception as e:
                LOG.warn(f"Query Exception caught: {e}")

        LOG.debug(f"Dragon Server on {hostname} is exiting")

    def get_metrics_from_db(self, cursor: object) -> list:
        """Retrieve distinct metrics from database

        Args:
            cursor (object): SQLite3 Cursor object to execute SQL statements

        Returns:
            list: distinct metrics from metrics table
        """
        sql = "SELECT DISTINCT(metric) FROM datapoints"
        res = list(cursor.execute(sql))
        res = [x[0] for x in res]
        return res

    def check_is_shutdown(self, cursor: object) -> bool:
        """Check flags table if shutdown event is set

        Args:
            cursor (object): SQLite3 Cursor object to execute SQL statements

        Returns:
            bool: True if shutdown event is set, else False
        """
        sql = "SELECT is_shutdown FROM flags LIMIT 1"
        shutdown_event = loads(list(cursor.execute(sql).fetchone())[0])
        return shutdown_event.is_set()

    def shutdown_telemetry_server(self):
        """Signal local TSDBServer to begin shutdown sequence"""
        tsdb_port = str(self.telem_cfg.get("tsdb_server_port", "4243"))
        shutdown_url = f"http://localhost:{tsdb_port}/api/telemetry_shutdown"
        _ = requests.get(shutdown_url)
        return

    def shutdown_aggregator_server(self):
        """Send request to Aggregator signaling shutdown.
        May return RequestException if Aggregator is not on that specific compute node.
        """
        agg_port = str(self.telem_cfg.get("aggregator_port", "4242"))
        shutdown_url = f"http://localhost:{agg_port}/api/telemetry_shutdown"
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
        def create_dps_tags(tsdb: list) -> dict:
            """Transform rows retrieved from database/table

            Args:
                tsdb (list): Rows from SQLite3 table

            Returns:
                dict: Transformed rows
            """
            result = {}
            for row in tsdb:
                # row -> (metric, timestamp, value, tag_key, tag_value)
                try:
                    result[str(row[4])][row[1]] = row[2]
                except KeyError:
                    result[str(row[4])] = {row[1]: row[2]} 
            return result
        

        hostname = os.uname().nodename
        result = []
        queries = request_body["queries"]

        # Grafana sends request in milliseconds. But expects response in seconds :)
        # New Info: (Milliseconds can be changed in dashboard settings!)
        start_time = int(request_body["start"])/1000
        end_time = request_body.get("end", int(time.time()))

        metric_list = self.get_metrics_from_db(cursor)
        for q in queries:
            if q["metric"] in metric_list:
                # tags are deprecated in opentsdb 2.3
                # check if filters exists, then check if tagk exists
                # TODO: a better way to validate input - and return a clean reponse
                if "filters" in q and len(q["filters"]) > 0:
                    tagk = [f["tagk"] for f in q["filters"] if ("tagk" in f and f["tagk"] != "host")]
                    if len(tagk) == 0:
                        tagk = None
                    else:
                        tagk = tagk[0]
                else:
                    tagk = None
                # Filter datapoints by start time
                if tagk is None:
                    sql_query = "SELECT dps.metric, dps.timestamp, dps.value, json_each.key as tag_key, json_each.value as tag_value FROM datapoints as dps, json_each(dps.tags) WHERE dps.metric = ? AND CAST(timestamp as INTEGER) >= ? AND CAST(timestamp as INTEGER) <= ? ORDER BY tag_value"
                    tsdb = cursor.execute(sql_query, [q["metric"], start_time, end_time]).fetchall()
                else:
                    sql_query = "SELECT dps.metric, dps.timestamp, dps.value, json_each.key as tag_key, json_each.value as tag_value FROM datapoints as dps, json_each(dps.tags) WHERE dps.metric = ? AND tag_key = ? AND CAST(timestamp as INTEGER) >= ? AND CAST(timestamp as INTEGER) <= ? ORDER BY tag_value"
                    tsdb = cursor.execute(sql_query, [q["metric"], tagk, start_time, end_time]).fetchall()

                
                tsdb = create_dps_tags(tsdb)

                for tsdb_tag, tsdb_dps in tsdb.items():
                    res = {}
                    # return query request body as part of response
                    if tsdb_tag == "None":
                        tsdb_tag = None
                    if request_body["showQuery"] == True:
                        res["query"] = q
                        res["query"]["index"] = queries.index(q)
                    res["metric"] = q["metric"]
                    res["tags"] =  {"host": hostname}
                    res["aggregateTags"]= []
                    res["dps"] = tsdb_dps
                    if tagk != None:
                        res["tags"].update({tagk: tsdb_tag})
                    result.append(res)
        return result
