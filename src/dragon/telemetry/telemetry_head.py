import dragon
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate
from dragon.globalservices.node import get_list, query
import os
import sys
import time
from flask import Flask
import multiprocessing as mp
import gunicorn
import gunicorn.app.base
import dragon.telemetry.aggregator_app as aggregator_app
from dragon.telemetry.dragon_server import DragonServer
import logging

LOG = logging.getLogger(__name__)

class AggregatorApplication(gunicorn.app.base.BaseApplication):
    """Gunicorn Server implementing Aggregator App

    Args:
        gunicorn (_type_): inherit from BaseApplication, and load with custom Aggregator Application
    """

    def __init__(self, app: object, options: dict):
        """ Initialize Gunicorn server for Aggregator
        Args:
            app (object): Flask app for Aggregator with APIs
            options (dict): Server configurations.
        """

        self.options = options or {}
        app.config["queue_dict"] = self.options["queue_dict"]
        app.config["return_queue"] = self.options["return_queue"]
        app.config["result_dict"] = self.options["result_dict"]

        self.application = app
        print(f"ssh command: ssh -NL localhost:4242:localhost:4242 {os.uname().nodename}", flush=True)
        super().__init__()

    def load_config(self):
        """
        Loads config values to the application config
        """
        config = {key: value for key, value in self.options.items()
                  if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self) -> object:
        """ Loads Flask application

        Returns:
            object: Flask application
        """
        return self.application

def aggregator_server(queue_dict: dict, return_queue: object):
    """Start an instance of AggregatorApplication

    Args:
        queue_dict (dict): Dictionary mapping of hostname to queue (request queue)
        return_queue (object): Queue to retrive responses from each node
    """
    result_dict = {}

    def post_request(worker: object , req: object, environ: dict , resp: object):
        """Server hook to be called after each request to the aggregator.
        If the endpoint called is /api/telemetry_shutdown, exit gunicorn server.

        Args:
            worker (object): Gunicorn worker object
            req (object): Gunicorn request received
            environ (dict): Request environment
            resp (object): Worker response to request
        """

        if environ['RAW_URI'] == "/api/telemetry_shutdown":
            sys.exit(4)
        return

    def on_exit(server: object):
        """Server hook when server exits

        Args:
            server (object): Gunicorn server object (Arbiter)
        """
        LOG.debug(f"Exiting Aggregator on {os.uname().nodename}....")
        return

    def worker_abort(worker):
        worker.log.info("worker received SIGABRT signal")
        sys.exit(4)

    options = {
        'bind': '%s:%s' % ('localhost', '4242'), # Port binding for aggregator
        'workers': 1,
        #'errorlog': '/tmp/aggregator_gunicorn.log',
        'queue_dict': queue_dict,
        'return_queue': return_queue,
        'result_dict': result_dict,
        # 'threads': 2,
        'loglevel': 'critical',
        'on_exit': on_exit,
        'post_request': post_request,
        'worker_abort': worker_abort,
    }
    aggregator_application = AggregatorApplication(aggregator_app.app, options)
    aggregator_application.run()

def start_server(queue_dict, return_queue, shutdown_event):
    ds = DragonServer(queue_dict=queue_dict, return_queue=return_queue, shutdown_event=shutdown_event)
    ds.telemetry_handler()


def start_telemetry():

    mp.set_start_method("dragon")
    queue_dict = {}
    tsdb_collector_procs = []
    ds_procs = []
    node_list = get_list()
    node_descr_list = [query(h_uid) for h_uid in node_list]
    node_list = [descr.host_name for descr in node_descr_list]
    num_nodes = len(node_list)
    shutdown_event = mp.Event()

    for host in node_list:
        queue_dict[host] = mp.Queue()
    return_queue = mp.Queue()
    args=(queue_dict, return_queue, shutdown_event)
    cwd = os.getcwd()
    grp = ProcessGroup(restart=False, pmi_enabled=False)
    grp.add_process(nproc=num_nodes, template=ProcessTemplate(target=start_server, args=args, cwd=cwd))

    grp.init()
    grp.start()

    aggregator_proc = mp.Process(target=aggregator_server, args=[queue_dict, return_queue ])
    aggregator_proc.start()

    grp.join()
    grp.stop()
    aggregator_proc.terminate()
    aggregator_proc.join()

if __name__ == '__main__':
    start_telemetry()