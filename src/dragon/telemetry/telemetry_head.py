import dragon
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Process
from dragon.native.queue import Queue
from dragon.globalservices.node import get_list, query
from dragon.infrastructure.policy import Policy
from dragon.infrastructure.parameters import this_process
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
import cloudpickle
import socket
from dragon.dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from dragon.utils import set_local_kv, B64
from dragon.channels import Channel
from dragon.infrastructure.connection import Pipe, Connection, ConnectionOptions
import queue
from dragon.telemetry.analysis import AnalysisServer, LS_TAS_KEY
import yaml
import subprocess

log = None


def setup_logging():
    # This block turns on a client log for each client
    global log
    if log is None:
        fname = f"{dls.TELEM}_{socket.gethostname()}_head_{str(this_process.my_puid)}.log"
        setup_BE_logging(service=dls.TELEM, fname=fname)
        log = logging.getLogger(str(dls.TELEM))


class AggregatorApplication(gunicorn.app.base.BaseApplication):
    """Gunicorn Server implementing Aggregator App

    Args:
        gunicorn (_type_): inherit from BaseApplication, and load with custom Aggregator Application
    """

    def __init__(self, app: object, options: dict):
        """Initialize Gunicorn server for Aggregator
        Args:
            app (object): Flask app for Aggregator with APIs
            options (dict): Server configurations.
        """

        self.options = options or {}
        app.config["queue_dict"] = self.options["queue_dict"]
        app.config["return_queue"] = self.options["return_queue"]
        app.config["result_dict"] = self.options["result_dict"]

        self.application = app
        if options.get("remote_tunnel_node") is None:
            print(
                f"ssh command: ssh -NL {self.options['bind']}:{self.options['bind']} {os.uname().nodename}",
                flush=True,
            )
        super().__init__()

    def load_config(self):
        """
        Loads config values to the application config
        """
        config = {key: value for key, value in self.options.items() if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self) -> object:
        """Loads Flask application

        Returns:
            object: Flask application
        """
        return self.application


def aggregator_server(queue_dict: dict, return_queue: object, telemetry_cfg: object):
    """Start an instance of AggregatorApplication

    Args:
        queue_dict (dict): Dictionary mapping of hostname to queue (request queue)
        return_queue (object): Queue to retrive responses from each node
        telemetry_cfg (object): Telemetry environment config
    """
    result_dict = {}

    def when_ready(server):
        login_node = server.app.options.get("remote_tunnel_node", None)
        if login_node is not None:
            tunnel = server.app.options.get("bind")
            tunnel_port = server.app.options.get("remote_tunnel_port", None)
            ssh_command = ["ssh", "-NR", f"localhost:{tunnel_port}:{tunnel}", login_node]
            print("Port forwarded using: ", " ".join(ssh_command), flush=True)
            output = subprocess.Popen(ssh_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def post_request(worker: object, req: object, environ: dict, resp: object):
        """Server hook to be called after each request to the aggregator.
        If the endpoint called is /api/telemetry_shutdown, exit gunicorn server.

        Args:
            worker (object): Gunicorn worker object
            req (object): Gunicorn request received
            environ (dict): Request environment
            resp (object): Worker response to request
        """

        if environ["RAW_URI"] == "/api/telemetry_shutdown":
            sys.exit(4)
        return

    def on_exit(server: object):
        """Server hook when server exits

        Args:
            server (object): Gunicorn server object (Arbiter)
        """
        log.debug(f"Exiting Aggregator on {os.uname().nodename}....")
        return

    def worker_abort(worker):
        worker.log.info("worker received SIGABRT signal")
        sys.exit(4)

    # If this is a kubernetes environment, create a label and assign it to the backend pod that
    # the aggregator is running
    is_k8s = (os.getenv("KUBERNETES_SERVICE_HOST") and os.getenv("KUBERNETES_SERVICE_PORT")) != None
    if is_k8s:
        k8s_telemetry_service()

    agg_port = telemetry_cfg.get("aggregator_port", 4242)
    remote_tunnel_node = telemetry_cfg.get("remote_tunnel_node", None)
    remote_tunnel_port = telemetry_cfg.get("remote_tunnel_port", agg_port)
    options = {
        "bind": "%s:%s" % ("0.0.0.0", str(agg_port)),  # Port binding for aggregator
        "workers": 1,
        # "errorlog": "/tmp/aggregator_gunicorn.log",
        "queue_dict": queue_dict,
        "return_queue": return_queue,
        "result_dict": result_dict,
        # 'threads': 2,
        # "loglevel": "critical",
        "remote_tunnel_node": remote_tunnel_node,
        "remote_tunnel_port": remote_tunnel_port,
        "on_exit": on_exit,
        "post_request": post_request,
        "worker_abort": worker_abort,
        "when_ready": when_ready,
    }
    aggregator_application = AggregatorApplication(aggregator_app.app, options)
    setup_logging()
    aggregator_application.run()


def _register_with_local_services(serialized_slow_node_channel):
    set_local_kv(key=LS_TAS_KEY, value=serialized_slow_node_channel)


def start_server(queue_discovery, slow_node_discovery, return_queue_dict, shutdown_event, telemetry_cfg: object):
    ds_queue = Queue()
    hostname = os.uname().nodename
    queue_discovery.put((hostname, ds_queue))
    # if slow_node_service:
    # TODO: should consider a long timeout here maybe
    slow_node_channel_sdesc = slow_node_discovery.get()
    _register_with_local_services(slow_node_channel_sdesc)

    ds = DragonServer(
        input_queue=ds_queue,
        return_queue_dict=return_queue_dict,
        shutdown_event=shutdown_event,
        telemetry_config=telemetry_cfg,
    )
    ds.telemetry_handler()


def k8s_telemetry_service():
    from ..launcher.wlm.k8s import KubernetesNetworkConfig

    kubernetes = KubernetesNetworkConfig()
    pod_name = os.getenv("POD_NAME")
    patch_labels = {"metadata": {"labels": {"telemetry": f"{os.getenv('BACKEND_JOB_LABEL')}_aggregator"}}}
    kubernetes.k8s_api_v1.patch_namespaced_pod(pod_name, namespace=kubernetes.namespace, body=patch_labels)


def start_telemetry():

    setup_logging()
    mp.set_start_method("dragon")
    queue_dict = {}
    node_list = get_list()
    node_list = [query(h_uid).host_name for h_uid in node_list]
    node_list.sort()
    shutdown_event = mp.Event()
    queue_discovery = Queue()
    as_discovery = Queue()
    return_queue_aggregator = Queue()
    return_queue_as = Queue()
    return_queue_dict = {"aggregator": return_queue_aggregator, "analysis-server": return_queue_as}
    cfg_path = os.getenv("DRAGON_TELEMETRY_CONFIG", None)
    # if cfg is none pass empty dict, else pass actual cfg
    if cfg_path is None:
        telemetry_cfg = {}
    else:
        with open(cfg_path, "r") as file:
            telemetry_cfg = yaml.safe_load(file)
    args = (queue_discovery, as_discovery, return_queue_dict, shutdown_event, telemetry_cfg)
    cwd = os.getcwd()

    grp = ProcessGroup(restart=False, pmi_enabled=False)
    for hostname in node_list:
        local_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname)
        grp.add_process(nproc=1, template=ProcessTemplate(target=start_server, args=args, cwd=cwd, policy=local_policy))

    grp.init()
    grp.start()

    for _ in node_list:
        host, queue = queue_discovery.get()
        queue_dict[host] = queue

    aggregator_proc = Process(
        target=aggregator_server,
        args=[queue_dict, return_queue_aggregator, telemetry_cfg],
        policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=node_list[0]),
    )
    aggregator_proc.start()

    as_server = AnalysisServer(queue_dict, return_queue_as, as_discovery, shutdown_event, len(node_list))
    as_server_proc = Process(
        target=as_server.run,
        policy=Policy(placement=Policy.Placement.HOST_NAME, host_name=node_list[0]),
    )
    as_server_proc.start()

    log.debug("grp joining")
    grp.join()
    log.debug("grp closing")
    grp.close()
    log.debug("aggregator process terminating")
    aggregator_proc.terminate()
    log.debug("aggregator process joining")
    aggregator_proc.join()
    log.debug("slow-node process joining")
    as_server_proc.join()


if __name__ == "__main__":
    start_telemetry()
