from flask import Flask, Response, request
import queue
import json
import time
import os
from itertools import chain
from threading import get_ident
import logging
from http import HTTPStatus
from flask_jwt_extended import JWTManager, create_access_token, jwt_required

from dragon.globalservices.process import runtime_reboot

LOG = logging.getLogger(__name__)


app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = str(os.urandom(12).hex())
jwt = JWTManager(app)


# Call if auth_key is not None
@jwt_required()
def jwt_optional():
    pass

@app.route("/api/token", methods=["POST"])
def get_jwt_token():
    """POST /api/token
        Retrieve JWT Bearer Token.
        Request:
        body:
            auth_key:
                type: string
                description: Auth Key value in YAML config

    Returns:
        Bearer Token
    """ 
    telem_key = request.json.get("auth_key", None)
    if telem_key == app.config["auth_key"]:
        access_token = create_access_token(identity=telem_key)
        return Response(json.dumps({"access_token": "Bearer " + access_token}), mimetype="application/json"), HTTPStatus.OK
    else:
        return Response(json.dumps({"msg": "Bad auth_key"}), mimetype="application/json"), HTTPStatus.BAD_REQUEST

@app.route("/api/aggregators")
def get_aggregators() -> list:
    """GET /api/aggregators
        Return aggregation functions

    Returns:
        list:  of aggregator functions
    """
    if app.config["auth_key"] is not None:
        jwt_optional()

    # List of possible aggregations
    aggregators_res = ["min", "sum", "max", "avg", "dev"]

    return Response(json.dumps((aggregators_res)), mimetype="application/json"), HTTPStatus.OK


@app.route("/api/query", methods=["POST"])
def post_query() -> object:

    """POST /api/query
    Send queries to node queues, and retrieve responses from return queue
    Returns:
        object: datapoints for metrics that have been queried
    """

    if app.config["auth_key"] is not None:
        jwt_optional()
    
    query = request.json
    uid = str(int(time.time() * 100)) + "_" + str(get_ident())
    query["req_id"] = uid
    query["type"] = "query"
    query["return_queue"] = "aggregator"
    queue_dict = app.config["queue_dict"]
    return_queue = app.config["return_queue"]
    for host, q in queue_dict.items():
        query["host"] = host
        # Convert query request to a specific structure and put in queue
        q.put(query)
    app.config["result_dict"][uid] = []
    total_hosts = len(queue_dict)
    remaining_hosts = len(queue_dict)
    while remaining_hosts != 0:
        try:
            res = return_queue.get(timeout=1)
            app.config["result_dict"][res["req_id"]].append(res["result"])
            remaining_hosts = total_hosts - (len(app.config["result_dict"][uid]))
        except queue.Empty as e:
            continue
        except Exception as e:
            LOG.warning(f"Aggregator Error: {e}")
            return Response(json.dumps(app.config["result_dict"]))
    # Grafana allocates label colors by result order -
    result = sorted(list(chain.from_iterable(app.config["result_dict"][uid])), key=lambda d: d["tags"]["host"])

    return Response(json.dumps(result).encode("utf-8"), mimetype="application/json"), HTTPStatus.OK


@app.route("/api/suggest")
def suggest() -> list:
    """GET /api/suggest
    Auto-suggest metric names using query string
    Request:
        params:
            q:
                type: string
                description: metric name

    Returns:
        list:  metrics containing the entered query string
    """
    if app.config["auth_key"] is not None:
        jwt_optional()

    uid = str(int(time.time() * 100)) + "_" + str(os.getpid())
    query = {"type": "suggest", "req_id": uid, "return_queue": "aggregator", "request": []}
    # Empty list val in shared dict
    queue_dict = app.config["queue_dict"]
    return_queue = app.config["return_queue"]
    _query = request.args.get("q") or None
    _type = request.args.get("type") or None
    for host, q in queue_dict.items():
        query["host"] = host
        q.put(query)
    app.config["result_dict"][uid] = []
    remaining_hosts = len(queue_dict)
    total_hosts = len(queue_dict)
    while remaining_hosts != 0:
        try:
            res = return_queue.get(timeout=1)
            app.config["result_dict"][res["req_id"]].append(res["result"])
            remaining_hosts = total_hosts - (len(app.config["result_dict"][uid]))

        except queue.Empty as e:
            continue
    result = list(set(chain(*app.config["result_dict"][uid])))
    if _query is not None:
        result = list(filter(lambda k: _query in k, result))
    app.config["result_dict"].pop(uid, None)
    return Response(json.dumps(result).encode("utf-8"), mimetype="application/json"), HTTPStatus.OK


@app.route("/api/telemetry_shutdown", methods=["GET"])
@jwt_required(optional=True)
# Stays optional because telemetry_shutdown is called through Telemetry finalize
def set_telemetry_shutdown() -> object:
    """GET /api/shutdown
    Signal shutdown sequence in Aggregator

    Returns:
        object:
            type: Response
            properties:
                shutdown_begin:
                    type: boolean
    """
    LOG.debug(f"Shutdown request Aggregator on: {os.uname().nodename}")
    resp = {"shutdown_begin": True}
    return Response(json.dumps(resp), mimetype="application/json"), HTTPStatus.OK


@app.route("/api/reboot", methods=["POST"])
def reboot_dragon() -> object:
    """POST /api/reboot

    Accepts x-www-form-urlencoded data only.
    Reboot Dragon and exclude nodes.
    Request:
        keys:
            hostnames:
                type: string
                description: CSV of hostnames
            huids:
                type: string
                description: CSV of huids
    Returns:
        object:
            type: Response
            properties:
                reboot:
                    type: boolean
                hostnames:
                    type: List(str)
                huids:
                    type: List(str)

    """
    if app.config["auth_key"] is not None:
        jwt_optional()

    hostnames = request.form.get("hostnames", "")
    h_uids = request.form.get("h_uids", "")

    hostnames = hostnames.split(",") if hostnames != "" else None
    h_uids = h_uids.split(",") if h_uids != "" else None
    resp = {"reboot": True, "hostnames": hostnames, "h_uids": h_uids}
    try:
        runtime_reboot(hostnames=hostnames, huids=h_uids)
    except Exception as e:
        LOG.warn(f"Aggregator Error: {e}")
        resp["error"] = str(e)

    return Response(json.dumps(resp), mimetype="application/json"), HTTPStatus.OK

if __name__ == "__main__":
    app.run()
