from flask import Flask, request, Response
import json
import sqlite3
import time
import os
from pickle import loads
import logging
from yaml import safe_load
from http import HTTPStatus

LOG = logging.getLogger(__name__)

# Creates instance of Flask App
app = Flask(__name__)

user = os.environ.get("USER", str(os.getuid()))
telem_cfg = os.getenv("DRAGON_TELEMETRY_CONFIG", None)
if telem_cfg is None:
    telemetry_cfg = {}
else:
    with open(telem_cfg, "r") as file:
        telemetry_cfg = safe_load(file)

tmdb_directory = telemetry_cfg.get("default_tmdb_dir", "/tmp")

filename = os.path.join(tmdb_directory, "ts_" + user + "_" + os.uname().nodename + ".db")
connection = sqlite3.connect(filename)
c = connection.cursor()


def get_metrics_from_db() -> list:
  """Retrieves distinct metrics from metrics table

  Returns:
      list: Distinct metrics from database
  """
  sql = "SELECT DISTINCT(metric) FROM datapoints"
  res = list(c.execute(sql))
  res = [x[0] for x in res]
  return res


@app.route("/api/ready", methods=["GET"])
def check_is_ready() -> object:
    """API to check if Server and App are running. Returns True if Server is ready.
    Throws RequestException if not.

    Returns:
        object:
          is_ready: boolean (True)
    """
    resp = {"is_ready": True}
    return Response(json.dumps(resp), mimetype="application/json"), HTTPStatus.OK


@app.route("/api/metrics", methods=["POST"])
def add_metrics() -> object:
  """POST /api/metrics
        Inserts metrics into metrics table in local database
    body:
      type: application/json
      properties:
        dps:
          type: dict
          properties:
            "<metric_name>": "<metric_value>"
            metric_name: string
            metric_value: float
        timestamp: int
          required: false

  Returns:
      object:
        type: Response
        properties:
          success:
            type: int
            description: Number of successful inserts/updates
          falied:
            type: int
            description: Number of failed inserts/updates
          errors:
            type: [string]
            description: Error messages accompanying failures
  """
  req_body = request.json
  metrics_list = get_metrics_from_db()
  success = 0
  failed = 0
  error_msg = []
  timestamp = req_body["timestamp"] if "timestamp" in req_body.keys() else int(time.time())
  for req in req_body["dps"]:
      metric = req["metric"]
      tags = req.get("tags", None)
      try:
        sql_insert = "INSERT INTO datapoints VALUES (?,?,?,?)"
        c.execute(sql_insert, [metric, str(timestamp), req["value"], json.dumps(tags)])
        connection.commit()
      # Check if metric exists in the table
        if metric not in metrics_list:
          metrics_list.append(metric)
        success += 1

      except Exception as e:
        failed += 1
        error_msg.append(str(e))
        continue

  resp = {"success": success, "failed": failed, "errors": error_msg}

  return Response(json.dumps(resp), mimetype="application/json"), HTTPStatus.CREATED


@app.route("/api/set_shutdown", methods=["GET"])
def set_is_shutdown() -> object:
  """GET /api/shutdown
        Set shutdown event in database

  Returns:
      object:
        type: Response
        properties:
          success:
            type: int
            description: Number of successful shutdown requests
          falied:
            type: int
            description: Number of failed shutdown requests
          errors:
            type: [string]
            description: Error messages accompanying failures
  """
  LOG.debug(f"Request on: {os.uname().nodename}")
  success = 0
  failed = 0
  error_msg = []
  try:
    sql = "SELECT is_shutdown FROM flags LIMIT 1"
    shutdown_event = loads(list(c.execute(sql).fetchone())[0])
    shutdown_event.set()
    success+=1
  except sqlite3.Error as e:
    failed+=1
    error_msg.append(str(e))

  resp = {"success": success, "failed": failed, "errors": error_msg}
  return Response(json.dumps(resp), mimetype="application.json"), HTTPStatus.OK

@app.route("/api/tsdb_cleanup", methods=['POST'])
def cleanup_tsdb() -> object:
  """ POST /api/tsdb_cleanup
      Remove records for each metric that occur before the provided start time
  body:
    type: application/json
    properties:
      start_time:
        type: int
      custom:
        type: boolean
      default_metrics:
        type: [string]


  Returns:
      object:
        type: Response
        properties:
          success:
            type: int
            description: Number of successful inserts/updates
          falied:
            type: int
            description: Number of failed inserts/updates
          errors:
            type: [string]
            description: Error messages accompanying failures
  """
  req_body = request.json
  # LOG.debug(f"Cleanup request on: {os.uname().nodename}, Request: {req_body}")
  start_time = req_body["start_time"]
  select_sql = "DELETE FROM datapoints WHERE CAST(timestamp as INTEGER) < ?"
  res = list(c.execute(select_sql, [int(start_time)]))
  connection.commit()
  success = 0
  failed = 0
  error_msg = []
  resp = {"success": success, "failed":failed, "errors": error_msg }
  return Response(json.dumps(resp), mimetype="application/json"), HTTPStatus.CREATED


@app.route("/api/telemetry_shutdown", methods=["GET"])
def set_telemetry_shutdown():
    """GET /api/telemetry_shutdown
        Alert TSDB Server to begin shutdown sequence
    Returns:
        object:
          type: Response
          properties:
            shutdown:
              type: boolean
    """
    LOG.debug(f"Shutdown request on: {os.uname().nodename}")
    resp = {"shutdown": True}
    return Response(json.dumps(resp), mimetype="application/json"), HTTPStatus.OK


if __name__ == "__main__":
    app.run()
