from flask import Flask, request, Response
import json
import sqlite3
import time
import os
import sys
from pickle import loads
import logging

LOG = logging.getLogger(__name__)

# Creates instance of Flask App
app = Flask(__name__)

filename = "/tmp/ts_" + os.uname().nodename + ".db"
connection = sqlite3.connect(filename) 
c = connection.cursor()

def get_metrics_from_db() -> list:
  """Retrieves distinct metrics from metrics table

  Returns:
      list: Distinct metrics from database
  """
  sql = "SELECT DISTINCT(metric) FROM metrics" 
  res = list(c.execute(sql))
  res = [x[0] for x in res]
  return res

@app.route("/api/ready", methods=['GET'])
def check_is_ready() -> object:
  """ API to check if Server and App are running. Returns True if Server is ready.
  Throws RequestException if not.

  Returns:
      object: 
        is_ready: boolean (True)
  """
  resp = {"is_ready": True}
  return Response(json.dumps(resp), mimetype='application/json'), 200 

@app.route("/api/metrics", methods=['POST'])
def add_metrics() -> object:  
  """ POST /api/metrics
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
  for metric in req_body["dps"].keys():
      try:
      # Check if metric exists in the table
        if metric in metrics_list:
          sql_select = "SELECT json_extract(dps, '$') FROM metrics WHERE metric = ?" 
          rows = c.execute(sql_select, [metric]).fetchall()
          if len(rows) > 0:
              dps = [json.loads(i[0]) for i in rows][0]
              dps.update({str(timestamp):req_body["dps"][metric]})
              sql_update = "UPDATE metrics SET dps = ? WHERE metric = ?" 
              c.execute(sql_update, [json.dumps(dps), metric])
              connection.commit()
        else:
          # New row with new metric
          new_dp = {str(timestamp):req_body["dps"][metric]}
          sql_insert = "INSERT INTO metrics VALUES (?,?)" 
          c.execute(sql_insert, [metric, json.dumps(new_dp)])
          connection.commit()
          metrics_list.append(metric)
        success += 1

      except sqlite3.Error as e:
        failed+=1
        error_msg.append(e)
        continue
  resp = { "success": success, "failed": failed, "errors": error_msg}
  return Response(json.dumps(resp), mimetype='application/json'), 201 

@app.route("/api/set_shutdown", methods=['GET'])
def set_is_shutdown() -> object:
  """ GET /api/shutdown
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
    error_msg.append(e)

  resp = {"success": success, "failed": failed, "errors": error_msg}
  return Response(json.dumps(resp), mimetype="application.json"), 200

@app.route("/api/telemetry_shutdown", methods=['GET'])
def set_telemetry_shutdown():
  """ GET /api/telemetry_shutdown
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
  return Response(json.dumps(resp), mimetype="application/json"), 200

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
  select_sql = "SELECT * FROM metrics"
  res = list(c.execute(select_sql))
  success = 0
  failed = 0
  error_msg = []
  for (metric, dps) in res:
    try:
      dps = json.loads(dps)
      dps = {k:v for k,v in (dps).items() if int(k) >= int(start_time)}
      sql_update = "UPDATE metrics SET dps = ? WHERE metric = ?" 
      c.execute(sql_update, [json.dumps(dps), metric])
      connection.commit()
      success+=1
    except sqlite3.Error as e:
      failed+=1
      error_msg.append(e)
  
  resp = {"success": success, "failed":failed, "errors": error_msg }
  return Response(json.dumps(resp), mimetype="application/json"), 201

if __name__=="__main__":
   app.run()