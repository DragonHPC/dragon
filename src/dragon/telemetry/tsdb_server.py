import os
import sys
import sqlite3
import gunicorn.app.base
from dragon.telemetry.tsdb_app import app as tsdb_app
import dragon
import multiprocessing
from pickle import dumps, loads
import logging

LOG = logging.getLogger(__name__)

def tsdb(start_event: multiprocessing.Event, shutdown_event: multiprocessing.Event):
  """Initialize SQLite Database for the specific node (if one does not exist).
  Ex: pinoak0033 database would be named ts_pinoak0033.db
  Store this .db file in /tmp
  Returns a connection and cursor object to the database.

  :param start_event: event that indicated db is up and ready to be written to
  :type start_event: multiprocessing.Event
  :param shutdown_event: event that is set when shutdown is signaled from use app
  :type shutdown_event: multiprocessing.Event
  """


  tmdb_directory = str(os.getenv("DRAGON_DEFAULT_TMDB_DIR", "/tmp"))
  delete_db_file = int(os.getenv("DRAGON_DELETE_TMDB", 1))
  filename = tmdb_directory + "/ts_" + os.uname().nodename + ".db"
  connection = sqlite3.connect(filename)
  c = connection.cursor()
  sql_create_metrics = "CREATE TABLE IF NOT EXISTS metrics (metric varchar(50), dps json)"
  c.execute(sql_create_metrics)
  connection.commit()
  sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
  c.execute(sql_create_flags)
  connection.commit()


  def when_ready(server) -> None:
    """ Executes when server is ready to start accepting requests.
    Inserts shutdown_event (multiprocessing.Event) into local SQLite3 flags table.
    Sets start_event so that Collector can start sending requests.

    :param server: _description_
    :type server: object (gunicorn.Arbiter)
    """
    try:
      LOG.debug(f"TSDB Gunicorn running on {os.uname().nodename}")
      sql_insert_flag_event = "INSERT INTO flags VALUES (1, ?)"
      c.execute(sql_insert_flag_event, [dumps(shutdown_event)])
      connection.commit()
    except Exception as e:
      LOG.warn(f"Error: {e}")
      shutdown_event.clear()
    server.app.options['start_event'].set()
    return

  def post_request(worker, req, environ, resp):
    """Executes right before a response is returned to any request.
    Checks if URI called is /api/telemetry_shutdown
    If it is, begins shutdown sequence.

    :param worker: Gunicorn worker
    :type worker: object
    :param req: gunicorn Request
    :type req: object
    :param environ: request environment
    :type environ: dict
    :param resp: gunicron Response
    :type resp: object
    """
    if environ['RAW_URI'] == "/api/telemetry_shutdown":
      LOG.debug(f"TSDBServer on {os.uname().nodename} sending sys.exit(4)")
      sys.exit(4)
    return

  def on_exit(server):
    """Executes this method when server is exiting

    :param server: Gunicorn server
    :type server: object (gunicorn.Arbiter)
    """
    LOG.debug(f"Exiting TSDBServer on {os.uname().nodename}....")
    if delete_db_file == 1:
      os.remove(filename)
    return

  options = {
        'bind': '%s:%s' % ('localhost', '4243'),
        'workers': 1,
        #'errorlog': '/tmp/tsdb_gunicorn.log',
        'start_event': start_event,
        'shutdown_event': shutdown_event,
        'daemon': True,
        'when_ready': when_ready,
        'on_exit': on_exit,
        'post_request': post_request,
        'loglevel': 'critical'
    }
  tsdb_application = TSDBApplication(tsdb_app, options)
  tsdb_application.run()

class TSDBApplication(gunicorn.app.base.BaseApplication):
  """ Gunicorn Server handling requests to Flask TSDB App

  """
  def __init__(self, app, options=None):
      self.options = options or {}
      app.config['shutdown_event'] = self.options['shutdown_event']
      self.application = app
      super().__init__()

  def load_config(self) -> None:
    """ Load server config
    """
    config = {key: value for key, value in self.options.items()
              if key in self.cfg.settings and value is not None}
    for key, value in config.items():
        self.cfg.set(key.lower(), value)

  def load(self):
    """ Load and return Flask app over Gunicorn server

    :return: Flask application
    :rtype: object (Flask)
    """
    return self.application

if __name__=="__main__":
   tsdb()
