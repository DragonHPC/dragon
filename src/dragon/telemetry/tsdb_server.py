import os
import sys
import sqlite3
import gunicorn.app.base
from dragon.telemetry.tsdb_app import app as tsdb_app
import dragon
import multiprocessing
from pickle import dumps, loads
import logging
import stat

LOG = logging.getLogger(__name__)

ENFORCED_DB_PERMISSIONS = 0o750

def tsdb(start_event: multiprocessing.Event, shutdown_event: multiprocessing.Event, telemetry_cfg: object):
    """Initialize SQLite Database for the specific node (if one does not exist).
    Ex: pinoak0033 database would be named ts_pinoak0033.db
    Store this .db file in /tmp
    Returns a connection and cursor object to the database.

    :param start_event: event that indicated db is up and ready to be written to
    :type start_event: multiprocessing.Event
    :param shutdown_event: event that is set when shutdown is signaled from use app
    :type shutdown_event: multiprocessing.Event
    """

    user = os.environ.get("USER", str(os.getuid()))
    tmdb_directory = str(telemetry_cfg.get("default_tmdb_dir", "/tmp"))
    delete_db_file = int(telemetry_cfg.get("delete_tmdb", 1))
    filename = os.path.join(tmdb_directory, "ts_" + user + "_" + os.uname().nodename + ".db")
    connection = sqlite3.connect(filename)
    c = connection.cursor()
    sql_create_metrics = "CREATE TABLE IF NOT EXISTS datapoints (metric varchar(50), timestamp varchar(30), value real, tags json)"
    c.execute(sql_create_metrics)
    connection.commit()
    sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
    c.execute(sql_create_flags)
    connection.commit()

    db_permissions = stat.S_IMODE(os.stat(filename).st_mode)
    if db_permissions != ENFORCED_DB_PERMISSIONS:
        os.chmod(filename, ENFORCED_DB_PERMISSIONS)

    def when_ready(server) -> None:
        """Executes when server is ready to start accepting requests.
        Inserts shutdown_event (multiprocessing.Event) into local SQLite3 flags table.
        Sets start_event so that Collector can start sending requests.

        :param server: _description_
        :type server: object (gunicorn.Arbiter)
        """
        try:
            LOG.debug(f"TSDB Gunicorn running on {os.uname().nodename}")
            sql_insert_flag_event = "INSERT OR REPLACE INTO flags VALUES (1, ?)"
            c.execute(sql_insert_flag_event, [dumps(shutdown_event)])
            connection.commit()
        except Exception as e:
            LOG.warn(f"Error: {e}")
            shutdown_event.clear()
        server.app.options["start_event"].set()
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
        if environ["RAW_URI"] == "/api/telemetry_shutdown":
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

    tsdb_port = telemetry_cfg.get("tsdb_server_port", "4243")
    tmdb_dir = telemetry_cfg.get("default_tmdb_dir", "/tmp")
    options = {
        "bind": "%s:%s" % ("localhost", tsdb_port),
        "workers": 1,
        # "errorlog": "/tmp/tsdb_gunicorn.log",
        "start_event": start_event,
        "shutdown_event": shutdown_event,
        "daemon": True,
        "when_ready": when_ready,
        "on_exit": on_exit,
        "post_request": post_request,
        "loglevel": "critical",
        "tmdb_dir": tmdb_dir,
    }
    tsdb_application = TSDBApplication(tsdb_app, options)
    tsdb_application.run()


class TSDBApplication(gunicorn.app.base.BaseApplication):
    """Gunicorn Server handling requests to Flask TSDB App"""

    def __init__(self, app, options=None):
        self.options = options or {}
        app.config["shutdown_event"] = self.options["shutdown_event"]
        app.config["tmdb_dir"] = self.options["tmdb_dir"]
        self.application = app
        super().__init__()

    def load_config(self) -> None:
        """Load server config"""
        config = {key: value for key, value in self.options.items() if key in self.cfg.settings and value is not None}
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        """Load and return Flask app over Gunicorn server

        :return: Flask application
        :rtype: object (Flask)
        """
        return self.application


if __name__ == "__main__":
    tsdb()
