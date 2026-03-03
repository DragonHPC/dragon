import os
import yaml
import sqlite3
import json
import time
from collections import defaultdict
import multiprocessing as mp
import dragon
import logging
import socket
from dragon.native.process_group import ProcessGroup
from dragon.native.process import ProcessTemplate, Process
from dragon.native.queue import Queue
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.telemetry.telemetry_head import setup_logging, aggregator_server, start_server
from dragon.dlogging.util import setup_BE_logging, DragonLoggingServices as dls
from dragon.infrastructure.parameters import this_process
log = None


def setup_logging():
    # This block turns on a client log for each client
    global log
    if log is None:
        fname = f"{dls.TELEM}_{socket.gethostname()}_head_{str(this_process.my_puid)}.log"
        setup_BE_logging(service=dls.TELEM, fname=fname)
        log = logging.getLogger(str(dls.TELEM))

class MiniTelemetry():
    def __init__(self):
        telem_cfg_path = os.getenv("DRAGON_TELEMETRY_CONFIG", None)
        if telem_cfg_path is None:
            self.telemetry_cfg = {}
        else:
            try:
                with open(telem_cfg_path, "r") as file:
                    self.telemetry_cfg = yaml.safe_load(file)
            except (FileNotFoundError, yaml.YAMLError, OSError) as e:
                raise RuntimeError(f"Failed to load telemetry config from {telem_cfg_path}: {e}")
        self._validate_config()
    
    def _validate_config(self):
        """Validate that required configuration keys exist"""
        required_keys = ["dump_node", "dump_dir"]
        missing_keys = [key for key in required_keys if key not in self.telemetry_cfg or not self.telemetry_cfg[key]]
        
        if missing_keys:
            raise ValueError(
                f"Missing required telemetry configuration keys: {', '.join(missing_keys)}. "
                f"Please ensure these keys are defined in your telemetry configuration file."
            )
        
        # Validate dump_dir exists
        dump_dir = self.telemetry_cfg["dump_dir"]
        if not os.path.exists(dump_dir):
            raise FileNotFoundError(f"Configured dump_dir does not exist: {dump_dir}")
        
        if not os.path.isdir(dump_dir):
            raise NotADirectoryError(f"Configured dump_dir is not a directory: {dump_dir}")
        
        if not os.access(dump_dir, os.W_OK):
            raise PermissionError(f"No write permission for dump_dir: {dump_dir}")
     
    
    def merge_ts_dbs(self):
        def get_db_base_name(filename):
            # removes the last two parts (timestamp and .db) ts_user_pinoak0041_20251117_103313.db
            return "_".join(filename.split("_")[:-2]) 
        try:
            # Prepare merged database path
            db_dir = os.path.join(self.telemetry_cfg.get("dump_dir"), "telemetry/")
            user = os.environ.get("USER", str(os.getuid()))
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            nodename = os.uname().nodename
            db_name = "mini_ts_" + user + "_" + nodename
            merged_db_name = os.path.join(db_dir, db_name + ".db")
            
            # Find all telemetry db files in the dump directory
            db_files = [f for f in os.listdir(db_dir) if f.endswith('.db') and "mini_ts_" not in f]
            
            groups = defaultdict(list)
            for f in db_files:
                base = get_db_base_name(f)
                groups[base].append(f)
            # Merge databases
            mdb_conn = sqlite3.connect(merged_db_name)
            mdb_cursor = mdb_conn.cursor()
            mdb_cursor.execute("CREATE TABLE IF NOT EXISTS datapoints (table_name text, metric text, timestamp text, value real, tags json)")

            list_of_tables = []
            
            # Iterate over grouped files and merge them
            for base, files in groups.items():
                table_name = base.split("_")[2]  # Extract node name from base
                list_of_tables.append(table_name)
                for df in files:
                    db_path = os.path.join(db_dir, df)
                    try:
                        mdb_cursor.execute(f"ATTACH DATABASE '{db_path}' AS src")
                        mdb_cursor.execute(f"INSERT OR IGNORE INTO datapoints SELECT '{table_name}', * FROM src.datapoints")
                        mdb_conn.commit()
                        mdb_cursor.execute("DETACH DATABASE src")
                    except Exception as e:
                        log.error(f"Error merging database {db_path}: {e}")
            
            sql_create_flags = "CREATE TABLE IF NOT EXISTS flags (id INTEGER PRIMARY KEY CHECK (id = 1), is_shutdown BLOB)"
            mdb_cursor.execute(sql_create_flags)
            mdb_conn.commit()
                
            mdb_conn.close()
        except (sqlite3.Error, OSError) as e:
            log.error(f"Error during merging telemetry databases: {e}")
            raise e
        return list_of_tables, db_name

def start_mini_telemetry(table_list, mt_db_name):
    """Starts mini telemetry servers on multiple nodes and an aggregator process.
    param table_list: List of table names (nodes) to monitor.
    param mt_db_name: Name of the merged telemetry database.
    """
    setup_logging()
    queue_dict = {}
    alloc = System()
    nodes = [Node(id) for id in alloc.nodes]
    node_list = []
    primary_node_hostname = None
    for node in nodes:
        node_list.append(node.hostname)
        if node.is_primary:
            primary_node_hostname = node.hostname

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
    cwd = os.getcwd()
    
    # Divide table_list equally among nodes
    num_nodes = len(node_list)
    tables_per_node = len(table_list) // num_nodes
    extra_tables = len(table_list) % num_nodes
    
    ds_procs = []
    start_idx = 0

    grp = ProcessGroup(restart=False, pmi=None)
    cwd = os.getcwd()
    # Start DragonServer on each node
    for i, hostname in enumerate(node_list):
        # Calculate how many tables this node gets
        node_table_count = tables_per_node + (1 if i < extra_tables else 0)
        end_idx = start_idx + node_table_count
        
        # Get the subset of tables for this node
        node_tables = table_list[start_idx:end_idx]
        
        args = (queue_discovery, as_discovery, return_queue_dict, shutdown_event, telemetry_cfg, (node_tables, mt_db_name))
        
        local_policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=hostname)
        grp.add_process(nproc=1, template=ProcessTemplate(target=start_server, args=args, cwd=cwd, policy=local_policy))
        print(f"Sending mini telemetry server to node: {hostname} for nodes:{node_tables}", flush=True)
        start_idx = end_idx
    
    grp.init()
    grp.start()

    print(f"Started Dragon Server for mini telemetry.", flush=True)
    
    host, queue = queue_discovery.get()
    queue_dict[host] = queue
    
    # Start Aggregator Process
    aggregator_proc = Process(
        target=aggregator_server,
        args=[queue_dict, return_queue_aggregator, telemetry_cfg],
    )

    aggregator_proc.start()
    print(f"Started Aggregator for mini telemetry.", flush=True)

    log.debug("grp joining")
    grp.join()
    log.debug("grp closing")
    grp.close()
    log.debug("aggregator process terminating")
    aggregator_proc.terminate()
    log.debug("aggregator process joining")
    aggregator_proc.join()


def main():
    mp.set_start_method("dragon")
    minitelem = MiniTelemetry()
    tables, db_name = minitelem.merge_ts_dbs()

    print("Merge complete, starting mini telemetry server...", flush=True)

    start_mini_telemetry(tables, db_name)

if __name__=="__main__":
    main()


