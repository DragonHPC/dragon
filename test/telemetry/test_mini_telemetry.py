import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import yaml
from dragon.telemetry.mini_telemetry import MiniTelemetry
from telemetry.telemetry_data import SAMPLE_DATA
import tempfile
import shutil
import sqlite3
import json

class TestDragonTelemetryMiniTelemetry(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=True)
    def test_init_no_env_var(self):
        """Test initialization when DRAGON_TELEMETRY_CONFIG is not set."""
        # Should raise ValueError because telemetry_cfg defaults to {} and validation fails
        with self.assertRaises(ValueError) as cm:
            MiniTelemetry()
        self.assertIn("Missing required telemetry configuration keys", str(cm.exception))
    
    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", side_effect=FileNotFoundError("File not found"))
    def test_init_config_file_not_found(self, mock_file):
        """Test initialization when config file does not exist."""
        with self.assertRaises(RuntimeError) as cm:
            MiniTelemetry()
        self.assertIn("Failed to load telemetry config", str(cm.exception))

    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", new_callable=mock_open, read_data="invalid_yaml")
    @patch("yaml.safe_load", side_effect=yaml.YAMLError("Invalid YAML"))
    def test_init_yaml_error(self, mock_yaml, mock_file):
        """Test initialization when config file contains invalid YAML."""
        with self.assertRaises(RuntimeError) as cm:
            MiniTelemetry()
        self.assertIn("Failed to load telemetry config", str(cm.exception))


    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.safe_load", return_value={"dump_node": "node1"}) # Missing dump_dir
    def test_init_missing_keys(self, mock_yaml, mock_file):
        """Test initialization with missing required keys in config."""
        with self.assertRaises(ValueError) as cm:
            MiniTelemetry()
        self.assertIn("Missing required telemetry configuration keys", str(cm.exception))

    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.safe_load", return_value={"dump_node": "node1", "dump_dir": "/tmp/dump"})
    @patch("os.path.exists", return_value=False)
    def test_init_dump_dir_not_exist(self, mock_exists, mock_yaml, mock_file):
        """Test initialization when dump_dir does not exist."""
        with self.assertRaises(FileNotFoundError) as cm:
            MiniTelemetry()
        self.assertIn("Configured dump_dir does not exist", str(cm.exception))

    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.safe_load", return_value={"dump_node": "node1", "dump_dir": "/tmp/dump"})
    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=False)
    def test_init_dump_dir_not_directory(self, mock_isdir, mock_exists, mock_yaml, mock_file):
        """Test initialization when dump_dir is not a directory."""
        with self.assertRaises(NotADirectoryError) as cm:
            MiniTelemetry()
        self.assertIn("Configured dump_dir is not a directory", str(cm.exception))

    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.safe_load", return_value={"dump_node": "node1", "dump_dir": "/tmp/dump"})
    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=True)
    @patch("os.access", return_value=False)
    def test_init_dump_dir_no_permission(self, mock_access, mock_isdir, mock_exists, mock_yaml, mock_file):
        """Test initialization when dump_dir is not writable."""
        with self.assertRaises(PermissionError) as cm:
            MiniTelemetry()
        self.assertIn("No write permission for dump_dir", str(cm.exception))

    @patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/path/to/config.yaml"})
    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.safe_load", return_value={"dump_node": "node1", "dump_dir": "/tmp/dump"})
    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=True)
    @patch("os.access", return_value=True)
    def test_init_success(self, mock_access, mock_isdir, mock_exists, mock_yaml, mock_file):
        mt = MiniTelemetry()
        self.assertEqual(mt.telemetry_cfg["dump_node"], "node1")
        self.assertEqual(mt.telemetry_cfg["dump_dir"], "/tmp/dump")

class TestMiniTelemetryMerge(unittest.TestCase):
    """Tests for MiniTelemetry.merge_ts_dbs"""

    @classmethod
    def setUpClass(cls):
        # Create a temporary directory structure
        cls.test_dir = tempfile.mkdtemp()
        cls.dump_dir = os.path.join(cls.test_dir, "dump")
        cls.telemetry_dir = os.path.join(cls.dump_dir, "telemetry")
        os.makedirs(cls.telemetry_dir, exist_ok=True)
        
        # Define source database names simulating different nodes/timestamps
        # Format expected by get_db_base_name: ts_user_node_date_time.db
        user = os.environ.get("USER", str(os.getuid()))
        cls.node1_name = "node001"
        cls.node2_name = "node002"
        
        # Create DB files for Node 1
        cls.db1_path = os.path.join(cls.telemetry_dir, f"ts_{user}_{cls.node1_name}_20250101_100000.db")
        cls.create_dummy_db(cls.db1_path, SAMPLE_DATA)
        
        # Create DB files for Node 2
        cls.db2_path = os.path.join(cls.telemetry_dir, f"ts_{user}_{cls.node2_name}_20250101_100000.db")
        cls.create_dummy_db(cls.db2_path, SAMPLE_DATA)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir)

    @classmethod
    def create_dummy_db(cls, filepath, data):
        conn = sqlite3.connect(filepath)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS datapoints (metric text, timestamp text, value real, tags json)")
        
        sql_insert = "INSERT INTO datapoints VALUES (?,?,?,?)"
        for metric, time_dict in data.items():
            for ts, val in time_dict.items():
                cursor.execute(sql_insert, [metric, ts, val, json.dumps(None)])
        conn.commit()
        conn.close()

    def setUp(self):
        # Patch environment to avoid loading real config
        self.env_patcher = patch.dict(os.environ, {"DRAGON_TELEMETRY_CONFIG": "/dev/null"})
        self.env_patcher.start()
        
        # Mock open/yaml load to return our test config
        self.open_mock = patch("builtins.open", new_callable=MagicMock)
        self.yaml_mock = patch("yaml.safe_load", return_value={
            "dump_node": "test_node", 
            "dump_dir": self.dump_dir
        })
        
        self.mock_open = self.open_mock.start()
        self.mock_yaml = self.yaml_mock.start()
        
        # Mock validation checks since we are pointing to real temp dirs
        self.exists_mock = patch("os.path.exists", return_value=True)
        self.isdir_mock = patch("os.path.isdir", return_value=True)
        self.access_mock = patch("os.access", return_value=True)
        
        self.exists_mock.start()
        self.isdir_mock.start()
        self.access_mock.start()

    def tearDown(self):
        self.env_patcher.stop()
        self.open_mock.stop()
        self.yaml_mock.stop()
        self.exists_mock.stop()
        self.isdir_mock.stop()
        self.access_mock.stop()

    def test_merge_ts_dbs_success(self):
        """Test that databases are merged correctly and tables are identified."""
        mt = MiniTelemetry()
        
        # Run merge
        tables, db_name = mt.merge_ts_dbs()
        
        # Verify returned tables list contains our node names
        self.assertIn(self.node1_name, tables)
        self.assertIn(self.node2_name, tables)
        self.assertEqual(len(tables), 2)
        
        # Verify the merged database exists
        merged_db_path = os.path.join(self.telemetry_dir, db_name + ".db")
        self.assertTrue(os.path.exists(merged_db_path))
        
        # Verify content of merged database
        conn = sqlite3.connect(merged_db_path)
        cursor = conn.cursor()
        
        # Check flags table created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flags'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check datapoints table created
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='datapoints'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Verify data count
        # SAMPLE_DATA has 5 metrics * 3 timestamps = 15 rows per DB
        # We merged 2 DBs, so we expect 30 rows total
        cursor.execute("SELECT count(*) FROM datapoints")
        count = cursor.fetchone()[0]
        expected_rows = sum(len(v) for v in SAMPLE_DATA.values()) * 2
        self.assertEqual(count, expected_rows)
        
        # Verify table_name column was populated correctly
        cursor.execute(f"SELECT count(*) FROM datapoints WHERE table_name='{self.node1_name}'")
        count_node1 = cursor.fetchone()[0]
        self.assertEqual(count_node1, expected_rows // 2)
        
        conn.close()

    def test_merge_ts_dbs_empty_dir(self):
        """Test behavior when no source databases exist."""
        # Create a separate empty directory for this test
        empty_dir = tempfile.mkdtemp()
        empty_telem_dir = os.path.join(empty_dir, "telemetry")
        os.makedirs(empty_telem_dir)
        
        # Update mock to point to empty dir
        self.mock_yaml.return_value = {"dump_node": "test_node", "dump_dir": empty_dir}
        
        mt = MiniTelemetry()
        tables, db_name = mt.merge_ts_dbs()
        
        self.assertEqual(tables, [])
        
        # Cleanup
        shutil.rmtree(empty_dir)

if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()