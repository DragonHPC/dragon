import os
import sys
import subprocess
import unittest
import unittest.mock
from re import match

from textwrap import dedent
from dragon.tools.dragon_cleanup import DragonCleanup
from psutil import Process


class DragonCleanupProcessMockTest(unittest.TestCase):
    def setUp(self):
        # Patch process_iter and Path.exists/is_dir for all tests in this class
        patcher_process_iter = unittest.mock.patch('psutil.process_iter')
        patcher_path_exists = unittest.mock.patch('pathlib.Path.exists', return_value=True)
        patcher_path_isdir = unittest.mock.patch('pathlib.Path.is_dir', return_value=True)
        patcher_path_iterdir = unittest.mock.patch('pathlib.Path.iterdir', return_value=[])
        self.mock_process_iter = patcher_process_iter.start()
        self.mock_path_exists = patcher_path_exists.start()
        self.mock_path_isdir = patcher_path_isdir.start()
        self.mock_path_iterdir = patcher_path_iterdir.start()
        self.addCleanup(patcher_process_iter.stop)
        self.addCleanup(patcher_path_exists.stop)
        self.addCleanup(patcher_path_isdir.stop)
        self.addCleanup(patcher_path_iterdir.stop)

    def make_mock_process(self, pid, username, cmdline, children=None, exe='/usr/bin/dragon'):
        proc = unittest.mock.Mock(spec=Process)
        proc.pid = pid
        proc.username.return_value = username
        proc.cmdline.return_value = cmdline
        proc.children.return_value = children or []
        proc.exe.return_value = exe
        return proc

    def test_dragon_cleanup_class_options(self):
        my_rank = 0
        num_ranks = 1
        os.environ["DRAGON_RUN_RANK"] = str(my_rank)
        os.environ["DRAGON_RUN_NUM_RANKS"] = str(num_ranks)

        dc = DragonCleanup(resilient=False, dry_run=False, timeout=2)
        self.assertEqual(dc.my_rank, my_rank, "Rank mismatch in DragonCleanup instance")
        self.assertEqual(dc.num_ranks, num_ranks, "Num ranks mismatch in DragonCleanup instance")
        self.assertFalse(dc.resilient, "Resilient flag mismatch in DragonCleanup instance")
        self.assertFalse(dc.dry_run, "Dry run flag mismatch in DragonCleanup instance")
        self.assertEqual(dc.timeout, 2, "Timeout mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_single_node, "is_single_node mismatch in DragonCleanup instance")
        self.assertFalse(dc.is_multi_node, "is_multi_node mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_fe, "is_fe mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_be, "is_be mismatch in DragonCleanup instance")

        my_rank = 0
        num_ranks = 4
        os.environ["DRAGON_RUN_RANK"] = str(my_rank)
        os.environ["DRAGON_RUN_NUM_RANKS"] = str(num_ranks)

        dc = DragonCleanup(resilient=True, dry_run=False, timeout=5)
        self.assertEqual(dc.my_rank, my_rank, "Rank mismatch in DragonCleanup instance")
        self.assertEqual(dc.num_ranks, num_ranks, "Num ranks mismatch in DragonCleanup instance")
        self.assertTrue(dc.resilient, "Resilient flag mismatch in DragonCleanup instance")
        self.assertFalse(dc.dry_run, "Dry run flag mismatch in DragonCleanup instance")
        self.assertEqual(dc.timeout, 5, "Timeout mismatch in DragonCleanup instance")
        self.assertFalse(dc.is_single_node, "is_single_node mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_multi_node, "is_multi_node mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_fe, "is_fe mismatch in DragonCleanup instance")
        self.assertFalse(dc.is_be, "is_be mismatch in DragonCleanup instance")

        my_rank = 3
        num_ranks = 4
        os.environ["DRAGON_RUN_RANK"] = str(my_rank)
        os.environ["DRAGON_RUN_NUM_RANKS"] = str(num_ranks)

        dc = DragonCleanup(resilient=False, dry_run=True, timeout=2)
        self.assertEqual(dc.my_rank, my_rank, "Rank mismatch in DragonCleanup instance")
        self.assertEqual(dc.num_ranks, num_ranks, "Num ranks mismatch in DragonCleanup instance")
        self.assertFalse(dc.resilient, "Resilient flag mismatch in DragonCleanup instance")
        self.assertTrue(dc.dry_run, "Dry run flag mismatch in DragonCleanup instance")
        self.assertEqual(dc.timeout, 2, "Timeout mismatch in DragonCleanup instance")
        self.assertFalse(dc.is_single_node, "is_single_node mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_multi_node, "is_multi_node mismatch in DragonCleanup instance")
        self.assertFalse(dc.is_fe, "is_fe mismatch in DragonCleanup instance")
        self.assertTrue(dc.is_be, "is_be mismatch in DragonCleanup instance")

    def test_kill_process_tree_terminates_process_and_children(self):
        # Pseudocode:
        # - Create a parent process with two children, each with no children.
        # - Patch terminate, wait, kill on all.
        # - Call _kill_process_tree and assert terminate/wait called on all.
        parent = self.make_mock_process(100, DragonCleanup.MY_USERNAME, ['/usr/bin/dragon'])
        child1 = self.make_mock_process(101, DragonCleanup.MY_USERNAME, ['/usr/bin/dragon-child'])
        child2 = self.make_mock_process(102, DragonCleanup.MY_USERNAME, ['/usr/bin/dragon-child2'])
        parent.children.return_value = [child1, child2]
        child1.children.return_value = []
        child2.children.return_value = []

        dc = DragonCleanup()
        dc._kill_process_tree([parent])

        parent.terminate.assert_called_once()
        parent.wait.assert_called_once()
        child1.terminate.assert_called_once()
        child1.wait.assert_called_once()
        child2.terminate.assert_called_once()
        child2.wait.assert_called_once()

    def test_kill_process_tree_dry_run(self):
        parent = self.make_mock_process(100, DragonCleanup.MY_USERNAME, ['/usr/bin/dragon'])
        dc = DragonCleanup(dry_run=True)
        dc._kill_process_tree([parent])
        parent.terminate.assert_not_called()
        parent.wait.assert_not_called()

    def test_kill_dragon_process_tree_finds_and_kills_fe(self):
        # Pseudocode:
        # - Patch user_processes to include a process matching FE regex.
        # - Patch _kill_process_tree to track calls.
        # - Call kill_dragon_process_tree and assert _kill_process_tree called.
        proc = self.make_mock_process(200, DragonCleanup.MY_USERNAME, ['/usr/bin/python3', '_env/bin/dragon'])
        dc = DragonCleanup()
        dc.my_rank = 0
        dc.num_ranks = 4
        dc.user_processes = [proc] # type: ignore
        self.assertTrue(dc.is_fe, "Test setup error: not BE rank")
        with unittest.mock.patch.object(dc, '_kill_process_tree') as mock_kill:
            dc.kill_dragon_process_tree()
            mock_kill.assert_called()

    def test_kill_dragon_process_tree_finds_and_kills_be(self):
        # Pseudocode:
        # - Patch user_processes to include a process matching FE regex.
        # - Patch _kill_process_tree to track calls.
        # - Call kill_dragon_process_tree and assert _kill_process_tree called.
        proc = self.make_mock_process(200, DragonCleanup.MY_USERNAME, ['/usr/bin/python3', '_env/bin/dragon-backend'])
        dc = DragonCleanup()
        dc.my_rank = 2
        dc.num_ranks = 4
        dc.user_processes = [proc] # type: ignore
        self.assertTrue(dc.is_be, "Test setup error: not BE rank")
        with unittest.mock.patch.object(dc, '_kill_process_tree') as mock_kill:
            dc.kill_dragon_process_tree()
            mock_kill.assert_called()

    def test_restart_nvidia_cuda_mps_kills_and_restarts(self):
        # Pseudocode:
        # - Patch user_processes to include a process matching NVIDIA_CUDA_MPS_REGEX.
        # - Patch subprocess.run to check it's called.
        # - Patch Path("/dev/nvidiactl").exists to True.
        proc = self.make_mock_process(300, DragonCleanup.MY_USERNAME, ['/usr/bin/nvidia-cuda-mps'])
        dc = DragonCleanup()
        dc.user_processes = [proc] # type: ignore
        dc.my_rank = 2
        dc.num_ranks = 4
        dc.nvidia_gpus = True
        with unittest.mock.patch('subprocess.run') as mock_run:
            dc.restart_nvidia_cuda_mps()
            proc.terminate.assert_called_once()
            proc.wait.assert_called_once()
            mock_run.assert_called_once()

    def test_restart_nvidia_cuda_mps_dry_run(self):
        # Pseudocode:
        # - Patch user_processes to include a process matching NVIDIA_CUDA_MPS_REGEX.
        # - Patch subprocess.run to check it's called.
        # - Patch Path("/dev/nvidiactl").exists to True.
        proc = self.make_mock_process(300, DragonCleanup.MY_USERNAME, ['/usr/bin/nvidia-cuda-mps'])
        dc = DragonCleanup(dry_run=True)
        dc.user_processes = [proc] # type: ignore
        dc.my_rank = 2
        dc.num_ranks = 4
        dc.nvidia_gpus = True
        with unittest.mock.patch('subprocess.run') as mock_run:
            dc.restart_nvidia_cuda_mps()
            proc.terminate.assert_not_called()
            proc.wait.assert_not_called()
            mock_run.assert_not_called()

    def test_cleanup_dev_shm_removes_files(self):
        # Pseudocode:
        # - Patch Path.iterdir to return files owned by user.
        # - Patch file.unlink to check it's called.
        file1 = unittest.mock.Mock()
        file1.is_file.return_value = True
        file1.owner.return_value = DragonCleanup.MY_USERNAME
        file1.match.return_value = False
        file1.unlink = unittest.mock.Mock()
        dc = DragonCleanup()
        with unittest.mock.patch('pathlib.Path.iterdir', return_value=[file1]):
            dc.cleanup_dev_shm()
            file1.unlink.assert_called_once()

    def test_cleanup_dev_shm_resilient(self):
        # Pseudocode:
        # - Patch Path.iterdir to return files owned by user.
        # - Patch file.unlink to check it's called.
        file1 = unittest.mock.Mock()
        file1.is_file.return_value = True
        file1.owner.return_value = DragonCleanup.MY_USERNAME
        file1.unlink = unittest.mock.Mock()
        file1.match.side_effect = [True]
        dc = DragonCleanup(resilient=True)
        dc.my_rank = 2
        dc.num_ranks = 4
        with unittest.mock.patch('pathlib.Path.iterdir', return_value=[file1]):
            dc.cleanup_dev_shm()
            file1.unlink.assert_not_called()

    def test_cleanup_dev_shm_dry_run(self):
        # Pseudocode:
        # - Patch Path.iterdir to return files owned by user.
        # - Patch file.unlink to check it's called.
        file1 = unittest.mock.Mock()
        file1.is_file.return_value = True
        file1.owner.return_value = DragonCleanup.MY_USERNAME
        file1.unlink = unittest.mock.Mock()
        dc = DragonCleanup(dry_run=True)
        with unittest.mock.patch('pathlib.Path.iterdir', return_value=[file1]):
            dc.cleanup_dev_shm()
            file1.unlink.assert_not_called()

    def test_cleanup_tmp_removes_matching_files(self):
        # Pseudocode:
        # - Patch Path.iterdir to return files matching *.db.
        # - Patch file.unlink to check it's called.
        file1 = unittest.mock.Mock()
        file1.is_file.return_value = True
        file1.owner.return_value = DragonCleanup.MY_USERNAME
        file1.unlink = unittest.mock.Mock()
        dc = DragonCleanup()
        with unittest.mock.patch('pathlib.Path.iterdir', return_value=[file1]):
            dc.cleanup_tmp()
            file1.unlink.assert_called_once()

    def test_cleanup_tmp_resilient(self):
        # Pseudocode:
        # - Patch Path.iterdir to return files matching *.db.
        # - Patch file.unlink to check it's called.
        file1 = unittest.mock.Mock()
        file1.is_file.return_value = True
        file1.owner.return_value = DragonCleanup.MY_USERNAME
        file1.unlink = unittest.mock.Mock()
        file1.match.side_effect = [False, False, True]
        dc = DragonCleanup(resilient=True)
        dc.my_rank = 2
        dc.num_ranks = 4
        with unittest.mock.patch('pathlib.Path.iterdir', return_value=[file1]):
            dc.cleanup_tmp()
            file1.unlink.assert_not_called()

    def test_cleanup_tmp_removes_dry_run(self):
        # Pseudocode:
        # - Patch Path.iterdir to return files matching *.db.
        # - Patch file.unlink to check it's called.
        file1 = unittest.mock.Mock()
        file1.is_file.return_value = True
        file1.owner.return_value = DragonCleanup.MY_USERNAME
        file1.unlink = unittest.mock.Mock()
        dc = DragonCleanup(dry_run=True)
        with unittest.mock.patch('pathlib.Path.iterdir', return_value=[file1]):
            dc.cleanup_tmp()
            file1.unlink.assert_not_called()


if __name__ == "__main__":
    print("Starting Dragon Cleanup Tests...", flush=True)
    unittest.main()