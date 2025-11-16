import os
import unittest
import pathlib
import dragon
from dragon.native.process import Popen
from dragon.data.ddict.ddict import DDict, PosixCheckpointPersister
from dragon.native.machine import System, Node
from dragon.infrastructure.policy import Policy
from dragon.infrastructure.facts import DRAGON_LIB_DIR
import multiprocessing as mp

test_dir = pathlib.Path(__file__).resolve().parent
os.system(f"cd {test_dir}; make --silent")

ENV = dict(os.environ)
ENV["LD_LIBRARY_PATH"] = str(DRAGON_LIB_DIR) + ":" + str(ENV.get("LD_LIBRARY_PATH", ""))


class TestDDictC(unittest.TestCase):

    def tearDown(cls):
        for p in pathlib.Path(".").glob("*.ddict"):
            os.remove(p)

    def test_attach_detach(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_attach_detach"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_manager_placement(self):
        # create policy
        my_alloc = System()
        node_list = my_alloc.nodes

        policies = []

        # have only one manager on the last node
        node_id = node_list[-1]
        node = Node(node_id)
        policy = Policy(placement=Policy.Placement.HOST_NAME, host_name=node.hostname)
        policies.append(policy)

        # note that we're going to ignore what it says for number of nodes.
        exe = "c_ddict"
        # one manager on sencond node, we want to test when c client is on different node.
        ddict = DDict(None, None, 1 * 1024 * 1024 * 1024, managers_per_policy=1, policy=policies, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_manager_placement"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_length(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        ddict["hello"] = "world"
        ddict["dragon"] = "runtime"
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_length"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_clear(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        ddict["hello"] = "world"
        ddict["dragon"] = "runtime"
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_clear"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_put(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_put"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_put_multiple_values(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_put_multiple_values"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_put_multiple_key_writes(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_put_multiple_key_writes"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_pput(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_pput"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_contains_existing_key(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_contains_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_contains_non_existing_key(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_contains_non_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_get(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_get"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_get_multiple_values(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_get_multiple_values"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_get_receive_bytes_into(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_get_receive_bytes_into"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_get_multiple_key_writes(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_get_multiple_key_writes"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_get_read_mem(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_get_read_mem"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_get_non_existing_key(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_get_non_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_pop_existing_key(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_pop_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_pop_non_existing_key(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_pop_non_existing_key"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_keys(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_keys"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_keys_multiple_key_writes(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_keys_multiple_key_writes"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_keys_multiple_keys_and_writes(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_keys_multiple_keys_and_writes"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_checkpoint(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_checkpoint"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_rollback(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_rollback"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_rollback_zero_chkpt_id(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_rollback_zero_chkpt_id"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_sync_to_newest_checkpoint(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ddict.checkpoint()
        ddict.pput("hello", "world")
        ddict.checkpoint()
        ddict.pput("hello0", "world0")
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_sync_to_newest_checkpoint"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_checkpoint_id(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, wait_for_keys=True, working_set_size=2, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_checkpoint_id"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_local_manager(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        local_manager = ddict.local_manager
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_manager", local_manager], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_main_manager(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        main_manager = ddict.main_manager
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_main_manager", main_manager], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_serialize(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_serialize"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_attach(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_attach"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_put(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_put"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_get(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_get"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_sync_to_newest_checkpoint(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True, working_set_size=2, wait_for_keys=True)
        ddict_m0 = ddict.manager(0)
        ddict_m1 = ddict.manager(1)

        # checkpoint manager 0 to chkpt 3
        for _ in range(3):
            ddict_m0.checkpoint()
        ddict_m0.pput("dragon", "runtime")

        # checkpoint manager 1 to chkpt 2
        for _ in range(2):
            ddict_m1.checkpoint()
        ddict_m1.pput("hello", "world")

        ser_ddict = ddict.serialize()
        proc = Popen(
            executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_sync_to_newest_checkpoint"], env=ENV
        )
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_clear(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)

        ddict_m0 = ddict.manager(0)
        ddict_m1 = ddict.manager(1)
        ddict_m0["hello"] = "world"
        ddict_m1[333] = 666
        ddict_m1["dragon"] = "runtime"

        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_clear"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_pop(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_pop"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_custom_manager_keys(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_custom_manager_keys"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_empty_managers(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_empty_managers"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_local_managers(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_managers"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_local_keys(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_keys"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_synchronize(self):
        exe = "c_ddict"
        ddict1 = DDict(2, 1, 3000000, trace=True)
        ddict2 = DDict(2, 1, 3000000, trace=True)
        ddict3 = DDict(2, 1, 3000000, trace=True)

        ddict1["hello"] = "world"
        ddict2["hello2"] = "world"
        ddict3["hello3"] = "world"
        ddict2._mark_as_drained(0)
        ddict2._mark_as_drained(1)
        ddict3._mark_as_drained(0)
        ddict3._mark_as_drained(1)

        ser_ddict = ddict1.serialize()
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, "test_synchronize", 3, ddict1.serialize(), ddict2.serialize(), ddict3.serialize()],
            env=ENV,
        )

        proc.wait()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

        self.assertTrue("hello" in ddict2)
        self.assertTrue("hello" in ddict3)
        self.assertTrue("hello2" not in ddict2)
        self.assertTrue("hello3" not in ddict3)

        ddict1.destroy()
        ddict2.destroy()
        ddict3.destroy()

    def test_clone(self):
        exe = "c_ddict"
        d = DDict(2, 1, 3000000, trace=True)
        d_clone = DDict(2, 1, 3000000, trace=True)
        d_clone_1 = DDict(2, 1, 3000000, trace=True)

        d["dragon"] = "runtime"
        d["hello"] = "world"
        d[100] = 1000
        d["ddict"] = "test"
        d_clone["test_key"] = "test_val"
        d_clone_1["test_key_1"] = "test_val_1"

        ser_ddict = d.serialize()
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, "test_clone", 2, d_clone.serialize(), d_clone_1.serialize()],
            env=ENV,
        )

        proc.wait()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

        self.assertEqual(d_clone["dragon"], "runtime")
        self.assertEqual(d_clone["hello"], "world")
        self.assertEqual(d_clone[100], 1000)
        self.assertEqual(d_clone["ddict"], "test")
        self.assertFalse("test_key" in d_clone)

        self.assertEqual(d_clone_1["dragon"], "runtime")
        self.assertEqual(d_clone_1["hello"], "world")
        self.assertEqual(d_clone_1[100], 1000)
        self.assertEqual(d_clone_1["ddict"], "test")
        self.assertFalse("test_key_1" in d_clone_1)

        d.destroy()
        d_clone.destroy()
        d_clone_1.destroy()

    def test_freeze(self):
        exe = "c_ddict"
        d = DDict(2, 1, 3000000, trace=True)
        ser_ddict = d.serialize()
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, "test_freeze"],
            env=ENV,
        )
        proc.wait()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")
        d.destroy()

    def test_get_from_frozen_dict(self):
        exe = "c_ddict"
        d = DDict(2, 1, 3000000, trace=True)
        ser_ddict = d.serialize()
        proc = Popen(
            executable=str(test_dir / exe),
            args=[ser_ddict, "test_get_from_frozen_dict"],
            env=ENV,
        )
        proc.wait()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")
        d.destroy()

    def test_batch_put(self):
        exe = "c_ddict"
        d = DDict(2, 1, 3000000, working_set_size=2, trace=True, wait_for_keys=True)
        ser_ddict = d.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_batch_put"], env=ENV)
        proc.wait()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_bput_bget(self):
        exe = "c_ddict"
        num_managers = 2
        d = DDict(num_managers, 1, 3000000, working_set_size=2, trace=True, wait_for_keys=True)
        ser_ddict = d.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_bput_bget", num_managers], env=ENV)
        proc.wait()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_bput_batch(self):
        exe = "c_ddict"
        num_managers = 12
        d = DDict(num_managers, 1, 1500000 * num_managers, wait_for_keys=True, working_set_size=4, trace=True)
        ser_ddict = d.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_bput_batch", num_managers], env=ENV)
        proc.wait()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_bput_invalid_op(self):
        exe = "c_ddict"
        num_managers = 12
        d = DDict(num_managers, 1, 1500000 * num_managers, wait_for_keys=True, working_set_size=4, trace=True)
        ser_ddict = d.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_bput_invalid_op", num_managers], env=ENV)
        proc.wait()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_bput_multiple_batch(self):
        exe = "c_ddict"
        num_managers = 12
        d = DDict(num_managers, 1, 1500000 * num_managers, wait_for_keys=True, working_set_size=4, trace=True)
        ser_ddict = d.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_bput_multiple_batch", num_managers], env=ENV)
        proc.wait()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_advance(self):
        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 2
        persist_count = 2
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = d.serialize()
        exe = "c_ddict"
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_write_chkpts_to_disk"], env=ENV)
        proc.wait()
        restore_name = d.get_name()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

        chkpt_restore = 0
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            persist_freq=persist_freq,
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = dd_restore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_advance"], env=ENV)
        proc.wait()
        dd_restore.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_persist_restore(self):
        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 8
        persist_count = -1
        exe = "c_ddict"
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = d.serialize()

        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_persist"], env=ENV)
        proc.wait()
        restore_name = d.get_name()
        d.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

        chkpt_restore = 4
        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            persist_freq=2,
            name=restore_name,
            restore_from=chkpt_restore,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        self.assertEqual(dd_restore.persisted_ids(), [0, 2, 4])
        ser_ddict = dd_restore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_restore"], env=ENV)
        proc.wait()
        dd_restore.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_persisted_ids(self):
        exe = "c_ddict"
        NUM_MANAGERS = 1
        working_set_size = 2
        persist_freq = 2
        persist_count = -1
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = d.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_no_persisted_ids"], env=ENV)
        proc.wait()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

        # persists 0, 2, 4
        for i in range(7):
            d["hello"] = "world"
            d.checkpoint()

        restore_name = d.get_name()

        d.destroy()

        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            persist_freq=2,
            name=restore_name,
            restore_from=0,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = dd_restore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_persisted_ids_0_2_4"], env=ENV)
        proc.wait()
        dd_restore.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_multiple_managers_persisted_ids(self):
        exe = "c_ddict"
        NUM_MANAGERS = 3
        working_set_size = 2
        persist_freq = 2
        persist_count = 4
        d = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            working_set_size=working_set_size,
            wait_for_keys=True,
            persist_path="",
            persist_freq=persist_freq,
            persist_count=persist_count,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = d.serialize()
        # manager 0 persists 0, 2, 4, 6
        d0 = d.manager(0)
        for _ in range(9):
            d0["hello"] = "world"
            d0.checkpoint()

        # manager 1 persists 2, 4, 6, 8
        d1 = d.manager(1)
        for _ in range(11):
            d1["hello"] = "world"
            d1.checkpoint()

        # manager 2 persists 4, 6, 8, 10
        d2 = d.manager(2)
        for _ in range(13):
            d2["hello"] = "world"
            d2.checkpoint()

        restore_name = d.get_name()

        d.destroy()

        dd_restore = DDict(
            NUM_MANAGERS,
            1,
            1500000 * NUM_MANAGERS,
            trace=True,
            persist_path=".",
            persist_freq=2,
            name=restore_name,
            restore_from=4,
            read_only=True,
            persister_class=PosixCheckpointPersister,
        )
        ser_ddict = dd_restore.serialize()
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_persisted_ids_4_6"], env=ENV)
        proc.wait()
        dd_restore.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")

    def test_local_length(self):
        exe = "c_ddict"
        ddict = DDict(2, 1, 3000000, trace=True)
        ser_ddict = ddict.serialize()
        ddict["hello"] = "world"
        ddict["dragon"] = "runtime"
        proc = Popen(executable=str(test_dir / exe), args=[ser_ddict, "test_local_length"], env=ENV)
        proc.wait()
        ddict.destroy()
        self.assertEqual(proc.returncode, 0, "C client exited with non-zero exit code")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    unittest.main()
