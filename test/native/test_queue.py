import unittest
import sys
import pickle
import time

import dragon
from dragon.native.queue import Queue

from dragon.channels import Channel, MASQUERADE_AS_REMOTE
from dragon.managed_memory import MemoryPool

from dragon.globalservices.process import create as process_create, join as process_join
from dragon.globalservices.pool import query as pool_query, get_list as pool_get_list
from dragon.infrastructure.process_desc import ProcessOptions
from dragon.infrastructure.channel_desc import ChannelOptions
from dragon.infrastructure.facts import default_pool_muid_from_index
import dragon.utils as du


def _putter(env_str):

    bytes = du.B64.str_to_bytes(env_str)
    q, item = pickle.loads(bytes)

    q.put(item)


def _joiner(env_str):

    bytes = du.B64.str_to_bytes(env_str)
    q, item = pickle.loads(bytes)

    q.put(item)

    q.join()


class TestQueue(unittest.TestCase):
    def test_simple(self):
        """Test basic functionality"""

        q = Queue(joinable=False)
        item = "Queue-item-0"

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps((q, item), protocol=5))

        the_descr = process_create(cmd, wdir, [__file__, "putter", env_str], None, options=options)

        new_item = q.get()

        self.assertTrue(new_item == item)

        process_join(the_descr.p_uid)

    def test_joinable(self):
        """Test that the queue can be joinable"""

        q = Queue(joinable=True)

        self.assertTrue(q._joinable == True)

        item = "JQueue-item-0"

        cmd = sys.executable
        wdir = "."
        options = ProcessOptions(make_inf_channels=True)
        env_str = du.B64.bytes_to_str(pickle.dumps((q, item), protocol=5))

        the_descr = process_create(cmd, wdir, [__file__, "joiner", env_str], None, options=options)

        new_item = q.get()
        self.assertTrue(new_item == item)
        time.sleep(0.1)

        ready = process_join(the_descr.p_uid, timeout=0)  # worker remains blocked
        self.assertTrue(ready == None)

        q.task_done()  # unblock worker

        ready = process_join(the_descr.p_uid, timeout=None)  # must cleanly exit
        self.assertTrue(ready == 0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "putter":
        _putter(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "joiner":
        _joiner(sys.argv[2])
    else:
        unittest.main()
