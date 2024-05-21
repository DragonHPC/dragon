""" The Dragon Dictionary is a performant and distributed solution that is
available to the applications and workflows written for the Dragon ecosystem.

This is Dragon's specialized implementation based on Dragon channels and memory,
that works across single and multi node settings. The Dragon Dictionary is designed
to be used as a standard Python Dictionary.

The Dragon Dictionary creates a manager owner process, along with manager processes,
and their associated dragon channels, memory pools. The communication with the client
is established by hashing and redirecting the client to the correct manager process.
"""

import os
import sys
import enum
import uuid
import dragon
import pickle
import logging
import multiprocessing

from ...utils import B64
from ... import managed_memory
from ...infrastructure.process_desc import ProcessOptions
from ...globalservices import channel
from ...globalservices import pool
from ...globalservices import process
from ...infrastructure import facts
from ...infrastructure import parameters
from ...channels import Channel, Message, Many2ManyWritingChannelFile, Many2ManyReadingChannelFile
from ...native.process import ProcessTemplate, Process
from ...native.process_group import ProcessGroup
from .dict_managers import manager_proc

LOG = logging.getLogger(__name__)

class managerPool(ProcessGroup):
    """Inherited from the native pool workers. The manager pool is intended to maintain
    specific variables and functionality requirements to maintain the pool of managers.

    :param nworkers: number of manager processes to start
    :type nworkers: int
    :param psize: size of the manager pool
    :type psize: int
    :param cmd: command
    :type cmd: str
    :param args: arguments to cmd
    :type args: tuple
    :param options: options will be used to pass along arbitrary extra info for things like placement
    :type options: tuple
    """

class ManagerOwnerProc:
    """Instantiate a manager owner process object that takes care of handling the manager processes.

    :param managers_per_node: Number of manager processes to be created on each node.
    :type managers_per_node: int
    :param n_nodes: Number of nodes specified for creating the manager processes.
    :type n_nodes: int
    :param total_mem: Total memory for the allocated pools of all the manager processes.
    This parameter along with managers_per_node, n_nodes helps to decide the pool size of each manager.
    This parameter expects to be specified in GB.
    :type total_mem: int
    """
    def __init__(self, managers_per_node, n_nodes, total_mem):
        LOG.debug(f"Initializing manager owner proc")
        self.managers_per_node = managers_per_node
        self.n_nodes = n_nodes
        self.total_mem = total_mem
        # Store the owner process id, channel, and manager processes
        self.mowner_puid = None
        self.mowner_chnl_desc = None
        self.mowner_chnl = None
        self.manager_processes = []
        # Manager pool workers object
        self.manager_pool = None
        # Store the handle to the dictionary object
        self.dict_chnl_desc = None
        self.dict_chnl = None


    def start(self, dict_chnl_name):
        n_workers = (self.n_nodes * self.managers_per_node)
        mpool_size = self.total_mem // n_workers
        LOG.debug(f"Manager pool size is {mpool_size} bytes")

        # Attach the manager owner process to the dictionary channel
        self.dict_chnl_desc = channel.query(dict_chnl_name)
        self.dict_chnl = Channel.attach(self.dict_chnl_desc.sdesc)

        # Create the manager owner channel
        default_muid = facts.default_pool_muid_from_index(parameters.this_process.index)
        mowner_chnl_uuid = str(uuid.uuid4())
        mowner_chnl_create = channel.create(default_muid, user_name="mowner-proc-"+mowner_chnl_uuid)
        self.mowner_chnl_desc = channel.join(mowner_chnl_create.name)
        self.mowner_chnl = Channel.attach(self.mowner_chnl_desc.sdesc)

        # Create the manager owner process
        mownerproc = Process(self.manager_owner_proc)
        mownerproc.start()
        self.mowner_puid = mownerproc.puid

        # Initialize the manager pool of workers
        # Pool workers are manager processes that serve the data to the client
        args = (mpool_size, mowner_chnl_create.name)
        template = ProcessTemplate(manager_proc, args=args)
        self.manager_pool = managerPool()
        self.manager_pool.add_process(n_workers, template)
        self.manager_pool.init()
        self.manager_pool.start()

        # Wait for the manager process information from the owner process
        wait = True
        while(wait):
            with Many2ManyReadingChannelFile(self.dict_chnl) as adapter:
                resp_msg = pickle.load(adapter)
                if("manager-info" in resp_msg):
                    manager_proc_info = resp_msg.split(':')
                    bytes = B64.str_to_bytes(manager_proc_info[0])
                    self.manager_processes = pickle.loads(bytes)
                    wait = False

        LOG.debug(f"Manager owner received information from {len(self.manager_processes)} manager workers")


    def manager_owner_proc(self):
        """Manager owner process that holds information about the manager workers.
        Join and wait for the messages from the manager processes on the owner channel
        """
        worker_count = 0
        mproc_puids = []
        manager_processes = []
        n_workers = (self.n_nodes * self.managers_per_node)

        # Worker count for all the manager workers to be created
        while (worker_count < n_workers):
            with Many2ManyReadingChannelFile(self.mowner_chnl) as adapter:
                manager_info = pickle.load(adapter)
                worker_count += 1
                mproc_puids.append(manager_info['p_uid'])
                manager_process = {'p_uid': manager_info['p_uid'],
                                   'chnl_desc': manager_info['chnl_desc'],
                                   'pool_desc': manager_info['pool_desc']}
                manager_processes.append(manager_process)

        # Store the manager processes information onto the client
        msg_str = B64.bytes_to_str(pickle.dumps(manager_processes)) + ":" + "manager-info"
        # Utilize the default pool memory of the node of the manager owner process
        pool_mem = managed_memory.MemoryPool.attach(B64.str_to_bytes(parameters.this_process.default_pd))

        with Many2ManyWritingChannelFile(self.dict_chnl, buffer_pool=pool_mem) as adapter:
            pickle.dump(msg_str, file=adapter, protocol=5)
        pool_mem.detach()

        process.multi_join(mproc_puids, join_all=True)
        LOG.debug(f"Manager owner exiting the dictionary process")


    def free_manager_resources(self):
        """
        Free the dictionary resources. Destroy the manager owner channels and pools.
        """
        # Detach the owner process with the dictionary channel
        LOG.debug(f"Destroying the dictionary channel")
        self.dict_chnl.detach()
        channel.destroy(self.dict_chnl_desc.c_uid)

        # Free up the manager owner channel
        LOG.debug(f"Destroying the manager owner channel")
        self.mowner_chnl.detach()
        channel.destroy(self.mowner_chnl_desc.c_uid)

        # Free up the channel and pool associated with manager processes
        for i in range(0, len(self.manager_processes)):
            manager_chnl_desc = self.manager_processes[i]['chnl_desc']
            channel.destroy(manager_chnl_desc.c_uid)
            manager_pool_desc = self.manager_processes[i]['pool_desc']
            pool.destroy(manager_pool_desc.m_uid)


    def stop(self):
        """
        Kill the manager owner process
        """
        LOG.debug(f"Killing the manager owner process")
        process.kill(self.mowner_puid, sig=process.signal.SIGKILL)


    def __del__(self):
        # Destroy the manager pool object
        del self.manager_pool


class DistributedDict:
    def __init__(self, managers_per_node, n_nodes, total_mem):
        """Instantiate a distributed dictionary creating the manager owner and the processes.
        :param managers_per_node: Number of manager processes to be created on each node.
        :type managers_per_node: int
        :param n_nodes: Number of nodes specified for creating the manager processes.
        :type n_nodes: int
        :param total_mem: Total memory for the allocated pools of all the manager processes.
        This parameter along with managers_per_node, n_nodes helps to decide the pool size
        of each manager. This parameter expects to be specified in GB.
        :type total_mem: int
        """
        self._managers_per_node = managers_per_node
        self._nodes = n_nodes
        self._total_mem = total_mem
        # Manager owner process created as part of the distributed dictionary
        self.manager_owner = None


    def start(self):
        """
        Creates the client channel using the Global Services and the default dragon memory pool.
        Instantiates the manager owner process, that inturn creates all the manager processes and channels.
        """
        # Using default memory pool for the client channel
        dpool_muid = facts.default_pool_muid_from_index(parameters.this_process.index)

        # Ask Global services to create a main channel which listens the manager owner information
        dict_chnl_uuid = str(uuid.uuid4())
        dict_chnl_create = channel.create(dpool_muid, user_name="ddict-chnl-"+dict_chnl_uuid)

        self.manager_owner = ManagerOwnerProc(self.managers_per_node, self.nodes, self.total_mem)
        self.manager_owner.start(dict_chnl_create.name)


    @property
    def nodes(self):
        """
        Return the nodes the dictionary spanned across
        """
        return self._nodes


    @property
    def managers_per_node(self):
        """
        Return the managers per node of the dictionary
        """
        return self._managers_per_node


    @property
    def total_mem(self):
        """
        Total allocated memory for the dictionary
        """
        return self._total_mem


    @property
    def manager_processes(self):
        """
        Return the manager processes of the dictionary
        """
        return self.manager_owner.manager_processes


    def retrieve_channel_desc(self, request_str):
        """
        Return the manager channel descriptor to the client
        """
        manager_processes = self.manager_processes
        chnl_num = hash(request_str) % len(manager_processes)
        return manager_processes[chnl_num]['chnl_desc']


    def stop(self):
        """
        Stop the distributed dictionary. Free all the resources including processes,
        associated channels, handles etc.
        """
        LOG.debug(f"Executing the STOP call on the dictionary")
        # Kill all the manager processes
        self.manager_owner.manager_pool.stop()

        # Free all the manager process resources
        self.manager_owner.free_manager_resources()


    def __del__(self):
        # Destroy the manager owner object
        del self.manager_owner


if __name__ == "__main__":
    multiprocessing.set_start_method("dragon")

    # Disable the non-deterministic behavior of the python hashing algorithm behavior
    hashseed = os.getenv('PYTHONHASHSEED')
    if not hashseed:
        os.environ['PYTHONHASHSEED'] = '0'
        os.execv(sys.executable, [sys.executable] + sys.argv)

    try:
        managers_per_node = int(sys.argv[1])
    except IndexError:
        # By default, create only a single manager on each node
        managers_per_node = 1

    try:
        n_nodes = int(sys.argv[2])
    except IndexError:
        # By defualt, consider only the client node
        n_nodes = 1

    try:
        total_mem = int(sys.argv[3])
    except IndexError:
        # By default, allocate 1GB of memory for the dictionary
        total_mem = 1

    # Convert total_mem to bytes
    total_mem *= (1024*1024*1024)

    # Executing the script directly. Just start and stop the distributed dictionary
    distDict = DistributedDict(managers_per_node, n_nodes, total_mem)
    distDict.start()
    distDict.stop()
