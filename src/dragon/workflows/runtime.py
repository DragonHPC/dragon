import atexit
import json
import os
import pathlib
import socket
import subprocess
import time

import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as dfacts
import dragon.infrastructure.util as dutil
import dragon.globalservices.api_setup as api_setup
import dragon.transport.oob as doob
import dragon.utils as dutils


current_rt_uid = None
already_published = {}
sdesc_by_system_and_name = {}
runtime_table = {}
oob_net_list = []
must_register_teardown = True


def proxy_teardown():

    global already_published
    for publish_path in already_published:
        os.remove(publish_path)

    runtime_table.clear()
    oob_net_list.clear()


def free_port(host, port):

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, int(port)))
        s.shutdown(socket.SHUT_WR)
        s.close()
        return False
    except ConnectionRefusedError:
        return True


def get_port(host, start_port, end_port):

    for port in range(start_port, end_port + 1):
        if free_port(host, port):
            return port

    raise RuntimeError("No available ports")


def get_ip():

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(("10.254.254.254", 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = "127.0.0.1"
    finally:
        s.close()
    return IP


def get_current_inf_env():

    rv = {}
    gs_cd_key = dfacts.env_name("GS_CD")
    gs_ret_cd_key = dfacts.env_name("GS_RET_CD")
    ls_cd_key = dfacts.env_name("LOCAL_SHEP_CD")
    ls_ret_cd_key = dfacts.env_name("SHEP_RET_CD")
    rv[gs_cd_key] = os.environ[gs_cd_key]
    rv[gs_ret_cd_key] = os.environ[gs_ret_cd_key]
    rv[ls_cd_key] = os.environ[ls_cd_key]
    rv[ls_ret_cd_key] = os.environ[ls_ret_cd_key]
    # get runtime ip addrs
    rt_uid_key = "DRAGON_RT_UID"
    rv[rt_uid_key] = os.environ[rt_uid_key]
    return rv


def set_inf_env(env):

    gs_cd_key = dfacts.env_name("GS_CD")
    gs_ret_cd_key = dfacts.env_name("GS_RET_CD")
    ls_cd_key = dfacts.env_name("LOCAL_SHEP_CD")
    ls_ret_cd_key = dfacts.env_name("SHEP_RET_CD")
    os.environ[gs_cd_key] = env[gs_cd_key]
    os.environ[gs_ret_cd_key] = env[gs_ret_cd_key]
    os.environ[ls_cd_key] = env[ls_cd_key]
    os.environ[ls_ret_cd_key] = env[ls_ret_cd_key]
    # set runtime ip addrs
    rt_uid_key = "DRAGON_RT_UID"
    os.environ[rt_uid_key] = env[rt_uid_key]

    global current_rt_uid
    current_rt_uid = env[rt_uid_key]

    api_setup.connect_to_infrastructure(force=True)


@property
def current_rt_uid():

    global current_rt_uid
    if current_rt_uid is None:
        return dutils.get_local_rt_uid()
    else:
        return current_rt_uid


class Proxy:

    def __init__(self, sdesc, oob_net):
        self._sdesc = sdesc
        self._env = json.loads(sdesc.env)
        self._original_env = None
        self._oob_net = oob_net

    def __enter__(self):
        # Reconfigure infrastructure connnections
        set_inf_env(self._env)

    def __exit__(self, exc_type, exc_value, traceback):
        # Restore the original infrastructure connections
        set_inf_env(self._original_env)

    def enable(self):
        self._original_env = get_current_inf_env()
        set_inf_env(self._env)

    def disable(self):
        set_inf_env(self._original_env)

    def get_env(self):
        return self._env


def get_sdesc():

    gs_cd = os.environ[dfacts.env_name(dfacts.GS_CD)]
    gs_ret_cd = os.environ[dfacts.env_name(dfacts.GS_RET_CD)]
    ls_cd = os.environ[dfacts.env_name(dfacts.LOCAL_SHEP_CD)]
    ls_ret_cd = os.environ[dfacts.env_name(dfacts.SHEP_RET_CD)]

    fe_ext_ip_addr = os.environ["DRAGON_FE_EXTERNAL_IP_ADDR"]
    head_node_ip_addr = os.environ["DRAGON_HEAD_NODE_IP_ADDR"]
    oob_port = dfacts.OOB_PORT
    sdesc = dmsg.RuntimeDesc(
        0, gs_cd, gs_ret_cd, ls_cd, ls_ret_cd, fe_ext_ip_addr, head_node_ip_addr, oob_port, os.environ
    )
    return sdesc.serialize()


def publish(name):

    sdesc_str = get_sdesc()

    global already_published
    if name in already_published:
        return get_sdesc()

    home_dir = pathlib.Path.home()
    dragon_dir = home_dir / ".dragon"
    publish_path = f"{dragon_dir}/{name}"
    pathlib.Path(dragon_dir).mkdir(parents=True, exist_ok=True)

    # if the file already exists, just read from it and return the sdesc
    # (depending on the filesystem, the file can potentially be visible
    # anywhere on the system, so this is a better, but more costly, check
    # than using "already_published" above)

    if os.path.exists(publish_path):
        with open(publish_path, "r") as publish_file:
            return publish_file.read()

    try:
        with open(publish_path, "w") as publish_file:
            publish_file.write(sdesc_str)

        already_published[publish_path] = True

        global oob_net_list
        oob_net = doob.OutOfBand()
        oob_net.accept(dfacts.OOB_PORT)
        oob_net_list.append(oob_net)

        global must_register_teardown
        if must_register_teardown:
            atexit.register(proxy_teardown)
            must_register_teardown = False
    except:
        raise RuntimeError(f"publish failed: {name} has already been published")

    return sdesc_str


def lookup(system, name, timeout_in=None):

    global sdesc_by_system_and_name

    # if we've already looked up this system/name combination, just grab
    # the sdesc from our local dict
    if (system, name) in sdesc_by_system_and_name:
        return sdesc_by_system_and_name[(system, name)]

    # loop until timeout trying to scp the sdesc file from the remote system,
    # sleeping 1 seconds between each attempt
    time_so_far = 0
    if timeout_in is None:
        timeout = 1
    else:
        timeout = timeout_in

    home_dir = pathlib.Path.home()
    dragon_dir = home_dir / ".dragon"
    publish_path = f"{dragon_dir}/{name}"

    while time_so_far < timeout:
        rc = os.system(f"scp {system}:{publish_path} . > /dev/null 2>&1")
        if rc == 0:
            with open(name, "r") as publish_file:
                sdesc = publish_file.read()
            sdesc_by_system_and_name[(system, name)] = sdesc
            os.remove(name)
            return sdesc
        else:
            # do ls on remote .dragon directory to force visibility of
            # newly created files on the parallel file system
            os.system(f'ssh {system} "ls {publish_path}" > /dev/null 2>&1')
            time.sleep(1)
            time_so_far += 1

    raise RuntimeError(f"lookup failed: could not obtain serialized descriptor for {system=} and {name=}")


def attach(sdesc_str):

    sdesc = dmsg.parse(sdesc_str)

    jump_host = sdesc.fe_ext_ip_addr
    compute_node = sdesc.head_node_ip_addr
    tunnel_port = sdesc.oob_port

    oob_net = doob.OutOfBand()
    oob_net.connect(jump_host, compute_node, str(tunnel_port))

    global must_register_teardown

    if must_register_teardown:
        atexit.register(proxy_teardown)
        must_register_teardown = False

    proxy = Proxy(sdesc, oob_net)
    remote_rt_uid = dutil.rt_uid_from_ip_addrs(sdesc.fe_ext_ip_addr, sdesc.head_node_ip_addr)
    runtime_table[remote_rt_uid] = proxy

    return proxy
