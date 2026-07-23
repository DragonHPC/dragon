import subprocess
import json
from pprint import pprint


def get_ip_addresses():

    try:
        local_args = ["python3", "-m", "dragon.transport.ifaddrs", "--ip", "--no-loopback", "--up", "--running"]

        srun_args = ["srun", "--nodes=1"]

        local_proc = subprocess.run(local_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        lout = json.loads(local_proc.stdout)
        print(f"Type: {type(lout)}")
        pprint(lout)
        remote_proc = subprocess.run(srun_args + local_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rout = json.loads(remote_proc.stdout)
        print(f"Type: {type(rout)}")
        pprint(rout)

    except Exception as e:
        raise RuntimeError(e)


if __name__ == "__main__":

    get_ip_addresses()
