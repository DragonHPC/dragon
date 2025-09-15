import os
import yaml
import subprocess

from .base import BaseWLM
from ...infrastructure.node_desc import NodeDescriptor
from ...infrastructure.facts import DEFAULT_TRANSPORT_NETIF, DEFAULT_OVERLAY_NETWORK_PORT, DEFAULT_PORT_RANGE
from ...utils import host_id_from_k8s


from typing import Union, Tuple, Optional


class KubernetesNetworkConfig(BaseWLM):

    def __init__(self, network_prefix=None, port=None, hostlist=None):

        try:
            from kubernetes import client, config
        except ImportError:
            raise RuntimeError(
                "Trying to launch Dragon within Kubernetes, but the Python kubernetes library is not installed."
            )

        if hostlist is None:
            hostlist = []

        super().__init__("k8s", network_prefix, port, len(hostlist))

        # Load the in-cluster config
        config.load_incluster_config()
        # Create an instance of the k8s API class
        self.k8s_api_v1 = client.CoreV1Api()
        self.k8s_batch_v1 = client.BatchV1Api()

        # Read the namespace from the service account token's namespace file
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
            self.namespace = f.read().strip()

    @classmethod
    def check_for_wlm_support(cls) -> bool:
        return (os.getenv("KUBERNETES_SERVICE_HOST") and os.getenv("KUBERNETES_SERVICE_PORT")) != None

    def _get_wlm_job_id(self) -> str:
        raise RuntimeError("KubernetesNetworkConfig does not implement _get_wlm_job_id")

    def _supports_net_conf_cache(self) -> bool:
        return False

    def _launch_network_config_helper(
        self,
        network_prefix: str = DEFAULT_TRANSPORT_NETIF,
        port_range: Union[Tuple[int, int], int] = (
            DEFAULT_OVERLAY_NETWORK_PORT,
            DEFAULT_OVERLAY_NETWORK_PORT + DEFAULT_PORT_RANGE,
        ),
    ):

        # Query Kubernetes API for the network topology
        with open(f"/config/backend_pod_{os.getenv('FRONTEND_JOB_LABEL')}.yml", "r") as f:
            be_job_config = yaml.safe_load(f)
        be_label_selector = (
            be_job_config.get("spec", {}).get("template", {}).get("metadata", {}).get("labels", {}).get("app", None)
        )
        # Get the port from the environment variable
        be_containers = be_job_config.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        for container in be_containers:
            env_list = container.get("env", [])
            for env_var in env_list:
                if env_var.get("name") == "BACKEND_OVERLAY_PORT":
                    be_port = env_var.get("value")
                    break

        be_label_selector = f"app={be_label_selector}"
        del be_job_config

        be_pods = self.k8s_api_v1.list_namespaced_pod(namespace=self.namespace, label_selector=be_label_selector)
        self.NNODES = len(be_pods.items)
        for node_index, pod in enumerate(be_pods.items):
            self.node_descriptors[str(node_index)] = NodeDescriptor(
                state=NodeDescriptor.State.ACTIVE,
                name=pod.metadata.name,
                host_name=pod.metadata.name,
                ip_addrs=[pod.status.pod_ip],
                host_id=host_id_from_k8s(pod.metadata.uid),
                port=int(be_port),
            )
        return self.node_descriptors

    def _get_dragon_launch_be_args(
        self,
        fe_ip_addr: str,
        fe_host_id: str,
        frontend_sdesc: str,
        network_prefix: str,
        overlay_port: int,
        transport_test_env: bool,
    ) -> list[str]:
        raise RuntimeError("KubernetesNetworkConfig does not implement _get_dragon_launch_be_args")

    def _get_wlm_launch_be_args(self, args_map: dict, launch_args: list):
        raise RuntimeError("KubernetesNetworkConfig does not implement _get_wlm_launch_be_args")

    def _get_launch_be_args(
        self,
        args_map: dict,
        launch_args: list,
        nnodes: Optional[int] = None,
        nodelist: Optional[list[str]] = None,
        hostname: Optional[str] = None,
    ):
        raise RuntimeError("KubernetesNetworkConfig does not implement _get_launch_be_args")

    def launch_backend(
        self,
        nnodes: int,
        node_ip_addrs: Optional[list[str]],
        nodelist: list[str],
        args_map: dict,
        fe_ip_addr: str,
        fe_host_id: str,
        frontend_sdesc: str,
        network_prefix: str,
        overlay_port: int,
        transport_test_env: bool,
    ) -> subprocess.Popen:
        raise RuntimeError("KubernetesNetworkConfig does not implement launch_backend")
