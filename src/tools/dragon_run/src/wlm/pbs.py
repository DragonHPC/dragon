import os
import shutil

from .base import WLMBase


class WLMPBS(WLMBase):

    ENV_PBS_JOB_ID = "PBS_JOBID"
    ENV_PBS_NODEFILE = "PBS_NODEFILE"
    NAME = "PBS"

    @classmethod
    def check_for_wlm_support(cls):
        return all(
            [
                shutil.which("qstat") is not None,
                shutil.which("mpiexec") is not None,
            ]
        )

    @classmethod
    def check_for_allocation(cls) -> bool:
        if not all(
            [
                os.environ.get(cls.ENV_PBS_JOB_ID, None) is not None,
                os.environ.get(cls.ENV_PBS_NODEFILE, None) is not None,
            ]
        ):
            return False

        nodefile = os.environ.get(cls.ENV_PBS_NODEFILE)
        assert nodefile
        return os.path.isfile(nodefile) and os.access(nodefile, os.R_OK)

    @classmethod
    def get_host_list(cls):
        filename = os.environ.get(cls.ENV_PBS_NODEFILE)
        assert filename

        try:
            with open(filename) as inf:
                return [line.strip() for line in inf]
        except FileNotFoundError:
            pass

        return []


if __name__ == "__main__":
    # Example usage
    if WLMPBS.check_for_wlm_support():
        print("WLM PBS is supported.")
        if WLMPBS.check_for_allocation():
            print("Allocation found.")
            print("Host list:", WLMPBS.get_host_list())
        else:
            print("No allocation found.")
    else:
        print("WLM PBS is not supported on this system.")
