from abc import ABC, abstractmethod
from typing import List


class WLMBase(ABC):

    NAME = "UNKNOWN"

    @classmethod
    @abstractmethod
    def check_for_wlm_support(cls) -> bool:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def check_for_allocation(cls) -> bool:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_host_list(cls) -> List:
        raise NotImplementedError
