from dragon.dtypes_inc cimport *
from dragon.return_codes cimport *


class DragonNUMA:
    """
    Class to represent NUMA information for a node.
    """

    def __init__(self):

        # Start by trying to load libnuma
        cdef dragonError_t derr

        derr = dragon_numa_available()

        if derr != DRAGON_SUCCESS:
            raise RuntimeError("NUMA is not available on this system")


    @staticmethod
    def get_numa_node_count():
        """
        Get the number of NUMA nodes on the system.
        """
        cdef int node_count
        cdef dragonError_t derr

        derr = dragon_numa_num_configured_nodes(&node_count)

        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Failed to get NUMA node count")

        return node_count

    @staticmethod
    def get_numa_node_to_cpus(nid: int):
        """
        Get the CPUs associated with a given NUMA node.
        """
        cdef dragonError_t derr
        cdef bitmask* bm = NULL
        cdef int bitset
        cdef unsigned long mask_size

        derr = dragon_numa_allocate_cpumask(&bm)
        if derr != DRAGON_SUCCESS:
            raise RuntimeError("Failed to allocate CPU mask")

        try:
            derr = dragon_numa_node_to_cpus(nid, bm)
            if derr != DRAGON_SUCCESS:
                raise RuntimeError(f"Failed to get CPUs for NUMA node {nid}")

            derr = dragon_numa_bitmask_size(bm, &mask_size)
            if derr != DRAGON_SUCCESS:
                raise RuntimeError("Failed to get CPU mask size")

            cpus = []
            for ibit in range(mask_size):
                derr = dragon_numa_bitmask_isbitset(bm, ibit, &bitset)
                if derr != DRAGON_SUCCESS:
                    raise RuntimeError(f"Failed to check if CPU {ibit} is set in bitmask")

                if bitset:
                    cpus.append(ibit)
        finally:
            derr = dragon_numa_free_cpumask(bm)
            if derr != DRAGON_SUCCESS:
                raise RuntimeError("Failed to free CPU mask")

        return cpus
