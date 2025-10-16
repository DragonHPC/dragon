from dragon.dtypes_inc cimport *

cdef class PriorityHeap:
    """
    Cython wrapper for the Dragon Channel Priority Heap
    """

    cdef dragonPriorityHeap_t _hdl

    def _handle_err(self, derr, msg):
        RuntimeError(f"Priority Heap Error: {msg}, Dragon Err code:({dragon_get_rc_string(derr)})")

    @staticmethod
    def create(dragonPriorityHeapUint_t base, dragonPriorityHeapLongUint_t capacity,
             dragonPriorityHeapUint_t nvals, unsigned char[:] py_ptr):

        cdef dragonError_t derr
        cdef void * ptr = &py_ptr[0]

        heap = PriorityHeap()

        derr = dragon_priority_heap_init(&heap._hdl, base, capacity, nvals, ptr)
        if derr != DRAGON_SUCCESS:
            heap._handle_err(derr, "Error initializing priority heap")

        return heap

    @staticmethod
    def attach(unsigned char[:] py_ptr):

        cdef dragonError_t derr
        cdef void * ptr = &py_ptr[0]

        heap = PriorityHeap()

        derr = dragon_priority_heap_attach(&heap._hdl, ptr)
        if derr != DRAGON_SUCCESS:
            heap._handle_err(derr, "Error attaching handle to existing priority heap")

        return heap

    @staticmethod
    def size(dragonPriorityHeapLongUint_t capacity, dragonPriorityHeapUint_t nvals_per_key):
        cdef size_t ret
        ret = dragon_priority_heap_size(capacity, nvals_per_key)
        return ret

    def destroy(self):
        cdef dragonError_t derr

        derr = dragon_priority_heap_destroy(&self._hdl)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Error trying to destroy priority heap handle")

    def detach(self):
        cdef dragonError_t derr

        derr = dragon_priority_heap_detach(&self._hdl)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Error trying to detach priority heap handle")

    def insert(self, unsigned long long[:] vals, urgent=False):
        """
        Takes a pointer to the thing to store on the priority heap and whether its urgent
        """
        cdef:
            dragonError_t derr
            dragonPriorityHeapLongUint_t * ptr = <dragonPriorityHeapLongUint_t*>(&vals[0])

        if urgent:
            derr = dragon_priority_heap_insert_urgent_item(&self._hdl, ptr)
        else:
            derr = dragon_priority_heap_insert_item(&self._hdl, ptr)

        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, f"Error inserting item, urgency was {urgent}.")

    def extract(self, unsigned long long[:] vals, dragonPriorityHeapLongUint_t priority):
        cdef:
            dragonError_t derr
            dragonPriorityHeapLongUint_t * ptr = <dragonPriorityHeapLongUint_t*>(&vals[0])

        derr = dragon_priority_heap_extract_highest_priority(&self._hdl, ptr, &priority)
        if derr != DRAGON_SUCCESS:
            self._handle_err(derr, "Could not extract highest priority")

    def _harr(self, idx):
        return self._hdl._harr[idx]
