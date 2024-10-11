#ifndef DRAGON_FABRIC_HPP
#define DRAGON_FABRIC_HPP

#include <cstddef>
extern "C" {
#ifdef DF_BACKEND_IS_MPI
#include "pmi.h"
#include "pmi2.h"
#include "mpi.h"
#endif // DF_BACKEND_IS_MPI
#ifdef DF_BACKEND_IS_UCX
#include "ucp/api/ucp.h"
#endif // DF_BACKEND_IS_UCX
#ifdef DF_BACKEND_IS_OFI
#include "rdma/fabric.h"
#include "rdma/fi_endpoint.h"
#include "rdma/fi_domain.h"
#include "rdma/fi_tagged.h"
#include "rdma/fi_rma.h"
#include "rdma/fi_cm.h"
#include "rdma/fi_errno.h"
#endif // DF_BACKEND_IS_OFI
}

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <string>
#include <utility>
#include <vector>

#include "extras.hpp"

// constants

#define DF_MAX_CQ_EVENTS       8
#define DF_MAX_RMA_CNTR_EVENTS 8
#define DF_INVALID_RANK       -1
// TODO: determine this tag mask from ep_attr->mem_tag_format
#define DF_OFI_TAG_MASK        0xfffffffffffful
#define DF_DEFAULT_TIMEOUT     30

// enums and classes

enum dfError {
    DFABRIC_SUCCESS = 0,
    DFABRIC_FAILURE,
    DFABRIC_LAST
};

enum dfBackendType {
    DFABRIC_BACKEND_OFI = 0,
    DFABRIC_BACKEND_UCX,
    DFABRIC_BACKEND_MPI,
    DFABRIC_BACKEND_IO_URING,
    DFABRIC_BACKEND_LAST
};

enum dfTransportType {
    DFABRIC_TRANSPORT_TYPE_RMA = 0,
    DFABRIC_TRANSPORT_TYPE_P2P,
    DFABRIC_TRANSPORT_TYPE_LAST
};

class DragonFabric {
protected:

    int rank;
    int num_procs;
    int nid;
    int num_nodes;
    int nic_idx;
    int num_nics;

public:

    dfBackendType backend;
    dfTransportType transport_type;
    void *ep_addr;
    size_t ep_addr_len;
    bool ctor_success;

    DragonFabric();

    // need this to be able to delete DragonFabric objects
    virtual ~DragonFabric()
    {
        free(this->ep_addr);
    }

    virtual void send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) = 0;
    virtual void recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) = 0;
    virtual void rdma_get(void *base, size_t size, int rank, CqEvent *cqe) = 0;
    virtual void cq_read() = 0;
    virtual void insert_ep_addr(void *ep_addr, size_t ep_addr_len, int rank) = 0;
    virtual void mem_register(const void *buf, size_t size, RmaIov *rma_iov) = 0;
    virtual void mem_unregister(const void *buf) = 0;
    virtual void free_packed_rkey(void *packed_rkey) = 0;
    virtual void free_rkey(Rkey rkey) = 0;
    virtual void *unpack_rkey(int src_rank, const void *packed_rkey) = 0;
    virtual void cancel(CqEvent *cqe) = 0;
    virtual void log_nic_info() = 0;

    void handle_err(const char *msg);

    dfBackendType get_backend()
    {
        return this->backend;
    }

    int get_rank()
    {
        return this->rank;
    }

    int get_num_ranks()
    {
        return this->num_procs;
    }
};

#ifdef DF_BACKEND_IS_OFI

class IOVBox {
public:

    struct iovec iov;

    void init(void *addr, size_t size)
    {
        this->iov.iov_base = addr;
        this->iov.iov_len = size;
    }

    void fini() {}
};

DECL_OBJQ_RING_2(IOVBox,
                 void *,
                 size_t)

class RemoteIOVBox {
public:

    struct fi_rma_iov riov;

    void init(uint64_t base, size_t size, uint64_t mr_key)
    {
        this->riov.addr = base;
        this->riov.len  = size;
        this->riov.key  = mr_key;
    }

    void fini() {}
};

DECL_OBJQ_RING_3(RemoteIOVBox,
                 uint64_t,
                 size_t,
                 uint64_t)

class DragonFabric_ofi final : public DragonFabric {
private:

    struct fid_fabric *fabric;
    struct fid_domain *domain;
    struct fi_info *prov;
    struct fid_av *av;
    struct fid_ep *ep;
    struct fid_cq *cq;
    std::vector<fi_addr_t> mapped_addr_table;
    IOVBox_ObjQ iov_box_objq;
    RemoteIOVBox_ObjQ riov_box_objq;
    std::unordered_map<CqEvent *, IOVBox *> cqe_to_iov_box;
    std::unordered_map<CqEvent *, RemoteIOVBox *> cqe_to_riov_box;

public:

    DragonFabric_ofi(dfTransportType transport_type);
    ~DragonFabric_ofi();
    // NOTE: the _tl suffix ("thread local") indicates a function is
    // called by all threads during the backend's initialization
    struct fi_info *get_provider_tl();
    void init_endpoint_tl();
    void handle_err_ofi(const char *msg, int ofi_err);
    IOVBox *get_iov_box(void *base, size_t size, CqEvent *cqe);
    RemoteIOVBox *get_riov_box(uint64_t offset, size_t size, CqEvent *cqe);

    void send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) override;
    void recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) override;
    void rdma_get(void *base, size_t size, int rank, CqEvent *cqe) override;
    void cq_read() override;
    void insert_ep_addr(void *ep_addr, size_t ep_addr_len, int rank) override;
    void mem_register(const void *buf, size_t size, RmaIov *rma_iov) override;
    void mem_unregister(const void *buf) override;
    void cancel(CqEvent *cqe) override;
    void log_nic_info() override;

    void free_packed_rkey(void *packed_rkey) override
    {
        // no-op
    }

    void free_rkey(Rkey rkey) override
    {
        // no-op
    }

    void *unpack_rkey(int src_rank, const void *packed_rkey) override
    {
        return (void *) *(uint64_t *)packed_rkey;
    }

    void
    cleanup_iov_box(CqEvent *cqe)
    {
        auto iov_box = this->cqe_to_iov_box[cqe];
        if (iov_box != nullptr) {
            this->iov_box_objq.push_back(iov_box);
        }

        auto riov_box = this->cqe_to_riov_box[cqe];
        if (riov_box != nullptr) {
            this->riov_box_objq.push_back(riov_box);
        }
    }

    void
    set_ep_addr_tl()
    {
        this->ep_addr_len = FI_NAME_MAX;

        auto ofi_rc = fi_getname((fid_t) this->ep, this->ep_addr, &this->ep_addr_len);
        if (ofi_rc != FI_SUCCESS) {
            this->handle_err("failed to get address name for endpoint");
        }
    }
};
#endif // DF_BACKEND_IS_OFI

#ifdef DF_BACKEND_IS_UCX
class DragonFabric_ucx final : public DragonFabric {
private:

    ucp_context_h context;
    ucp_worker_h worker;
    std::vector<ucp_ep_h> eps;

public:

    DragonFabric_ucx(dfTransportType transport_type);
    ~DragonFabric_ucx();
    void send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) override;
    void recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) override;
    void rdma_get(void *base, size_t size, int rank, CqEvent *cqe) override;
    void cq_read() override;
    void insert_ep_addr(void *ep_addr, size_t ep_addr_len, int rank) override;
    void mem_register(const void *buf, size_t size, RmaIov *rma_iov) override;
    void free_packed_rkey(void *packed_rkey) override;
    void free_rkey(Rkey rkey) override;
    void *unpack_rkey(int src_rank, const void *packed_rkey) override;
    void mem_unregister(const void *buf) override;
    void cancel(CqEvent *cqe) override;
    void log_nic_info() override;

    void set_ep_addr_tl();
};
#endif // DF_BACKEND_IS_UCX

#ifdef DF_BACKEND_IS_MPI
class DragonFabric_mpi final : public DragonFabric {
private:

    uint64_t tag_mask;
    std::unordered_map<CqEvent *, MPI_Request> cqe_to_mpi_req;

public:

    DragonFabric_mpi(dfTransportType transport_type);
    ~DragonFabric_mpi();
    void send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) override;
    void recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe) override;
    void rdma_get(void *base, size_t size, int rank, CqEvent *cqe) override;
    void cq_read() override;
    void cancel(CqEvent *cqe) override;

    void mem_register(const void *buf, size_t size, RmaIov *rma_iov) override
    {
        // no-op for now
    }

    void mem_unregister(const void *buf) override
    {
        // no-op for now
    }

    void insert_ep_addr(void *ep_addr, size_t ep_addr_len, int rank) override
    {
        // no-op
    }

    void *unpack_rkey(int src_rank, const void *packed_rkey) override
    {
        // no-op
        return nullptr;
    }

    void log_nic_info() override
    {
        // no-op
    }
};
#endif // DF_BACKEND_IS_MPI

#endif // DRAGON_FABRIC_HPP
