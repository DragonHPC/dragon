#include "agent.hpp"
#include "cq.hpp"
#include "dragon_fabric.hpp"

#define HSTA_USE_RKEY_PACK_DEPRECATED 1

// file-scope variables

// TODO: default fabric backend
static std::unordered_map<void *, std::vector<void *>> df_mr_map;

// base implementation

DragonFabric::DragonFabric()
{
    this->nic_idx      = hsta_thread_idx;
    this->num_nics     = hsta_num_threads;
    this->nid          = hsta_startup_info.node_idx;
    this->num_nodes    = hsta_startup_info.num_nodes;
    this->rank         = (this->num_nics * this->nid) + this->nic_idx;
    this->num_procs    = this->num_nics * this->num_nodes;
    this->ctor_success = true;

    this->ep_addr = malloc(HSTA_MAX_EP_ADDR_LEN);
    hsta_dbg_assert(this->ep_addr != nullptr);
}

void
DragonFabric::handle_err(const char *msg)
{
    if (dragon_hsta_debug) {
        hsta_lock_acquire(hsta_utils.lock);
        fprintf(hsta_dbg_file, "DragonFabric error: %s,\n"
                                     "==> at line %d in file %s\n", msg, __LINE__, __FILE__);
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());
        fflush(hsta_dbg_file);
        hsta_lock_release(hsta_utils.lock);
    }
    hsta_utils.graceful_exit(SIGINT);
}

// OFI implementation

#ifdef DF_BACKEND_IS_OFI

extern "C" {
DragonFabric *
get_dfabric(dfTransportType transport_type)
{
    return (DragonFabric *)new DragonFabric_ofi(transport_type);
}
}

static uint64_t
get_mr_key()
{
    static uint64_t mr_key_cntr = 0UL;
    return mr_key_cntr++;
}

IOVBox *
DragonFabric_ofi::get_iov_box(void *base, size_t size, CqEvent *cqe)
{
    auto iov_box = this->iov_box_objq.pull_front(base, size);
    if (iov_box == nullptr) {
        this->handle_err("failed to allocate iovec");
    }

    this->cqe_to_iov_box[cqe] = iov_box;

    return iov_box;
}

RemoteIOVBox *
DragonFabric_ofi::get_riov_box(uint64_t offset, size_t size, CqEvent *cqe)
{
    // using hsta_thread_idx below to select an mr key means that
    // we will target the source endpoint with the same index
    auto riov_box =
        this->riov_box_objq.pull_front(
            offset,
            size,
            (uint64_t) cqe->rma_iov->rkeys[hsta_thread_idx]
        );

    if (riov_box == nullptr) {
        this->handle_err("failed to allocate iovec");
    }

    this->cqe_to_riov_box[cqe] = riov_box;

    return riov_box;
}

DragonFabric_ofi::DragonFabric_ofi(dfTransportType transport_type)
{
    this->backend        = DFABRIC_BACKEND_OFI;
    this->transport_type = transport_type;

    this->prov = this->get_provider_tl();
    if (this->prov == nullptr) {
        this->ctor_success = false;
        hsta_utils.log("failed to get provider\n");
        return;
    }

    auto ofi_rc = fi_fabric(this->prov->fabric_attr, &this->fabric, nullptr);
    if (ofi_rc != FI_SUCCESS) {
        this->ctor_success = false;
        hsta_utils.log("failed to open fabric provider\n");
        return;
    }

    // TODO: check the domain attrs to see if mr_mode specifies FI_MR_VIRT_ADDR
    ofi_rc = fi_domain(this->fabric, this->prov, &this->domain, nullptr);
    if (ofi_rc != FI_SUCCESS) {
        if (hsta_dump_net_config) {
            // set ep_addr_len to 0 here since we won't get the chance below
            this->ep_addr_len = 0UL;
            return;
        } else {
            this->ctor_success = false;
            hsta_utils.log("failed to open fabric access domain\n");
            return;
        }
    }

    // init address vector

    struct fi_av_attr av_attr;
    memset(&av_attr, 0, sizeof(av_attr));
    av_attr.type        = FI_AV_TABLE;
    av_attr.map_addr    = 0;
    av_attr.rx_ctx_bits = 0;
    // TODO: use a shared av
    av_attr.name        = nullptr; // FI_NAMED_AV_<appnum>
    av_attr.flags       = 0;       // FI_READ

    ofi_rc = fi_av_open(this->domain, &av_attr, &this->av, nullptr);
    if (ofi_rc != FI_SUCCESS) {
        this->ctor_success = false;
        hsta_utils.log("failed to open address vector\n");
        return;
    }

    this->init_endpoint_tl();
    this->set_ep_addr_tl();

    this->mapped_addr_table.resize(this->num_procs);

    if (hsta_dump_net_config) {
        // we can't do the av_insert when just dumping the config because in
        // this case hsta_startup_info isn't available
        return;
    }

    // for now (maybe forever), we assume that if endpoint address
    // info is available for node 0, then it's available for all nodes
    if (hsta_startup_info.nodes[0].ep_addrs_available) {
        auto num_addrs = fi_av_insert(
            this->av,
            (void *) &hsta_startup_info.addr_table[0],
            this->num_procs,
            &this->mapped_addr_table[0],
            0UL, nullptr
        );
        if (num_addrs != this->num_procs) {
            this->ctor_success = false;
            hsta_utils.log("failed to insert fabric addresses into address vector\n");
            return;
        }
    }
}

void
DragonFabric_ofi::insert_ep_addr(void *ep_addr, size_t ep_addr_len, int rank)
{
    auto num_addrs = 1;

    auto num_inserted = fi_av_insert(
        this->av,
        ep_addr,
        num_addrs,
        &this->mapped_addr_table[rank],
        0UL, nullptr
    );
    if (num_inserted != num_addrs) {
        this->handle_err("failed to insert fabric address into address vector");
    }
}

DragonFabric_ofi::~DragonFabric_ofi()
{
    // clean up libfabric

#if 0
    auto ofi_rc = fi_close(&this->ep->fid);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_close failed", ofi_rc);
    }

    ofi_rc = fi_close(&this->av->fid);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_close failed", ofi_rc);
    }

    ofi_rc = fi_close(&this->cq->fid);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_close failed", ofi_rc);
    }

    ofi_rc = fi_close(&this->domain->fid);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_close failed", ofi_rc);
    }

    ofi_rc = fi_close(&this->fabric->fid);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_close failed", ofi_rc);
    }

    fi_freeinfo(this->prov);
#endif
}

void DragonFabric_ofi::init_endpoint_tl()
{
    // create endpoint and bind completion queue and address vector to it

    auto ofi_rc = fi_endpoint(this->domain, this->prov, &this->ep, nullptr);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("failed to create fabric endpoint", ofi_rc);
    }

    // init completion queue

    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(cq_attr));
    cq_attr.format = FI_CQ_FORMAT_TAGGED;

    ofi_rc = fi_cq_open(this->domain, &cq_attr, &this->cq, nullptr);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("failed to open completion queue", ofi_rc);
    }

    ofi_rc = fi_ep_bind(this->ep, &this->cq->fid, FI_SEND | FI_RECV | FI_SELECTIVE_COMPLETION);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("failed to bind endpoint to completion queue", ofi_rc);
    }

    ofi_rc = fi_ep_bind(this->ep, &this->av->fid, 0);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("failed to bind endpoint to address vector", ofi_rc);
    }

    ofi_rc = fi_enable(this->ep);
    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("failed to enable endpoint", ofi_rc);
    }
}

void
DragonFabric_ofi::handle_err_ofi(const char *msg, int ofi_err)
{
    if (dragon_hsta_debug) {
        hsta_lock_acquire(hsta_utils.lock);
        fprintf(hsta_dbg_file, "DragonFabric error: %s,\n"
                                    "==> at line %d in file %s\n"
                                    "==> with err=%d: %s\n", msg, __LINE__, __FILE__, ofi_err, fi_strerror(ofi_err));
        fprintf(hsta_dbg_file, "\n%s\n\n", hsta_utils.get_backtrace().c_str());
        fflush(hsta_dbg_file);
        hsta_lock_release(hsta_utils.lock);
    }
    hsta_utils.graceful_exit(SIGINT);
}

struct fi_info *
DragonFabric_ofi::get_provider_tl()
{
    struct fi_info *prov = nullptr;
    struct fi_info *prov_list = nullptr;
    std::vector<struct fi_info *> providers;

    char cxi_pid[8];
    snprintf(cxi_pid, sizeof(cxi_pid), "%d", this->nic_idx);

    char dev_name[64];
    snprintf(dev_name, sizeof(dev_name), "cxi%d", this->nic_idx);

    auto ofi_rc = fi_getinfo(fi_version(), dev_name, cxi_pid, 0UL, nullptr, &prov_list);
    if (ofi_rc != FI_SUCCESS) {
        hsta_utils.log("fi_getinfo failed\n");
        return nullptr;
    }

    while (prov_list) {
        if (   prov_list->nic == nullptr
            || prov_list->nic->device_attr == nullptr
            || prov_list->nic->device_attr->name == nullptr
        ) {
            hsta_utils.log("nic info from fi_getinfo is not available\n");
            return nullptr;
        }

        if (strcmp(prov_list->nic->device_attr->name, dev_name) == 0) {
            prov = prov_list;
            break;
        }
        prov_list = prov_list->next;
    }

    if (prov == nullptr) {
        hsta_utils.log("cxi provider not found\n");
        return nullptr;
    }

    // FI_SOURCE causes a severe performance penalty at scale
    prov->caps                 &= ~(FI_SOURCE);
    prov->rx_attr->caps        &= ~(FI_SOURCE);
    // TODO: is it necessary to update these values?
    prov->domain_attr->av_type =  FI_AV_TABLE;
    prov->domain_attr->tclass  =  FI_TC_BEST_EFFORT;

    return prov;
}

void
DragonFabric_ofi::log_nic_info()
{
    auto dfabric = (DragonFabric_ofi *) hsta_agent[0]->network.get_dfabric();
    if (dfabric->prov == nullptr || dfabric->prov->nic == nullptr || dfabric->prov->nic->bus_attr == nullptr) {
        fprintf(hsta_dbg_file, "NIC INFO NOT AVAILABLE\n");
        return;
    }

    fprintf(hsta_dbg_file, "NIC INFO:\n");
    for (auto agent: hsta_agent) {
        auto dfabric = (DragonFabric_ofi *) agent->network.get_dfabric();
        fprintf(
            hsta_dbg_file, "> agent %d: dev name=%s, link addr=%s, pci domain=0x%x, pci bus=0x%x, pci dev=0x%x, pci func=0x%x\n",
            agent->idx,
            dfabric->prov->nic->device_attr->name,
            dfabric->prov->nic->link_attr->address,
            dfabric->prov->nic->bus_attr->attr.pci.domain_id,
            dfabric->prov->nic->bus_attr->attr.pci.bus_id,
            dfabric->prov->nic->bus_attr->attr.pci.device_id,
            dfabric->prov->nic->bus_attr->attr.pci.function_id
        );
    }
    fprintf(hsta_dbg_file, "\n");
    fflush(hsta_dbg_file);
}

void
DragonFabric_ofi::send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe)
{
    auto iov_box = this->get_iov_box(base, size, cqe);

    struct fi_msg_tagged msg;

    msg.msg_iov = &iov_box->iov;
    msg.iov_count = 1UL;
    msg.addr = this->mapped_addr_table[rank];
    msg.tag = hsta_utils.hash(tag) & DF_OFI_TAG_MASK;
    msg.context = (void *) cqe;
    msg.data = this->rank;
    msg.desc = nullptr;
    msg.ignore = 0UL;

    auto ofi_rc = FI_SUCCESS;
    uint64_t flags = FI_COMPLETION | FI_REMOTE_CQ_DATA;
    auto first_time = true;
    auto start_time = hsta_utils.get_time();

    while (hsta_utils.get_time() - start_time < DF_DEFAULT_TIMEOUT) {
        ofi_rc = fi_tsendmsg(this->ep, (const struct fi_msg_tagged *) &msg, flags);
        if (ofi_rc == -FI_EAGAIN) {
            this->cq_read();
            if (first_time) {
                hsta_log_history(cqe, "fi_tsendmsg failed with FI_EAGAIN", false, false);
                first_time = false;
            }
        } else {
            break;
        }
    }

    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_tsendmsg failed", ofi_rc);
    }
}

void
DragonFabric_ofi::recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe)
{
    auto iov_box = this->get_iov_box(base, size, cqe);

    struct fi_msg_tagged msg;

    msg.msg_iov = &iov_box->iov;
    msg.iov_count = 1UL;
    msg.addr = (rank == DF_INVALID_RANK) ? FI_ADDR_UNSPEC : this->mapped_addr_table[rank];
    msg.tag = hsta_utils.hash(tag) & DF_OFI_TAG_MASK;
    msg.context = (void *) cqe;
    msg.data = 0UL;
    msg.desc = nullptr;
    msg.ignore = 0UL;

    auto ofi_rc = FI_SUCCESS;
    uint64_t flags = FI_COMPLETION;
    auto first_time = true;
    auto start_time = hsta_utils.get_time();

    while (hsta_utils.get_time() - start_time < DF_DEFAULT_TIMEOUT) {
        ofi_rc = fi_trecvmsg(this->ep, (const struct fi_msg_tagged *) &msg, flags);
        if (ofi_rc == -FI_EAGAIN) {
            this->cq_read();
            if (first_time) {
                hsta_log_history(cqe, "fi_trecvmsg failed with FI_EAGAIN", false, false);
                first_time = false;
            }
        } else {
            break;
        }
    }

    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_trecvmsg failed", ofi_rc);
    }
}

void
DragonFabric_ofi::rdma_get(void *base, size_t size, int rank, CqEvent *cqe)
{
    auto iov_box = this->get_iov_box(base, size, cqe);
    auto riov_box = this->get_riov_box(cqe->rma_iov->core.payload_offset, size, cqe);

    struct fi_msg_rma msg;

    msg.desc = nullptr;
    msg.msg_iov = &iov_box->iov;
    msg.iov_count = 1UL;
    msg.addr = this->mapped_addr_table[rank];
    msg.rma_iov = &riov_box->riov;
    msg.rma_iov_count = 1UL;
    msg.context = (void *) cqe;
    msg.data = 0UL;

    auto ofi_rc = FI_SUCCESS;
    uint64_t flags = FI_COMPLETION;
    auto first_time = true;
    auto start_time = hsta_utils.get_time();

    while (hsta_utils.get_time() - start_time < DF_DEFAULT_TIMEOUT) {
        // TODO: use triggered ops here
        ofi_rc = fi_readmsg(this->ep, &msg, flags);
        if (ofi_rc == -FI_EAGAIN) {
            this->cq_read();
            if (first_time) {
                hsta_log_history(cqe, "fi_readmsg failed with FI_EAGAIN", false, false);
                first_time = false;
            }
        } else {
            break;
        }
    }

    if (ofi_rc != FI_SUCCESS) {
        this->handle_err_ofi("fi_readmsg failed", ofi_rc);
    }
}

void
DragonFabric_ofi::cq_read()
{
    struct fi_cq_tagged_entry ofi_cqes[DF_MAX_CQ_EVENTS];

    // check completion queue

    auto ofi_rc = fi_cq_read(this->cq, (void *) ofi_cqes, DF_MAX_CQ_EVENTS);

    auto num_complete = 0;

    if (ofi_rc > 0) {
        num_complete = (int) ofi_rc;
        for (auto i = 0; i < num_complete; ++i) {
            auto cqe = (CqEvent *) ofi_cqes[i].op_context;
            cqe->tx_is_complete.store(1UL, std::memory_order_relaxed);
            this->cleanup_iov_box(cqe);
        }
    } else if (ofi_rc != -FI_EAGAIN) {
        struct fi_cq_err_entry cqe_err;

        switch (ofi_rc) {
            case -FI_EAVAIL: {
                ofi_rc = fi_cq_readerr(this->cq, &cqe_err, 0UL);
                if (ofi_rc != FI_SUCCESS) {
                    hsta_utils.log("fi_cq_read failed: %s", fi_strerror(ofi_rc));
                }

                switch (cqe_err.err) {
                    case FI_ECANCELED: {
                        // TODO: any cleanup to do here?
                        break;
                    }
                    default: {
                        char msg[256];
                        snprintf(msg, 256, "fi_cq_read failed: ofi err=%d, \"%s\"; ofi cq err=%d, \"%s\"",
                                 cqe_err.err,
                                 fi_strerror(cqe_err.err),
                                 cqe_err.prov_errno,
                                 fi_cq_strerror(this->cq, cqe_err.prov_errno, cqe_err.err_data, nullptr, 0UL));

                        this->handle_err(msg);
                    }
                }

                break;
            }
            default: {
                this->handle_err_ofi("fi_cq_read failed", ofi_rc);
            }
        }
    }
}

void
DragonFabric_ofi::mem_register(const void *base_in, size_t size, RmaIov *rma_iov)
{
    auto base = (void *) base_in;
    auto i = 0;
    auto offset = 0UL;
    struct fid_mr *mr = nullptr;

    rma_iov->packed_rkey_vec->num_rkeys = hsta_agent.size();

    for (auto agent: hsta_agent) {
        auto requested_key = get_mr_key();
        auto dfabric = (DragonFabric_ofi *)agent->network.get_dfabric();

        auto ofi_rc = fi_mr_reg(
            dfabric->domain,
            base,
            size,
            FI_REMOTE_READ | FI_REMOTE_WRITE,
            0UL,
            requested_key,
            0UL,
            &mr,
            nullptr
        );
        if (ofi_rc != FI_SUCCESS) {
            this->handle_err_ofi("fi_mr_reg failed", ofi_rc);
        }

        df_mr_map[base].push_back((void *) mr);

        ofi_rc = fi_mr_bind(mr, &dfabric->ep->fid, 0UL);
        if (ofi_rc != FI_SUCCESS) {
            this->handle_err_ofi("fi_mr_bind failed", ofi_rc);
        }

        ofi_rc = fi_mr_enable(mr);
        if (ofi_rc != FI_SUCCESS) {
            this->handle_err_ofi("fi_mr_enable failed", ofi_rc);
        }

        auto rkey = fi_mr_key(mr);
        memcpy(&rma_iov->packed_rkey_vec->packed_rkeys[offset], &rkey, sizeof(rkey));
        rma_iov->packed_rkey_vec->packed_rkey_sizes[i] = sizeof(rkey);

        hsta_dbg_assert(sizeof(rkey) <= HSTA_MAX_PACKED_RKEY_SIZE);

        ++i;
    }
}

void
DragonFabric_ofi::mem_unregister(const void *base)
{
    for (auto mr_voidp: df_mr_map[(void *)base]) {
        auto mr = (struct fid_mr *) mr_voidp;
        fi_close(&mr->fid);
    }
}

void
DragonFabric_ofi::cancel(CqEvent *cqe)
{
    auto ofi_rc = fi_cancel((fid_t) this->ep, (void *) cqe);
    if (ofi_rc != FI_SUCCESS) {
        fprintf(hsta_dbg_file, "warning: failed to cancel operation");
    }
}

#endif // DF_BACKEND_IS_OFI

// UCX implementation

#ifdef DF_BACKEND_IS_UCX

extern "C" {
DragonFabric *
get_dfabric(dfTransportType transport_type)
{
    return (DragonFabric *)new DragonFabric_ucx(transport_type);
}
}

static void
df_ucx_request_init_cb(void *request)
{
    // no-op
}

DragonFabric_ucx::DragonFabric_ucx(dfTransportType transport_type)
{
    this->backend        = DFABRIC_BACKEND_UCX;
    this->transport_type = transport_type;

    // handle local init

    ucp_config_t *config;
    uint64_t features = 0;
    ucp_params_t ucp_params;

    auto ucs_rc = ucp_config_read(nullptr, nullptr, &config);
    if (ucs_rc != UCS_OK) {
        this->ctor_success = false;
        hsta_utils.log("failed to read config\n");
        return;
    }

    features = UCP_FEATURE_TAG | UCP_FEATURE_RMA;
    ucp_params.features = features;
    // TODO: does sizeof(uint64_t) make sense here? need to review
    // what the request is/what it's used for
    ucp_params.request_size = sizeof(uint64_t);
    ucp_params.request_init = nullptr;
    ucp_params.request_cleanup = nullptr;
    ucp_params.estimated_num_eps = this->num_procs;

    ucp_params.field_mask =
          UCP_PARAM_FIELD_FEATURES
        | UCP_PARAM_FIELD_REQUEST_SIZE
        | UCP_PARAM_FIELD_ESTIMATED_NUM_EPS
        | UCP_PARAM_FIELD_REQUEST_INIT;

    ucs_rc = ucp_init(&ucp_params, config, &this->context);
    if (ucs_rc != UCS_OK) {
        this->ctor_success = false;
        hsta_utils.log("failed to initialize ucp context\n");
        return;
    }

    ucp_config_release(config);

    // create worker

    ucp_worker_params_t worker_params;
    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

    ucs_rc = ucp_worker_create(this->context, &worker_params, &this->worker);
    if (ucs_rc != UCS_OK) {
        this->ctor_success = false;
        hsta_utils.log("failed to create worker\n");
        return;
    }

    // set endpoint address and its length
    this->set_ep_addr_tl();

    this->eps.resize(this->num_procs);
}

DragonFabric_ucx::~DragonFabric_ucx()
{
    // close eps

    const ucp_request_param_t req_params = {
        .op_attr_mask = 0U
    };

    for (auto remote_rank = 0; remote_rank < this->eps.size(); ++remote_rank) {
        // try to close ep

        auto remote_nid = hsta_my_agent->network.get_nid_from_rank(remote_rank);

        auto rank_is_valid =
               remote_rank != this->rank
            && hsta_startup_info.nodes[remote_nid].ep_addr_setup_state == EP_ADDR_SETUP_READY;

        if (rank_is_valid) {
            auto request = ucp_ep_close_nbx(this->eps[remote_rank], &req_params);
            if (request != nullptr) {
                if (UCS_PTR_IS_ERR(request)) {
                    this->handle_err("failed to clean up endpoint");
                }

                // wait (briefly) for this request to complete
                // (10,000 nodes X 8 eps/node X 100us/ep ~= 10s total wait during shutdown)

                auto ucs_rc = UCS_INPROGRESS;
                auto timeout = (double)0.0001; // == 100us
                auto start_time = hsta_utils.get_time();

                while (ucs_rc == UCS_INPROGRESS) {
                    if (hsta_utils.get_time() - start_time >= timeout) {
                        break;
                    }
                    auto ucs_rc = ucp_request_check_status(request);
                }

                if (ucs_rc == UCS_INPROGRESS) {
                    fprintf(hsta_dbg_file, "failed to close endpoint for rank %d\n", remote_rank);
                } else if (ucs_rc != UCS_OK) {
                    this->handle_err("failed to check request status");
                }

                ucp_request_free(request);
            }
        }
    }

    // cleanup worker and context
    ucp_worker_destroy(this->worker);
    // TODO: this is causing failures during HSTA cleanup
    //ucp_cleanup(this->context);
}

static void
hsta_ucx_send_cb(void *request, ucs_status_t ucs_rc, void *user_data)
{
    if (request != nullptr) {
        ucp_request_free(request);
    }

    auto cqe = (CqEvent *) user_data;
    cqe->tx_is_complete.store(1UL, std::memory_order_relaxed);
}

static void
hsta_ucx_recv_cb(void *request, ucs_status_t ucs_rc, const ucp_tag_recv_info_t *info, void *user_data)
{
    ucp_request_free(request);
    auto cqe = (CqEvent *) user_data;
    cqe->tx_is_complete.store(1UL, std::memory_order_relaxed);
}

void
DragonFabric_ucx::insert_ep_addr(void *ep_addr, size_t ep_addr_len, int rank)
{
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = (ucp_address_t *) ep_addr;

    ucp_ep_h ep;
    auto ucs_rc = ucp_ep_create(this->worker, &ep_params, &ep);
    if (ucs_rc != UCS_OK) {
        this->handle_err("failed to create endpoint");
    }

    this->eps[rank] = ep;
}

void
DragonFabric_ucx::send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe)
{
    auto ep = this->eps[rank];

    uint32_t op_attr_mask =
          UCP_OP_ATTR_FIELD_CALLBACK
        | UCP_OP_ATTR_FIELD_USER_DATA;

    const ucp_request_param_t req_params = {
        .op_attr_mask = op_attr_mask,
        .cb           = { .send = hsta_ucx_send_cb },
        .user_data    = cqe
    };

    // TODO: do we need to check for EAGAIN like we do with libfabric?
    auto request = ucp_tag_send_nbx(ep, base, size, tag, &req_params);
    if (request == nullptr) {
        // immediate completion, handle callback manually
        hsta_ucx_send_cb(req_params.request, UCS_OK, req_params.user_data);
    } else if (UCS_PTR_IS_ERR(request)) {
        this->handle_err("failed to post non-blocking send operation");
    }

    cqe->fabric_request = request;
}

void
DragonFabric_ucx::recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe)
{
    ucp_tag_t tag_mask = 0xffffffffffffffffUL;

    uint32_t op_attr_mask =
          UCP_OP_ATTR_FIELD_CALLBACK
        | UCP_OP_ATTR_FIELD_USER_DATA
        | UCP_OP_ATTR_FLAG_NO_IMM_CMPL;

    const ucp_request_param_t req_params = {
        .op_attr_mask = op_attr_mask,
        .cb           = { .recv = hsta_ucx_recv_cb },
        .user_data    = cqe
    };

    // TODO: do we need to check for EAGAIN?
    auto request = ucp_tag_recv_nbx(this->worker, base, size, tag, tag_mask, &req_params);
    if (UCS_PTR_IS_ERR(request)) {
        this->handle_err("failed to post non-blocking recv operation");
    }

    cqe->fabric_request = request;
}

void
DragonFabric_ucx::rdma_get(void *base, size_t size, int rank, CqEvent *cqe)
{
    auto ep = this->eps[rank];

    // TODO: should this be an offset or an address?
    auto remote_addr = cqe->rma_iov->core.payload_offset;
    auto rkey = (ucp_rkey_h) cqe->rma_iov->rkeys[hsta_thread_idx];

    uint32_t op_attr_mask =
          UCP_OP_ATTR_FIELD_CALLBACK
        | UCP_OP_ATTR_FIELD_USER_DATA;

    const ucp_request_param_t param = {
        .op_attr_mask = op_attr_mask,
        .cb           = { .send = hsta_ucx_send_cb },
        .user_data    = cqe
    };

    // TODO: do we need to check for EAGAIN?
    auto request = ucp_get_nbx(ep, base, size, remote_addr, rkey, &param);
    if (request == nullptr) {
        // immediate completion, handle callback manually
        hsta_ucx_send_cb(request, UCS_OK, param.user_data);
    } else if (UCS_PTR_IS_ERR(request)) {
        this->handle_err("failed to post rdma get operation");
    }

    cqe->fabric_request = request;
}

void
DragonFabric_ucx::cq_read()
{
    ucp_worker_progress(this->worker);
}

void
DragonFabric_ucx::mem_register(const void *base_in, size_t size, RmaIov *rma_iov)
{
    auto i = 0;
    auto base = (void *) base_in;
    auto offset = 0ul;

    rma_iov->packed_rkey_vec->num_rkeys = hsta_agent.size();

    for (auto agent: hsta_agent) {
        // register the buffer with the nic

        uint64_t mmap_field_mask =
              UCP_MEM_MAP_PARAM_FIELD_ADDRESS
            | UCP_MEM_MAP_PARAM_FIELD_LENGTH
            | UCP_MEM_MAP_PARAM_FIELD_FLAGS;

        const ucp_mem_map_params_t mem_map_params = {
            .field_mask = mmap_field_mask,
            .address    = base,
            .length     = size,
            .flags      = 0
        };

        ucp_mem_h mem_h;
        auto dfabric = (DragonFabric_ucx *) agent->network.get_dfabric();

        auto ucs_rc = ucp_mem_map(dfabric->context, &mem_map_params, &mem_h);
        if (ucs_rc != UCS_OK) {
            this->handle_err("failed to register memory segment");
        }

        df_mr_map[base].push_back((void *) mem_h);

        // pack the mem handle to get an rkey

        auto packed_rkey = (void *) nullptr;
        auto packed_rkey_size = 0UL;

#if HSTA_USE_RKEY_PACK_DEPRECATED
        ucs_rc = ucp_rkey_pack(
            dfabric->context,
            mem_h,
            &packed_rkey,
            &packed_rkey_size
        );
#else
        // TODO: is this correct?
        uint64_t pack_field_mask = UCP_MEMH_PACK_PARAM_FIELD_FLAGS;

        const ucp_memh_pack_params_t pack_params = {
            .field_mask = pack_field_mask,
            .flags      = 0
        };

        // this function is returning ucs_rc = UCS_ERR_UNSUPPORTED on pea2k,
        // even though ucp_rkey_pack is listed as deprecated in the online
        // documentation, and ucp_memh_pack is its replacement
        ucs_rc = ucp_memh_pack(
            mem_h,
            &pack_params,
            &packed_rkey,
            &packed_rkey_size
        );
#endif
        if (ucs_rc != UCS_OK) {
            this->handle_err("failed to pack memory handle");
        }

        hsta_dbg_assert(packed_rkey_size <= HSTA_MAX_PACKED_RKEY_SIZE);

        rma_iov->packed_rkey_vec->packed_rkey_sizes[i] = packed_rkey_size;
        memcpy(&rma_iov->packed_rkey_vec->packed_rkeys[offset], packed_rkey, packed_rkey_size);
        offset += packed_rkey_size;

        ++i;
    }
}

void
DragonFabric_ucx::mem_unregister(const void *base)
{
    const ucp_memh_buffer_release_params_t release_params = { .field_mask = 0UL };

    for (auto mem_h: df_mr_map[(void *) base]) {
        ucp_mem_unmap(this->context, (ucp_mem_h) mem_h);
        ucp_memh_buffer_release((void *) base, &release_params);
    }
}

void
DragonFabric_ucx::free_packed_rkey(void *packed_rkey)
{
#if HSTA_USE_RKEY_PACK_DEPRECATED
    ucp_rkey_buffer_release(packed_rkey);
#else
    ucp_memh_buffer_release(packed_rkey);
#endif
}

void
DragonFabric_ucx::free_rkey(Rkey rkey)
{
    ucp_rkey_destroy((ucp_rkey_h)rkey);
}

void *
DragonFabric_ucx::unpack_rkey(int src_rank, const void *packed_rkey)
{
    ucp_rkey_h rkey;

    auto ucs_rc = ucp_ep_rkey_unpack(this->eps[src_rank], packed_rkey, &rkey);
    if (ucs_rc != UCS_OK) {
        this->handle_err("failed to unpack rkey");
    }

    return (void *)rkey;
}

void
DragonFabric_ucx::cancel(CqEvent *cqe)
{
    ucp_request_cancel(this->worker, cqe->fabric_request);
}

void
DragonFabric_ucx::log_nic_info()
{
    // TODO: fill in
}

void
DragonFabric_ucx::set_ep_addr_tl()
{
    ucp_worker_attr_t worker_attrs;
    worker_attrs.field_mask =
        UCP_WORKER_ATTR_FIELD_ADDRESS | UCP_WORKER_ATTR_FIELD_ADDRESS_FLAGS;

    worker_attrs.address_flags = UCP_WORKER_ADDRESS_FLAG_NET_ONLY;

    auto ucs_rc = ucp_worker_query(this->worker, &worker_attrs);
    if (ucs_rc != UCS_OK) {
        this->handle_err("failed to get worker address");
    }

    this->ep_addr     = worker_attrs.address;
    this->ep_addr_len = worker_attrs.address_length;

    // TODO: are we calling ucp_worker_release_address anywhere? do we need to?
}

#endif // DF_BACKEND_IS_UCX

// MPI implementation

#ifdef DF_BACKEND_IS_MPI

extern "C" {
DragonFabric *
get_dfabric(dfTransportType transport_type)
{
    return (DragonFabric *)new DragonFabric_mpi(transport_type);
}
}

DragonFabric_mpi::DragonFabric_mpi(dfTransportType transport_type)
{
    this->backend        = DFABRIC_BACKEND_MPI;
    this->transport_type = transport_type;

    auto mpi_errno = MPI_Init(nullptr, nullptr);
    if (mpi_errno != MPI_SUCCESS) {
        this->handle_err("MPI_Init failed");
    }

    mpi_errno = MPI_Comm_rank(MPI_COMM_WORLD, &this->rank);
    if (mpi_errno != MPI_SUCCESS) {
        this->handle_err("MPI_Comm_rank failed");
    }

    mpi_errno = MPI_Comm_size(MPI_COMM_WORLD, &this->num_procs);
    if (mpi_errno != MPI_SUCCESS) {
        this->handle_err("MPI_Comm_size failed");
    }

    this->num_nodes = this->num_procs / hsta_num_threads;
    hsta_dbg_assert(this->num_procs % hsta_num_threads == 0);

    // the MPI backend is always single-threaded
    this->nic_idx = 0;

    // set tag mask using tag_ub

    auto *tag_ub_p = (void *) nullptr;
    auto tag_ub = 0u;
    auto flag = 0;

    mpi_errno = MPI_Attr_get(MPI_COMM_WORLD, MPI_TAG_UB, (void *) &tag_ub_p, &flag);
    if (mpi_errno != MPI_SUCCESS || !flag) {
        this->handle_err("MPI_Attr_get failed");
    }

    tag_ub = *(int *)tag_ub_p;

    auto tag_ub_round_up   = hsta_utils.next_power_of_two(tag_ub);
    auto tag_ub_round_down = (tag_ub == tag_ub_round_up) ? tag_ub_round_up : (tag_ub_round_up >> 1);

    if (tag_ub == tag_ub_round_up - 1u) {
        this->tag_mask = (uint64_t) tag_ub;
    } else {
        this->tag_mask = (uint64_t) (tag_ub_round_down - 1u);
    }
}

DragonFabric_mpi::~DragonFabric_mpi()
{
    auto mpi_errno = MPI_Finalize();
    if (mpi_errno != MPI_SUCCESS) {
        this->handle_err("MPI_Finalize failed");
    }
}

void
DragonFabric_mpi::send(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe)
{
    MPI_Request req;

    // TODO: tag collisions are possible
    auto mpi_tag = (int) (hsta_utils.hash(tag) & this->tag_mask);

    auto mpi_errno = MPI_Isend(
        base, size, MPI_BYTE,
        rank, mpi_tag, MPI_COMM_WORLD,
        &req
    );
    if (mpi_errno != MPI_SUCCESS) {
        this->handle_err("MPI_Isend failed");
    }

    this->cqe_to_mpi_req[cqe] = req;
}

void
DragonFabric_mpi::recv(void *base, size_t size, int rank, uint64_t tag, CqEvent *cqe)
{
    MPI_Request req;

    auto mpi_rank = (rank == DF_INVALID_RANK) ? MPI_ANY_SOURCE : (int) rank;
    // TODO: tag collisions are possible
    auto mpi_tag = (int) (hsta_utils.hash(tag) & this->tag_mask);

    auto mpi_errno = MPI_Irecv(
        base, size, MPI_BYTE,
        mpi_rank, mpi_tag, MPI_COMM_WORLD,
        &req
    );
    if (mpi_errno != MPI_SUCCESS) {
        this->handle_err("MPI_Isend failed");
    }

    this->cqe_to_mpi_req[cqe] = req;
}

void
DragonFabric_mpi::rdma_get(void *base, size_t size, int rank, CqEvent *cqe)
{
    this->handle_err("rdma_get is not implemented for the MPI fabric backend");
}

void
DragonFabric_mpi::cq_read()
{
    auto done_count = 0;
    auto it = this->cqe_to_mpi_req.begin();

    while (it != this->cqe_to_mpi_req.end()) {
        auto cqe = it->first;
        auto req = it->second;

        MPI_Status stat;
        int done;

        auto mpi_rc = MPI_Test(&req, &done, &stat);
        if (mpi_rc != MPI_SUCCESS) {
            this->handle_err("MPI_Test failed");
        }

        auto prev_it = it++;

        if (done) {
            auto rank = stat.MPI_SOURCE;
            cqe->nid = hsta_my_agent->network.get_nid_from_rank(rank);
            // this is ugly, but zero out size to avoid junk high bits
            cqe->size = 0UL;

            mpi_rc = MPI_Get_count(&stat, MPI_BYTE,
                                      reinterpret_cast<int *>(&cqe->size));
            if (mpi_rc != MPI_SUCCESS) {
                this->handle_err("MPI_Get_count failed");
            }

            this->cqe_to_mpi_req.erase(prev_it);

            if (++done_count == DF_MAX_CQ_EVENTS) {
                return;
            }
        }
    }
}

void DragonFabric_mpi::cancel(CqEvent *cqe)
{
    auto req = this->cqe_to_mpi_req[cqe];

    if (req != MPI_REQUEST_NULL) {
        auto mpi_errno = MPI_Cancel(&req);
        if (mpi_errno != MPI_SUCCESS) {
            this->handle_err("failed to cancel operation");
        }
    }
}

#endif // DF_BACKEND_IS_MPI
