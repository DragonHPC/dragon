#include "agent.hpp"
#include "cq.hpp"
#include "dragon_fabric.hpp"
#include "eager.hpp"
#include "globals.hpp"
#include "header.hpp"
#include "magic_numbers.hpp"
#include "mem_descr.hpp"
#include "network.hpp"
#include "obj_ring.hpp"
#include "request.hpp"
#include "tx_queue.hpp"
#include "utils.hpp"
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <string>
#include <unordered_set>

// global vars

uint64_t hsta_work_stealing_threshold = HSTA_DEFAULT_WORK_STEALING_THRESHOLD;
uint64_t dragon_hsta_max_ejection_bytes = HSTA_DEFAULT_MAX_EJECTION_MB * HSTA_NBYTES_IN_MB;
std::unordered_map<dragonULInt, int> hsta_hostid_to_nid;
std::unordered_map<std::string, int> hsta_ip_addr_to_nid;
std::unordered_map<dragonULInt, RmaIov *> hsta_rma_iov;
thread_local std::unordered_map<dragonULInt, RmaIov *> hsta_rma_iov_tl;
thread_local std::vector<bool> hsta_ep_addr_ready_tl;
StartupInfo hsta_startup_info;

// function pointers set via dlsym

extern "C" {
    DragonFabric *(*fn_get_dfabric)(dfTransportType);
}

// static functions

static dfBackendType
str_to_backend(char *fabric_backend_cstr)
{
    if (0 == strcmp(fabric_backend_cstr, "ofi")) {
        return DFABRIC_BACKEND_OFI;
    } else if (0 == strcmp(fabric_backend_cstr, "ucx")) {
        return DFABRIC_BACKEND_UCX;
    } else if (0 == strcmp(fabric_backend_cstr, "mpi")) {
        return DFABRIC_BACKEND_MPI;
    } else if (0 == strcmp(fabric_backend_cstr, "io_uring")) {
        return DFABRIC_BACKEND_IO_URING;
    } else {
        hsta_utils.log("invalid fabric backend type", fabric_backend_cstr);
        hsta_dbg_assert(false);
    }

    return DFABRIC_BACKEND_LAST;
}

static const char *
backend_to_str(dfBackendType fabric_backend)
{
    switch (fabric_backend) {
        case DFABRIC_BACKEND_OFI: {
            return "ofi";
        }
        case DFABRIC_BACKEND_UCX: {
            return "ucx";
        }
        case DFABRIC_BACKEND_MPI: {
            return "mpi";
        }
        case DFABRIC_BACKEND_IO_URING: {
            return "io_uring";
        }
        case DFABRIC_BACKEND_LAST: {
            return "last";
        }
        default: {
            hsta_default_case(fabric_backend);
        }
    }

    return nullptr;
}

static dfTransportType
str_to_transport_type(char *transport_type_cstr)
{
    if (0 == strcmp(transport_type_cstr, "rma")) {
        return DFABRIC_TRANSPORT_TYPE_RMA;
    } else if (0 == strcmp(transport_type_cstr, "p2p")) {
        return DFABRIC_TRANSPORT_TYPE_P2P;
    } else {
        hsta_utils.log("invalid transport type", transport_type_cstr);
        hsta_dbg_assert(false);
    }

    return DFABRIC_TRANSPORT_TYPE_LAST;
}

static DragonFabric *
get_dfabric(dfBackendType backend_type, dfTransportType transport_type)
{
    hsta_utils.log("trying to get dfabric object for %s backend\n", backend_to_str(backend_type));

    const int maxlen = 64;
    char dfabric_libname[maxlen];
    auto backend_cstr = backend_to_str(backend_type);

    if (backend_cstr != nullptr) {
        snprintf(dfabric_libname, maxlen, "libdfabric_%s.so", backend_to_str(backend_type));
    } else {
        return nullptr;
    }

    void *libdfabric = Utils::open_lib(dfabric_libname);
    if (libdfabric == nullptr) {
        hsta_utils.log("unable to dlopen %s\n", dfabric_libname);
        return nullptr;
    }

    Utils::resolve_symbol((void **) &fn_get_dfabric,
                          libdfabric,
                          std::string("get_dfabric"));

    auto dfabric = fn_get_dfabric(transport_type);
    if (dfabric->ctor_success) {
        hsta_utils.log("dfabric ctor successful\n");
        return dfabric;
    } else {
        hsta_utils.log("dfabric ctor failed\n");
        delete dfabric;
        return nullptr;
    }
}

static DragonFabric *
select_fabric()
{
    // TODO: need a way to determine which fabric backends are available
    auto tmp_backend_cstr = getenv("_DRAGON_HSTA_BACKEND_NAME");
    if (tmp_backend_cstr == nullptr) {
        tmp_backend_cstr = (char *)"ofi";
    }
    auto fabric_backend_str = std::string(tmp_backend_cstr);
    auto fabric_backend = str_to_backend(tmp_backend_cstr);

    auto tmp_transport_type_cstr = getenv("_DRAGON_HSTA_TRANSPORT_TYPE");
    // TODO: disabling rma transport for ucx until it's working
    if (tmp_transport_type_cstr == nullptr || fabric_backend == DFABRIC_BACKEND_UCX) {
        tmp_transport_type_cstr = (char *)"p2p";
    }
    auto transport_type_str = std::string(tmp_transport_type_cstr);
    auto transport_type = str_to_transport_type(tmp_transport_type_cstr);

    if (dragon_hsta_debug) {
        hsta_utils.log("fabric selection:\n");

        auto event_backend =
              std::string("> default or user-selected backend: ")
            + fabric_backend_str
            + std::string("\n");

        hsta_utils.log(event_backend.c_str());

        auto event_transport_type =
              std::string("> default or user-selected transport type: ")
            + transport_type_str
            + std::string("\n\n");

        hsta_utils.log(event_transport_type.c_str());
    }

    // set fabric backend

    // only including OFI and UCX backends for now (the MPI backend is
    // currently buggy, and the IO_URING backend doesn't exist yet)
    std::unordered_set<dfBackendType> fabric_backends = { DFABRIC_BACKEND_OFI, DFABRIC_BACKEND_UCX };
    fabric_backends.erase(fabric_backend);

    auto dfabric = get_dfabric(fabric_backend, transport_type);
    if (dfabric == nullptr) {
        for (auto tmp_backend: fabric_backends) {
            auto tmp_transport_type = (tmp_backend == DFABRIC_BACKEND_UCX) ? DFABRIC_TRANSPORT_TYPE_P2P : transport_type;
            dfabric = get_dfabric(tmp_backend, tmp_transport_type);
            if (dfabric != nullptr) {
                break;
            }
        }
    }

    hsta_dbg_assert(dfabric != nullptr);

    return dfabric;
}

// class functions

void
RmaIov::init(Agent *agent, void *pool_base, dragonULInt puid, size_t payload_offset, size_t payload_size, bool requires_rkey_setup)
{
    this->agent = agent;
    this->pool_base = pool_base;
    this->core.puid = puid;
    this->core.payload_offset = payload_offset;
    this->core.payload_size = payload_size;
    this->packed_rkey_vec = nullptr;
    this->rkeys = nullptr;
    this->free_packed_rkeys = false;
    this->free_rkeys = false;
    this->requires_rkey_setup = requires_rkey_setup;
}

void
RmaIov::fini()
{
    auto num_rkeys = hsta_num_threads;

    if (this->free_packed_rkeys) {
        auto offset = 0UL;
        auto dfabric = this->agent->network.get_dfabric();

        for (auto i = 0; i < num_rkeys; ++i) {
            auto packed_rkey = (void *)&this->packed_rkey_vec->packed_rkeys[offset];
            offset += this->packed_rkey_vec->packed_rkey_sizes[i];
            dfabric->free_packed_rkey(packed_rkey);
        }
        free(this->packed_rkey_vec);
    }

    if (this->free_rkeys) {
        auto dfabric = this->agent->network.get_dfabric();

        for (auto i = 0; i < num_rkeys; ++i) {
            dfabric->free_rkey(this->rkeys[i]);
        }
        free(this->rkeys);
    }
}

void
RmaIov::setup_rkeys_at(int target_nid)
{
    // set up rkeys at the target if necessary
    if (this->rkey_setup_table.find(target_nid) == this->rkey_setup_table.end()) {
        if (dragon_hsta_debug) {
            fprintf(hsta_dbg_file, "setting up rkeys at target nid = %d\n\n", target_nid);
        }

        // add nid to table
        this->rkey_setup_table.insert(target_nid);

        // send the rma serialized descriptor to the target agent
        auto cqe =
            this->agent->queue_to_send(
                (const void *) this->packed_rkey_vec,
                sizeof(*this->packed_rkey_vec),
                target_nid,
                0UL,
                0UL,
                HEADER_TYPE_CTRL_RKEY,
                WORK_REQ_TYPE_LAST,
                PROTOCOL_TYPE_LAST,
                nullptr
            );

        auto& tx_queue = this->agent->tx_queues[target_nid];
        tx_queue.try_tx(cqe);
    }
}

// NOTE: this function assumes little-endian byte order... no running on old Power systems :-(
void
Network::get_startup_info()
{
    // get network config for startup

    const size_t max_bytes = 512;
    const size_t expected_items = 1ul;
    uint8_t buf[max_bytes];

    // read num_nodes
    auto num_items = fread((void *) buf, sizeof(hsta_startup_info.num_nodes), expected_items, stdin);
    hsta_dbg_assert(num_items == expected_items);
    hsta_startup_info.num_nodes = *(int *)buf;

    // read this node_idx
    num_items = fread((void *) buf, sizeof(hsta_startup_info.node_idx), expected_items, stdin);
    hsta_dbg_assert(num_items == expected_items);
    hsta_startup_info.node_idx = *(int *)buf;

    // resize the nodes vector
    hsta_startup_info.nodes.resize(hsta_startup_info.num_nodes);

    // offset into the address table
    auto offset = 0ul;

    for (auto node_idx = 0; node_idx < hsta_startup_info.num_nodes; ++node_idx) {
        // read dragon_hsta_debug
        num_items = fread((void *) buf, sizeof(dragon_hsta_debug), expected_items, stdin);
        hsta_dbg_assert(num_items == expected_items);
        dragon_hsta_debug = *(int *)buf;

        // read host_id
        num_items = fread((void *) buf, sizeof(hsta_startup_info.nodes[node_idx].host_id), expected_items, stdin);
        hsta_dbg_assert(num_items == expected_items);
        auto host_id = *(dragonULInt *)buf;
        hsta_startup_info.nodes[node_idx].host_id = host_id;

        hsta_hostid_to_nid[host_id] = node_idx;

        // read ip_addr's length
        num_items = fread((void *) buf, sizeof(int), expected_items, stdin);
        hsta_dbg_assert(num_items == expected_items);
        auto ip_addr_len = *(int *)buf;
        hsta_startup_info.nodes[node_idx].ip_addr_len = ip_addr_len;

        // read ip_addr
        num_items = fread((void *) buf, ip_addr_len, expected_items, stdin);
        hsta_dbg_assert(num_items == expected_items);
        auto ip_addr = std::string((char *)buf, ip_addr_len);
        hsta_startup_info.nodes[node_idx].ip_addr = ip_addr;

        hsta_ip_addr_to_nid[ip_addr] = node_idx;

        // read num_nics
        num_items = fread((void *) buf, sizeof(hsta_startup_info.nodes[node_idx].num_nics), expected_items, stdin);
        hsta_dbg_assert(num_items == expected_items);
        hsta_startup_info.nodes[node_idx].num_nics = *(int *)buf;

        // read ep_addrs_available
        num_items = fread((void *) buf, sizeof(int), expected_items, stdin);
        hsta_dbg_assert(num_items == expected_items);
        hsta_startup_info.nodes[node_idx].ep_addrs_available = (bool) *(int *)buf;

        // read endpoint addresses and lengths

        auto num_nics = hsta_startup_info.nodes[node_idx].num_nics;

        if (hsta_startup_info.nodes[node_idx].ep_addrs_available) {
            hsta_startup_info.nodes[node_idx].have_sent_ep_addrs = true;
            hsta_startup_info.nodes[node_idx].ep_addr_setup_state = EP_ADDR_SETUP_READY;

            for (auto nic_idx = 0; nic_idx < num_nics; ++nic_idx) {
                // read endpoint address length
                num_items = fread((void *) buf, sizeof(int), expected_items, stdin);
                hsta_dbg_assert(num_items == expected_items);
                auto ep_addr_len = *(int *)buf;
                hsta_startup_info.nodes[node_idx].ep_addr_lens.push_back(ep_addr_len);

                // read endpoint address
                num_items = fread((void *) buf, ep_addr_len, expected_items, stdin);
                hsta_dbg_assert(num_items == expected_items);

                // copy endpoint name into address table at proper offset
                // (assuming little-endian byte order here)
                auto addr_table_size = hsta_startup_info.addr_table.size();
                hsta_startup_info.addr_table.resize(ep_addr_len + addr_table_size);

                memcpy(&hsta_startup_info.addr_table[offset], (void *) buf, ep_addr_len);
                offset += ep_addr_len;

                if (dragon_hsta_debug) {
                    auto ep_addr_str = dragon_base64_encode(&hsta_startup_info.addr_table[offset - ep_addr_len], ep_addr_len);
                    fprintf(hsta_dbg_file, "adding endpoint address to startup table: addr = %s, len = %d\n\n", ep_addr_str, ep_addr_len);
                    fflush(hsta_dbg_file);
                    free(ep_addr_str);
                }
            }
        } else {
            hsta_startup_info.nodes[node_idx].have_sent_ep_addrs = false;
            hsta_startup_info.nodes[node_idx].ep_addr_setup_state = EP_ADDR_SETUP_NOT_STARTED;
        }
    }
}

// static members

uint64_t FlowQ::uid_counter = 0ul;

// member functions

void Network::init(Agent *agent)
{
    this->dfabric = select_fabric();
    this->agent   = agent;

    // initialize group parameters

    // TODO: rethink these variables names, since they don't really make sense
    // in the multi-threaded (rather than multi-process on-node) architecture
    this->rank      = this->dfabric->get_rank();
    this->num_ranks = this->dfabric->get_num_ranks();
    this->ppn       = hsta_num_threads;
    this->lrank     = this->rank % this->ppn;
    this->this_nid  = this->rank / this->ppn;
    this->num_nodes = this->num_ranks / this->ppn;

    this->num_pending_eager_rx   = 0ul;
    this->total_rndv_bytes       = 0ul;
    this->pending_ejection_bytes = 0ul;

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        hsta_utils.log("fabric initialized:\n");
        if (!hsta_dump_net_config) {
            hsta_utils.log("> ip addr   = %s\n", hsta_startup_info.nodes[this->this_nid].ip_addr.c_str());
        }
        hsta_utils.log("> this_nid  = %d\n", this->this_nid);
        hsta_utils.log("> num_nodes = %d\n", this->num_nodes);
        hsta_utils.log("> lrank     = %d\n", this->lrank);
        hsta_utils.log("> ppn       = %d\n", this->ppn);
        hsta_utils.log("> rank      = %d\n", this->rank);
        hsta_utils.log("> num_ranks = %d\n\n", this->num_ranks);
    }
#endif // !HSTA_NDEBUG

    // TODO: the fabric memory pool should be created separately

    // get a memory pool to use for allocations

    if (!hsta_dump_net_config) {
        auto gw_chan = agent->gw_chans.peek_front();
        hsta_dbg_assert(gw_chan != nullptr);

        auto dragon_err =
            dragon_channel_get_pool((const dragonChannelDescr_t *) &gw_chan->dragon_gw_ch,
                                    &this->dragon_mem_pool);
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "failed to get gateway channel memory pool");
    }
}

void Network::cancel_pending_cqes()
{
    for (auto& port_flowq : this->flowq_map) {
        auto flow = port_flowq.second;
        while (auto cqe = flow->q.pull_front()) {
            this->dfabric->cancel(cqe);
            hsta_cqe_dec_refcount(cqe, &flow->q);
        }
    }
}

void Network::finalize()
{
    this->cancel_pending_cqes();
    delete this->dfabric;
}

void Network::send_ep_addrs(std::string ip_addr)
{
    // check if we've sent the local endpoint addrs to the remote node
    auto remote_nid = hsta_ip_addr_to_nid[ip_addr];
    if (hsta_startup_info.nodes[remote_nid].have_sent_ep_addrs) {
        return;
    }
    hsta_startup_info.nodes[remote_nid].have_sent_ep_addrs = true;

    // connect to the target rank

    struct sockaddr_in target_addr;
    auto port = 12358;

    auto sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    hsta_dbg_assert(sock_fd != -1);

    target_addr.sin_family = AF_INET;
    target_addr.sin_port = htons(port);

    auto rc = inet_pton(AF_INET, ip_addr.c_str(), &target_addr.sin_addr);
    hsta_dbg_assert(rc == 1);

    auto begin = hsta_utils.get_time();
    auto timeout = 90.0;

    do {
        rc = connect(sock_fd, (struct sockaddr *)&target_addr, sizeof(target_addr));
    }
    while (rc == -1 && (hsta_utils.get_time() - begin < timeout));

    hsta_dbg_assert(rc != -1);

    // send ep addr len and ep addr

    auto node_idx = hsta_startup_info.node_idx;
    auto my_ip_addr = hsta_startup_info.nodes[node_idx].ip_addr;
    auto my_ip_addr_len = hsta_startup_info.nodes[node_idx].ip_addr_len;

    rc = send(sock_fd, &my_ip_addr_len, sizeof(my_ip_addr_len), 0);
    hsta_dbg_assert(rc != -1);

    rc = send(sock_fd, my_ip_addr.c_str(), my_ip_addr_len, 0);
    hsta_dbg_assert(rc != -1);

    for (auto agent: hsta_agent) {
        auto ep_addr_len = (int)agent->network.get_ep_addr_len();
        rc = send(sock_fd, &ep_addr_len, sizeof(ep_addr_len), 0);
        hsta_dbg_assert(rc != -1);
    }

    for (auto agent: hsta_agent) {
        auto ep_addr_len = agent->network.get_ep_addr_len();
        auto ep_addr = agent->network.get_ep_addr();
        rc = send(sock_fd, ep_addr, ep_addr_len, 0);
        hsta_dbg_assert(rc != -1);
    }

    close(sock_fd);
}

void recv_msg(int sock_fd, void *buf, size_t size)
{
    auto tmp_buf = (uint8_t *)buf;
    auto remaining_bytes = size;

    while (remaining_bytes > 0) {
        auto rx_bytes = recv(sock_fd, tmp_buf, remaining_bytes, 0);
        hsta_dbg_assert(rx_bytes != -1);

        tmp_buf += rx_bytes;
        remaining_bytes -= rx_bytes;
    }
}

void Network::ep_addr_setup_progress()
{
    struct sockaddr_in saddr;
    socklen_t addr_len = sizeof(saddr);
    auto port = 12358;

    auto listen_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock_fd == -1 && dragon_hsta_debug) {
        hsta_utils.log("failed to create socket: errno = %d\n", errno);
    }

    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY;
    saddr.sin_port = htons(port);

    auto rc = bind(listen_sock_fd, (struct sockaddr *)&saddr, addr_len);
    if (rc == -1 && dragon_hsta_debug) {
        hsta_utils.log("failed to bind socket: errno = %d\n", errno);
    }

    rc = listen(listen_sock_fd, this->num_ranks - 1);
    if (rc == -1 && dragon_hsta_debug) {
        hsta_utils.log("failed to listen on socket: errno = %d\n", errno);
    }

    while (true) {
        auto sock_fd = accept(listen_sock_fd, (struct sockaddr *)&saddr, &addr_len);
        if (sock_fd == -1 && dragon_hsta_debug) {
            hsta_utils.log("failed to accept connection: errno = %d\n", errno);
        }

        // recv ip_addr_len
        auto const max_ip_addr_len = 16;
        auto ip_addr_len = 0;
        recv_msg(sock_fd, &ip_addr_len, sizeof(ip_addr_len));
        hsta_dbg_assert(ip_addr_len < max_ip_addr_len);

        // recv ip_addr
        char ip_addr[max_ip_addr_len];
        recv_msg(sock_fd, ip_addr, ip_addr_len);

        // get source node index from ip address
        auto ip_addr_str = std::string(ip_addr, ip_addr_len);
        auto remote_nid = hsta_ip_addr_to_nid[ip_addr_str];

        // recv ep_addr_lens
        auto& ep_addr_lens = hsta_startup_info.nodes[remote_nid].ep_addr_lens;
        ep_addr_lens.resize(hsta_agent.size());
        auto total_len = 0UL;

        for (auto i = 0UL; i < hsta_agent.size(); ++i) {
            recv_msg(sock_fd, &ep_addr_lens[i], sizeof(ep_addr_lens[i]));
            hsta_dbg_assert(0 < ep_addr_lens[i] && ep_addr_lens[i] <= HSTA_MAX_EP_ADDR_LEN);
            total_len += ep_addr_lens[i];
        }

        // recv ep_addr
        auto& ep_addrs = hsta_startup_info.nodes[remote_nid].ep_addrs;
        ep_addrs.resize(total_len);
        auto tmp_ep_addrs = &ep_addrs[0];

        for (auto i = 0UL; i < hsta_agent.size(); ++i) {
            recv_msg(sock_fd, tmp_ep_addrs, ep_addr_lens[i]);
            tmp_ep_addrs += ep_addr_lens[i];
        }

        close(sock_fd);

        // done receiving, now insert endpoint addresses so we can use them
        tmp_ep_addrs = (uint8_t *)&ep_addrs[0];
        auto base_rank = this->get_base_rank_from_nid(remote_nid);

        for (auto i = 0; i < (int)hsta_agent.size(); ++i) {
            auto dfabric = agent->network.get_dfabric();
            auto remote_rank = base_rank + i;

            if (dragon_hsta_debug) {
                std::string ep_addr_str = std::string((char *)tmp_ep_addrs, ep_addr_lens[i]);
                fprintf(hsta_dbg_file, "inserting endpoint address: rank = %d, addr = %s\n\n", remote_rank, ep_addr_str.c_str());
                fflush(hsta_dbg_file);
            }

            dfabric->insert_ep_addr(tmp_ep_addrs, ep_addr_lens[i], remote_rank);
            tmp_ep_addrs += ep_addr_lens[i];
        }

        // update the source endpoint address and length in hsta_startup_info,
        // as well as the relevant state machine for endpoint setup
        hsta_lock_acquire(this->state_machine_lock);
        hsta_startup_info.nodes[remote_nid].ep_addr_setup_state = EP_ADDR_SETUP_READY;
        this->send_ep_addrs(ip_addr_str);
        hsta_lock_release(this->state_machine_lock);

        if (hsta_fly_you_fools) {
            break;
        }
    }

    close(listen_sock_fd);
}

void *ep_addr_setup_progress(void *agent_in)
{
    auto agent = (Agent *)agent_in;
    agent->network.ep_addr_setup_progress();
    return nullptr;
}

bool Network::target_ep_addr_is_ready(int target_nid)
{
    if (hsta_ep_addr_ready_tl[target_nid]) {
        return true;
    }

    hsta_lock_acquire(this->state_machine_lock);

    auto ep_addr_setup_state = hsta_startup_info.nodes[target_nid].ep_addr_setup_state;
    auto is_ready = false;

    switch (ep_addr_setup_state) {
        case EP_ADDR_SETUP_NOT_STARTED: {
            if (dragon_hsta_debug) {
                hsta_utils.log("endpoint address setup for nid %s: STARTING\n\n", std::to_string(target_nid).c_str());
            }
            auto target_ip_addr = hsta_startup_info.nodes[target_nid].ip_addr;
            this->send_ep_addrs(target_ip_addr);
            hsta_startup_info.nodes[target_nid].ep_addr_setup_state = EP_ADDR_SETUP_IN_PROGRESS;
            break;
        }
        case EP_ADDR_SETUP_IN_PROGRESS: break;
        case EP_ADDR_SETUP_READY: {
            if (dragon_hsta_debug) {
                hsta_utils.log("endpoint address setup for nid %s: READY\n\n", std::to_string(target_nid).c_str());
            }
            is_ready = true;
            hsta_ep_addr_ready_tl[target_nid] = true;
            break;
        }
        default: {
            hsta_default_case(ep_addr_setup_state);
        }
    }

    hsta_lock_release(this->state_machine_lock);

    return is_ready;
}

void Network::send_eager(CqEvent *cqe)
{
    auto eager_buf = cqe->eager_buf;
    auto rank = this->get_rank_from_nid(cqe->nid);
    auto tag = cqe->header->port;

    this->dfabric->send(
        eager_buf->payload,
        eager_buf->ag_header->size,
        rank, tag, cqe
    );

    hsta_log_history(cqe, "sending eager buffer", false, false);

    auto flow = this->get_flowq(tag);
    flow->q.push_back(cqe);
    hsta_cqe_inc_refcount(cqe, &flow->q, "flowq");
    this->active_flowqs.push_back_unique(flow);
}

// TODO: take ejection credits into account here
void Network::recv_eager(CqEvent *cqe)
{
    auto eager_buf = cqe->eager_buf;
    auto tag = cqe->header->port;

    this->dfabric->recv(
        eager_buf->payload,
        HSTA_EAGER_BUF_MAX_BYTES,
        DF_INVALID_RANK, tag, cqe
    );

    hsta_log_history(cqe, "receiving eager buffer", false, false);

    // increment number of posted eager recvs
    ++this->num_pending_eager_rx;

    auto flow = this->get_flowq(tag);
    flow->q.push_back(cqe);
    hsta_cqe_inc_refcount(cqe, &flow->q, "flowq");
    this->active_flowqs.push_back_unique(flow);
}

void Network::send_rndv(CqEvent *cqe)
{
    auto rank = this->get_rank_from_nid(cqe->nid);
    auto tag = cqe->header->port;

    hsta_dbg_assert(cqe->src_addr != nullptr);
    hsta_dbg_assert(cqe->size > 0ul);

    switch (this->dfabric->transport_type) {
        case DFABRIC_TRANSPORT_TYPE_RMA: {
            // no-op
            cqe->immediate_completion();
            break;
        }
        case DFABRIC_TRANSPORT_TYPE_P2P: {
            this->dfabric->send(cqe->src_addr, cqe->size, rank, tag, cqe);
            hsta_log_history(cqe, "sending rendezvous payload", false, true);
            break;
        }
        default: {
            hsta_default_case(this->get_backend());
        }
    }

    auto flow = this->get_flowq(tag);
    flow->q.push_back(cqe);
    hsta_cqe_inc_refcount(cqe, &flow->q, "flowq");
    this->active_flowqs.push_back_unique(flow);
}

void Network::recv_rndv(CqEvent *cqe)
{
    auto rank = this->get_rank_from_nid(cqe->nid);
    auto tag = cqe->header->port;

    switch (this->dfabric->transport_type) {
        case DFABRIC_TRANSPORT_TYPE_RMA: {
            this->dfabric->rdma_get(cqe->dst_addr, cqe->size, rank, cqe);
            hsta_log_history(cqe, "posting rdma get of rendezvous payload", false, true);
            break;
        }
        case DFABRIC_TRANSPORT_TYPE_P2P: {
            this->dfabric->recv(cqe->dst_addr, cqe->size, rank, tag, cqe);
            hsta_log_history(cqe, "receiving rendezvous payload", false, true);
            break;
        }
        default: {
            hsta_default_case(this->get_backend());
        }
    }
}

void Network::mem_register(void *base, size_t size, RmaIov *rma_iov)
{
    if (dragon_hsta_debug) {
        fprintf(hsta_dbg_file, "registering (base=%p, size=%lu) for pool with puid = %p\n\n", base, size, (void *)rma_iov->core.puid);
        fflush(hsta_dbg_file);
    }
    this->dfabric->mem_register(base, size, rma_iov);
}

void Network::mem_unregister(void *base)
{
    if (dragon_hsta_debug) {
        fprintf(hsta_dbg_file, "unregistering memory for pool with base = %p\n\n", base);
        fflush(hsta_dbg_file);
    }
    this->dfabric->mem_unregister(base);
}

RmaIov *Network::get_rma_iov_rndv(
    void *pool_base,
    size_t pool_size,
    dragonULInt puid,
    size_t payload_offset,
    size_t payload_size,
    int nid,
    PackedRkeyVec *packed_rkey_vec)
{
    // first check our thread-local map to see if we have
    // a cached copy of the rma iovec
    auto primary_rma_iov = hsta_rma_iov_tl[puid];
    if (primary_rma_iov != nullptr) {
        auto rma_iov = this->clone_rma_iov(
            primary_rma_iov,
            payload_offset,
            payload_size,
            nid
        );
        return rma_iov;
    }

    hsta_lock_acquire(hsta_mr_lock);

    // try to look up the rma iovec for this pool, and create
    // one if none is cached
    auto rma_iov = (RmaIov *)nullptr;
    primary_rma_iov = hsta_rma_iov[puid];

    if (primary_rma_iov == nullptr) {
        auto requires_rkey_setup = (packed_rkey_vec == nullptr);

        primary_rma_iov = this->rma_iov_objq.pull_front(
            this->agent,
            pool_base,
            puid,
            payload_offset,
            payload_size,
            requires_rkey_setup
        );

        if (requires_rkey_setup) {
            // this is the tx side, so we need to init the packed_rkey_vec
            // and register the memory pool (mem_register also fills
            // out most of the packed_rkey_vec)

            primary_rma_iov->packed_rkey_vec = (PackedRkeyVec *)malloc(sizeof(PackedRkeyVec));
            hsta_dbg_assert(primary_rma_iov->packed_rkey_vec != nullptr);
            // only the primary rma_iov needs to free the packed_rkey_vec
            primary_rma_iov->free_packed_rkeys = true;

            primary_rma_iov->packed_rkey_vec->puid = puid;
            this->mem_register(pool_base, pool_size, primary_rma_iov);

            // there's no need to clone the primary rma iov on the rx side during
            // rkey setup, so we do it in this if-statement rather than below
            rma_iov = this->clone_rma_iov(primary_rma_iov, nid);
        } else {
            // this is the rkey setup on the rx side, so we need to unpack
            // the packed_rkey_vec to get the rkey pointers
            this->unpack_rkey_vec(nid, packed_rkey_vec, primary_rma_iov);
        }

        // cache the primary rma iov and create a cloned copy
        hsta_rma_iov[puid] = primary_rma_iov;
    } else {
        // 1. this branch can execute on either the tx or rx side
        // 2. but only threads that haven't cached a cloned copy of
        //    the primary rma iov in thread-local memory should ever
        //    enter this branch

        // clone the primary rma iov, but update the offset and size
        rma_iov = this->clone_rma_iov(
            primary_rma_iov,
            payload_offset,
            payload_size,
            nid
        );
    }

    hsta_lock_release(hsta_mr_lock);

    hsta_rma_iov_tl[puid] = primary_rma_iov;

    return rma_iov;
}

// Get an rma iovec for either p2p or rma messagees. The "core" component
// of the rma_iov is sent as eager data with an RTS_CTRL header to enable
// the use of RDMA Gets at the target.
RmaIov *Network::get_rma_iov_tx(const dragonMemoryDescr_t *mem_descr, int target_nid)
{
    RmaIov *rma_iov = nullptr;
    void *payload_base = nullptr;
    size_t payload_size;

    // get payload's base and size
    auto dragon_err =
        dragon_memory_get_pointer(mem_descr, &payload_base);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS,
                            "getting pointer from memory descriptor");

    dragon_err =
        dragon_memory_get_size(mem_descr, &payload_size);
    hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS,
                             "getting size from memory descriptor");

    auto taking_rndv_path = (this->agent->get_send_header_type(payload_size) == HEADER_TYPE_CTRL_RTS);

    if (taking_rndv_path) {
        // for rndv msgs using the rma transport we need to get the rkeys
        // for all the local nics and use a CTRL_RKEY msg to set up the
        // rkeys at the target
        dragonMemoryPoolDescr_t mpool;

        auto dragon_err = dragon_memory_get_pool(mem_descr, &mpool);
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "getting memory pool descriptor from memory descriptor");

        auto puid = mpool._idx;

        void *pool_base = nullptr;
        size_t pool_size;

        dragon_err = dragon_memory_pool_get_pointer(&mpool, &pool_base);
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "getting memory pool base address from descriptor");

        dragon_err = dragon_memory_pool_get_size(&mpool, &pool_size);
        hsta_dbg_no_obj_errcheck(dragon_err, DRAGON_SUCCESS, "getting memory pool size from descriptor");

        auto payload_offset = (size_t)payload_base - (size_t)pool_base;

        rma_iov = this->get_rma_iov_rndv(
            pool_base,
            pool_size,
            puid,
            payload_offset,
            payload_size,
            target_nid,
            nullptr
        );
    } else {
        // for an eager msgs we just need to payload's base and size, no rkey stuff
        // (and the payload vaddr is the offset)
        rma_iov = this->rma_iov_objq.pull_front(
            this->agent,
            nullptr,
            HSTA_INVALID_UINT64,
            (uint64_t)payload_base,
            payload_size,
            false
        );
    }

    return rma_iov;
}

// TODO: move this function to TxQ
void Network::dragon_msg_buf_set_from_eager(CqEvent *cqe)
{
    hsta_dbg_assert((size_t) cqe->dst_addr > (size_t) cqe->eager_buf->payload);

    auto offset = (size_t) cqe->dst_addr - (size_t) cqe->eager_buf->payload;
    cqe->dragon_msg_buf = cqe->eager_buf->mem_descr->get_descr_at_offset(offset, cqe->size);

    cqe->recv_buf_is_allocated = true;
}

void Network::post_next_recv(EagerBuf *eager_buf)
{
    eager_buf->reset();

    auto cqe_next_eager_recv =
        this->agent->create_cqe_aggregated(eager_buf);

    this->recv_eager(cqe_next_eager_recv);
}

void Network::handle_disag_tx_cq_event(CqEvent *cqe)
{
    hsta_log_history(cqe, "handling tx cq event", false, true);

    if (!cqe->needs_ack_for_completion && !cqe->work_req->is_complete)
    {
        assert(cqe->work_req != nullptr);
        cqe->work_req->complete(cqe);
    }
}

void Network::handle_rx_ctrl_conn_event(CqEvent *cqe)
{
    hsta_dbg_assert(cqe->dst_addr != nullptr);
    hsta_dbg_assert(cqe->size > 0);

    auto header = cqe->header;
    auto c_uid = header->c_uid;
    auto& chan_from_cuid = this->agent->chan_from_cuid;

    if (chan_from_cuid[c_uid] == nullptr) {
        auto conn_data = (uint8_t *)cqe->dst_addr;
        auto conn_data_size = cqe->size;

        chan_from_cuid[c_uid] =
            this->agent->chan_objq.pull_front(
                this->agent,
                c_uid,
                nullptr,
                conn_data,
                conn_data_size,
                true
            );
    }
}

void Network::handle_rx_ctrl_rkey(CqEvent *cqe)
{
    auto packed_rkey_vec = (PackedRkeyVec *)cqe->dst_addr;
    auto puid = packed_rkey_vec->puid;

    get_rma_iov_rndv(
        nullptr,
        HSTA_INVALID_UINT64,
        puid,
        HSTA_INVALID_UINT64,
        HSTA_INVALID_UINT64,
        cqe->nid,
        packed_rkey_vec
    );
}

void Network::handle_rx_ctrl_rts_event(CqEvent *cqe)
{
    // save a pointer to the rma_iov core (in eager buf memory)
    // before allocating a new dst_addr
    auto riov_core = (RmaIovCore *)cqe->dst_addr;

    // update the cqe (primarily its size member) before allocating a recv buffer
    cqe->size = riov_core->payload_size;
    cqe->traffic_type = TRAFFIC_TYPE_RX;
    cqe->header->set_type(HEADER_TYPE_DATA_RNDV);

    // get rma_iov for the recv/rdma get operation
    cqe->rma_iov = this->get_rma_iov_rndv(
        nullptr,
        HSTA_INVALID_UINT64,
        riov_core->puid,
        riov_core->payload_offset,
        riov_core->payload_size,
        cqe->nid,
        nullptr
    );

    cqe->src_addr = cqe->rma_iov->get_payload_base();

    // add cqe to its flowq and set the flowq as active
    auto flow = this->get_flowq(cqe->header->port);
    flow->q.push_back(cqe);
    hsta_cqe_inc_refcount(cqe, &flow->q, "flowq");
    this->active_flowqs.push_back_unique(flow);

    if (cqe->header->get_protocol_type() == PROTOCOL_TYPE_SENDMSG) {
        // queue channel operation now to maintain correct ordering
        cqe->target_ch->send_to_channel(&cqe);
    } else {
        this->return_response(cqe);
    }
}

void Network::handle_rx_ctrl_getmsg_event(CqEvent *cqe)
{
    // get msg from target channel and queue up work request for response
    cqe->target_ch->recv_from_channel(&cqe);
}

void Network::handle_rx_ctrl_poll_event(CqEvent *cqe)
{
    // poll target channel and queue up work request for response
    cqe->target_ch->poll_channel(&cqe);
}

void Network::handle_rx_ctrl_err_event(CqEvent *cqe_resp)
{
    switch (cqe_resp->header->get_protocol_type())
    {
        case PROTOCOL_TYPE_SENDMSG:
        {
            auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[cqe_resp->header->port];
            auto cqe_req = cqes_awaiting_resp.pull_front();
            // the refcount should be decremented after the last use
            // of the cqe in the current function

            if (dragon_hsta_debug) {
                hsta_dbg_assert(cqe_req != nullptr);
                hsta_dbg_assert(cqe_req->header->seqnum == cqe_resp->header->seqnum);
            }

            auto event_info = (EventInfo *) cqe_resp->dst_addr;
            cqe_resp->dragon_err = event_info->err;

            auto work_req = cqe_req->work_req;
            work_req->complete(cqe_resp);

            hsta_cqe_dec_refcount(cqe_req, &cqes_awaiting_resp);

            break;
        }
        case PROTOCOL_TYPE_GETMSG:
        {
            auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[cqe_resp->header->port];
            auto cqe_req = cqes_awaiting_resp.pull_front();
            // the refcount should be decremented after the last use
            // of the cqe in the current function

            if (dragon_hsta_debug) {
                hsta_dbg_assert(cqe_req != nullptr);
                // we assume that get_msg responses don't have to be strictly ordered
                //hsta_dbg_assert(cqe_req->header->seqnum == cqe_resp->header->seqnum);
            }

            auto event_info = (EventInfo *) cqe_resp->dst_addr;
            cqe_resp->dragon_err = event_info->err;

            auto work_req = cqe_req->work_req;
            work_req->complete(cqe_resp);

            hsta_cqe_dec_refcount(cqe_req, &cqes_awaiting_resp);

            break;
        }
        case PROTOCOL_TYPE_POLL:
        {
            auto event_info = (EventInfo *) cqe_resp->dst_addr;

            cqe_resp->dragon_err = event_info->err;
            cqe_resp->event_mask = event_info->mask;

            auto& cqes_awaiting_resp = hsta_cqes_awaiting_resp[cqe_resp->header->port];
            auto cqe_req = cqes_awaiting_resp.pull_front();
            // the refcount should be decremented after the last use
            // of the cqe in the current function

            if (dragon_hsta_debug) {
                hsta_dbg_assert(cqe_req != nullptr);
                hsta_dbg_assert(cqe_req->header->seqnum == cqe_resp->header->seqnum);
            }

            auto work_req = cqe_req->work_req;
            work_req->event_info = *event_info;
            work_req->complete(cqe_resp);

            hsta_cqe_dec_refcount(cqe_req, &cqes_awaiting_resp);

            break;
        }
        default:
        {
            hsta_default_case(cqe_resp->header->get_protocol_type());
        }
    }
}

void Network::handle_rx_data_eager_event(CqEvent *cqe)
{
    // this cqe needs to hold a reference to the eager buf, since the
    // eager buf contains its payload (a non-zero eager buf reference
    // means the refcount will be incremented in send_to_channel, and
    // decremented upon completion in try_send)
    cqe->eager_buf_ref = 1;
    cqe->payload_has_arrived = true;

    switch (cqe->header->get_protocol_type())
    {
        case PROTOCOL_TYPE_SENDMSG:
        {
            this->dragon_msg_buf_set_from_eager(cqe);
            cqe->target_ch->send_to_channel(&cqe);
            break;
        }
        case PROTOCOL_TYPE_GETMSG:
        {
            this->return_response(cqe);
            break;
        }
        default:
        {
            hsta_default_case(cqe->header->get_protocol_type());
        }
    }
}

void Network::handle_rx_data_rndv_event(CqEvent *cqe)
{
    // set flag indicating payload has arrived
    cqe->payload_has_arrived = true;

    // update ejection credits for the work agent
    auto work_agent = hsta_agent[cqe->work_agent_idx];
    hsta_dbg_assert(work_agent->network.pending_ejection_bytes >= cqe->size);
    work_agent->network.pending_ejection_bytes -= cqe->size;
}

void Network::handle_disag_rx_cq_event(CqEvent *cqe)
{
    auto header = cqe->header;
    cqe->target_ch = this->agent->chan_from_cuid[header->c_uid];

    auto header_type = cqe->header->get_type();
    auto must_init_flow =
        (header_type == HEADER_TYPE_DATA_EAGER || header_type == HEADER_TYPE_DATA_RNDV) && (header->get_protocol_type() == PROTOCOL_TYPE_SENDMSG);

    if (must_init_flow) {
        cqe->target_ch->init_flow(header->port, header->get_send_return_mode());
    }

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug)
    {
        std::string event = std::string("handling rx cq event: ") + std::string(cqe->header->type_to_string());
        hsta_log_history(cqe, event.c_str(), false, true);
    }
#endif // HSTA_NDEBUG

    switch (header->get_type())
    {
        case HEADER_TYPE_CTRL_CONN:   this->handle_rx_ctrl_conn_event(cqe); break;
        case HEADER_TYPE_CTRL_RKEY:   this->handle_rx_ctrl_rkey(cqe); break;
        case HEADER_TYPE_CTRL_RTS:    this->handle_rx_ctrl_rts_event(cqe); break;
        case HEADER_TYPE_CTRL_GETMSG: this->handle_rx_ctrl_getmsg_event(cqe); break;
        case HEADER_TYPE_CTRL_POLL:   this->handle_rx_ctrl_poll_event(cqe); break;
        case HEADER_TYPE_CTRL_ERR:    this->handle_rx_ctrl_err_event(cqe); break;
        case HEADER_TYPE_DATA_EAGER:  this->handle_rx_data_eager_event(cqe); break;
        case HEADER_TYPE_DATA_RNDV:   this->handle_rx_data_rndv_event(cqe); break;
        default:                      hsta_default_case(header->get_type());
    }
}

void Network::process_tx_eager_buf(CqEvent *cqe_ag)
{
    auto eager_buf = cqe_ag->eager_buf;

#ifndef HSTA_NDEBUG
    if (dragon_hsta_debug) {
        hsta_dbg_assert(eager_buf != nullptr);
        hsta_dbg_assert(eager_buf->traffic_type == TRAFFIC_TYPE_TX);
    }
#endif // !HSTA_NDEBUG

    while (auto cqe_disag = eager_buf->stashed_cqes.pull_front()) {
        hsta_cqe_dec_refcount(cqe_disag, &eager_buf->stashed_cqes);
    }

    this->agent->update_tx_credits(eager_buf);
}

void Network::process_rx_eager_buf(CqEvent *cqe_ag)
{
    auto eager_buf = cqe_ag->eager_buf;

    hsta_dbg_assert(eager_buf != nullptr);
    hsta_dbg_assert(eager_buf->traffic_type == TRAFFIC_TYPE_RX);

    cqe_ag->nid  = eager_buf->ag_header->nid;
    cqe_ag->size = eager_buf->ag_header->size;

    // process headers in an in-coming eager_buf and create headers
    // and cqes for each one

    void *dst_addr = nullptr;
    eager_size_t size;

    while (auto tmp_header = eager_buf->get_next_header(&dst_addr, &size))
    {
        // TODO: try to avoid unaligned accesses here
        auto header =
            this->agent->header_objq.pull_front(
                tmp_header->get_type(),
                tmp_header->get_protocol_type(),
                tmp_header->get_send_return_mode(),
                tmp_header->get_timeout(),
                tmp_header->eager_size,
                tmp_header->c_uid,
                tmp_header->port,
                tmp_header->get_clientid(),
                tmp_header->get_hints()
            );

        if (dragon_hsta_debug)
        {
            // TODO: move this inside Header::init
            header->seqnum   = tmp_header->seqnum;
            header->checksum = tmp_header->checksum;
            header->c_uid     = tmp_header->c_uid;
        }

        auto cqe =
            this->agent->cqe_objq.pull_front(
                header,
                cqe_ag->nid,
                nullptr,  // src_addr
                dst_addr,
                (size_t) size,
                false,    // is_aggregated
                eager_buf,
                nullptr,  // work_req
                this->agent,
                TRAFFIC_TYPE_RX
            );
        // TODO: handle out-of-memory conditions
        hsta_dbg_assert(cqe != nullptr);

        this->handle_disag_rx_cq_event(cqe);

        // in general we would call cleanup_cqe() inside dec_refcount(), but
        // this is a special case since the cqe wasn't pushed onto or pulled
        // off of a queue
        if (cqe->get_refcount() == 0ul) {
            hsta_my_agent->cleanup_cqe(cqe);
        }
    }

    // decrement number of posted eager recvs
    --this->num_pending_eager_rx;

    // add eager buffer to list of buffers with pending channel operations
    auto queue_idx = eager_buf->queue_index;
    auto rx_queue = &hsta_my_agent->rx_queues[queue_idx];
    rx_queue->pending_ch_op_bufs.push_back(eager_buf);
}

void Network::process_eager_buf(CqEvent *cqe)
{
    if (cqe->traffic_type == TRAFFIC_TYPE_TX)
    {
        this->process_tx_eager_buf(cqe);
    }
    else
    {
        this->process_rx_eager_buf(cqe);
    }
}

void Network::handle_disag_cq_event(CqEvent *cqe)
{
    if (cqe->traffic_type == TRAFFIC_TYPE_TX)
    {
        this->handle_disag_tx_cq_event(cqe);
    }
    else
    {
        this->handle_disag_rx_cq_event(cqe);
    }
}

void Network::handle_cq_event(CqEvent *cqe)
{
    switch (cqe->header->get_type())
    {
        case HEADER_TYPE_DATA_EAGER:
        {
            hsta_dbg_assert(cqe->is_aggregated);
            this->process_eager_buf(cqe);
            break;
        }
        case HEADER_TYPE_DATA_RNDV:
        {
            hsta_dbg_assert(!cqe->is_aggregated);
            this->handle_disag_cq_event(cqe);
            break;
        }
        default:
        {
            hsta_default_case(cqe->header->get_type());
        }
    }
}

/*
 * Work-stealing implementation (only applies to ofi_rma backend)
 * --------------------------------------------------------------
 *
 * - Flow agent: The agent object generally used by a thread, and the
 *   one responsible for maintaining the correct order of completions.
 *   If a thread's index is i, then hsta_agent[i] is the flow agent for
 *   that thread.
 * - Work agent: The agent whose nic handles the data movement associated
 *   with a rendezvous receive operation.
 * - <ring access function>_ts: For example, pull_front_ts. The _ts
 *   here indicates that ring access function is optionally thread-
 *   safe. The boolean parameter passed to the function indicates if
 *   thread-safety is needed.
 *
 * When processing the rndv recv work queue (i.e., rdma get operations that
 * are ready to be posted), a thread will first process the queue
 * belonging to its flow agent. As long as a thread still has ejection
 * credits, it will continue on to the following agents in order of their
 * index and attempt to steal rdma get work items from each agent's queue.
 *
 * Some time after the rdma get has been posted, a cqe corresponding to the
 * rdma Get will be obtained from a call to fi_cq_read by the work agent
 * (assuming this cqe was stolen). This cqe's tx_is_complete flag will be
 * updated atomically to indicate completion of the network operation to the
 * flow agent.
 *
 * The two main points of interaction between threads in the work-stealing
 * implementation are (1) registering a new memory pool for all threads, and
 * (2) accessing rndv recv work queues containing rdma Get work items.
 */

void
Network::process_cq_events()
{
    this->dfabric->cq_read();

    auto num_processed = 0ul;
    auto num_flowqs = this->active_flowqs.size();

    while (auto flow = this->active_flowqs.pull_front_unique()) {
        // handle cqes that are complete and at the front of their flowq
        while (auto cqe = flow->q.peek_front()) {
            if (cqe->tx_is_complete.load(std::memory_order_relaxed)) {
                hsta_log_history(cqe, "network operation complete", false, true);
                // if tmp_cqe belongs to this agent, then handle the cq event
                this->handle_cq_event(cqe);
                flow->q.pop_front();
                hsta_cqe_dec_refcount(cqe, &flow->q);
            } else {
                break;
            }
        }

        if (!flow->q.empty()) {
            this->active_flowqs.push_back_unique(flow);
        }

        if (++num_processed == num_flowqs) {
            break;
        }
    }
}

ObjectRing<CqEvent *> *
Network::get_work_from_agent(Agent *agent)
{
    auto agent_is_local = (agent->idx == hsta_thread_idx);

    while (auto cqe = agent->network.rndv_recv_workq.peek_front()) {
        // only steal work that's worth stealing
        if (!agent_is_local && (cqe->size < hsta_work_stealing_threshold)) {
            break;
        }

        if (this->ejection_credits_available(cqe)) {
            this->work.push_back(cqe);
            hsta_cqe_inc_refcount(cqe, &this->work, "work");

            agent->network.rndv_recv_workq.pop_front();
            hsta_cqe_dec_refcount(cqe, &agent->network.rndv_recv_workq);

            // set work agent and update its available ejection credits
            cqe->work_agent_idx = hsta_thread_idx;
            this->pending_ejection_bytes += cqe->size;
        } else {
            break;
        }
    }

    return &this->work;
}

bool
Network::try_recv_rndv(CqEvent *cqe)
{
    // OPTIMIZATION: recv_buf_is_allocated being false can block subsequent
    // cqes from being progressed, e.g., a scenario where a 128KB recv blocks
    // a bunch of subsequent 4KB recvs from being posted

    if (cqe->recv_buf_is_allocated) {
        // worker agent posts recv
        this->recv_rndv(cqe);
        return true;
    } else {
        return false;
    }
}

void
Network::process_rndv_recv_workq()
{
    // loop over agents (starting with my own) and try to make
    // progress posting rdma get operations
    for (auto i = 0; i < hsta_num_threads; ++i) {
        auto agent_idx = (hsta_thread_idx + i) % hsta_num_threads;
        auto agent = hsta_agent[agent_idx];

        // to keep the flowq entries in order, rndv_recv_workq must be processed
        // while holding a lock, rather than protecting individual rndv_recv_workq
        // accesses with a lock
        if (!hsta_lock_try_acquire(agent->network.rndv_recv_workq_lock)) {
            continue;
        }

        auto work = this->get_work_from_agent(agent);

        hsta_lock_release(agent->network.rndv_recv_workq_lock);

        // handle new work items
        while (auto cqe = work->peek_front()) {
            if (this->try_recv_rndv(cqe)) {
                work->pop_front();
                hsta_cqe_dec_refcount(cqe, work);

                if (dragon_hsta_debug) {
                    this->total_rndv_bytes += cqe->size;

                    auto agent_is_local = (agent_idx == hsta_thread_idx);
                    if (!agent_is_local) {
                        char event[200];
                        sprintf(event, "agent %d stole this cqe from agent %d", hsta_thread_idx, agent_idx);
                        hsta_log_history(cqe, event, false, false);
                    }
                }
            } else {
                return;
            }
        }
    }
}

void
Network::process_pending_responses()
{
    auto num_cqes = 0UL;
    auto total_cqes = this->pending_responses.size();

    while (auto cqe_resp = this->pending_responses.pull_front()) {
        if (!cqe_resp->try_return()) {
            this->pending_responses.push_back(cqe_resp);
        }
        else {
            hsta_cqe_dec_refcount(cqe_resp, &this->pending_responses);
        }

        if (++num_cqes == total_cqes) {
            break;
        }
    }
}
