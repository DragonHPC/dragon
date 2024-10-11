#include "agent.hpp"
#include "mem_descr.hpp"
#include "tx_queue.hpp"

uint64_t TxQ::uid_counter = 0ul;

TxQ::TxQ()
{
    this->read_counter                = 0ul;
    this->write_counter               = 0ul;
    this->progress_blocked_no_credits = false;

    this->uid = this->uid_counter++;
}

void TxQ::cleanup()
{
    while (auto eager_buf = this->eager_bufs.pull_front()) {
        this->agent->eager_buf_objq.push_back(eager_buf);
    }
}

void TxQ::new_eager_buf()
{
    auto *eager_buf = hsta_my_agent->eager_buf_objq.pull_front(hsta_my_agent, TRAFFIC_TYPE_TX, this->nid);
    this->eager_bufs.push_back(eager_buf);
}

/**
 * @brief Verifies that there's available tx credits
 */

bool TxQ::tx_credits_available()
{
    hsta_dbg_assert(this->write_counter >= this->read_counter);

    if (this->write_counter - this->read_counter >= this->eager_bufs.size())
    {
        hsta_dbg_printf(this, std::string("found insufficient tx credits"));

        // set flag indicating that progress is blocked on this
        // tx queue until credits can be replenished

        this->progress_blocked_no_credits = true;
        return false;
    }
    else
    {
        return true;
    }
}

/**
 * @brief Gets an eager buffer for small messages. If no buffer is available,
 *        then return nullptr indicating resource exhaustion.
 */

EagerBuf *TxQ::get_eager_buf(CqEvent *cqe)
{
    // if there's an eager buffer already in use, make sure that
    // it has space for the new header and any eager data
    if (auto eager_buf = this->active_eager_bufs.get_back()) {
        auto remaining_size = (size_t) (HSTA_EAGER_BUF_MAX_BYTES - eager_buf->ag_header->size);
        if (remaining_size < cqe->space_needed()) {
            // the no_space flag indicates that the eager buffer is ready
            // to be purged
            eager_buf->no_space = true;
        } else {
            return eager_buf;
        }
    }

    // if we don't have an active eager buf with enough space,
    // try to allocate a new one
    if (this->tx_credits_available()) {
        auto eager_buf = this->next_eager_buf();
        this->active_eager_bufs.push_back(eager_buf);
        return eager_buf;
    }

    return nullptr;
}

/**
 * @brief Tries to queue a header (specified by a cqe) onto this tx queue.
 */

bool TxQ::try_tx(CqEvent *cqe)
{
    if (cqe != nullptr) {
        this->overflow_cqes.push_back(cqe);
        this->agent->active_tx_queues.push_back_unique(this);
        hsta_cqe_inc_refcount(cqe, &this->overflow_cqes, "overflow_cqes");
        if (dragon_hsta_debug) {
            std::string event =
                std::string("adding cqe to overflow queue, queue size = ") + std::to_string(this->overflow_cqes.size());
            hsta_log_history(cqe, event.c_str(), false, false);
            this->check_for_duplicates(cqe);
        }
    }

    if (auto first_cqe = this->overflow_cqes.peek_front()) {
        if (auto eager_buf = this->get_eager_buf(first_cqe)) {
            eager_buf->insert(first_cqe);
            this->overflow_cqes.pop_front();
            hsta_cqe_dec_refcount(first_cqe, &this->overflow_cqes);
            return true;
        } else {
            hsta_log_history(first_cqe, "failed to get eager buf", false, false);
            return false;
        }
    }

    return false;
}

/*
 * rx queue functions
 */

RxQ::RxQ()
{
    this->failed_attempts = 0;
}

void RxQ::cleanup()
{
    while (auto eager_buf = this->pending_rx_eager_bufs.pull_front()) {
        this->agent->eager_buf_objq.push_back(eager_buf);
    }

    while (auto eager_buf = this->pending_ch_op_bufs.pull_front()) {
        this->agent->eager_buf_objq.push_back(eager_buf);
    }
}

void RxQ::post_next_recv(EagerBuf *eager_buf)
{
    eager_buf->reset();

    auto *cqe_next_eager_recv =
        this->agent->create_cqe_aggregated(eager_buf);

    this->agent->network.recv_eager(cqe_next_eager_recv);
}

void RxQ::new_eager_buf()
{
    // allocate a new eager buffer
    auto *eager_buf = hsta_my_agent->eager_buf_objq.pull_front(hsta_my_agent, TRAFFIC_TYPE_RX, 0);

    // post first recv for this eager buffer
    this->post_next_recv(eager_buf);
}
