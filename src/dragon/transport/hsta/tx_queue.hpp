#ifndef TX_QUEUE_HPP
#define TX_QUEUE_HPP

#include "cq.hpp"
#include "extras.hpp"
#include "eager.hpp"
#include "globals.hpp"
#include "utils.hpp"
#include <bits/stdint-uintn.h>
#include <unordered_set>

class TxQ
{
public:

    // data

    static uint64_t uid_counter;
    uint64_t uid;
    Agent *agent;
    ObjectRing<EagerBuf *> eager_bufs;
    uint64_t read_counter;
    uint64_t write_counter;
    int nid;
    bool progress_blocked_no_credits;
    ObjectRing<EagerBuf *> active_eager_bufs;
    ObjectRing<CqEvent *> overflow_cqes;
    std::unordered_map<uint64_t, bool> already_seen_cqes;
#ifndef HSTA_NDEBUG
    std::unordered_set<EagerBuf *> free_eager_bufs;
#endif // !HSTA_NDEBUG

    // functions

    TxQ();
    void cleanup();
    void new_eager_buf();
    bool tx_credits_available();
    EagerBuf *get_eager_buf(CqEvent *cqe);
    bool try_tx(CqEvent *cqe);

    /**
     * @brief Determines read index (read counter % size)
     *
     * @return The value of the read index
     */

    uint64_t read_index()
    {
        return this->read_counter & (this->eager_bufs.size() - 1);
    }

    /**
     * @brief Determines read index (read counter % size),
     *        and increments the read counter
     *
     * @return The value of the read index
     */

    uint64_t read_index_inc()
    {
        return (this->read_counter++) & (this->eager_bufs.size() - 1);
    }

    /**
     * @brief Determines write index (write counter % size)
     *
     * @return The value of the write index
     */

    uint64_t write_index()
    {
        return this->write_counter & (this->eager_bufs.size() - 1);
    }

    /**
     * @brief Determines write index (write counter % size),
     *        and increments the write counter
     *
     * @return The value of the write index
     */

    uint64_t write_index_inc()
    {
        return (this->write_counter++) & (this->eager_bufs.size() - 1);
    }

    /**
     * @brief Get the next available eager buffer on the transmit queue
     *
     * @return Returns a pointer to the eager buffer
     */

    EagerBuf *next_eager_buf()
    {
        auto *eager_buf = this->eager_bufs[this->write_index_inc()];
        eager_buf->reset();
        return eager_buf;
    }

    void check_for_duplicates(CqEvent *cqe)
    {
        if (this->already_seen_cqes[cqe->uid]) {
            hsta_dbg_assert(false);
        } else {
            this->already_seen_cqes[cqe->uid] = true;
        }
    }
};

class RxQ
{
public:

    // data

    Agent *agent;
    ObjectRing<EagerBuf *> pending_rx_eager_bufs;
    ObjectRing<EagerBuf *> pending_ch_op_bufs;
    MemDescr *eager_mem_descr;
    uint64_t failed_attempts;

    // functions

    RxQ();
    void cleanup();
    void post_next_recv(EagerBuf *eager_buf);
    void new_eager_buf();
};

#endif // TX_QUEUE_HPP
