#ifndef OBJ_RING_HPP
#define OBJ_RING_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <signal.h>
#include <unordered_map>
#include <unordered_set>
#include "bit_vector.hpp"
#include "build_constants.hpp"
#include "dyn_ring_buf.hpp"
#include "globals.hpp"
#include "magic_numbers.hpp"
#include "utils.hpp"

// class definitions

// this should only be used with pointers to objects
// TODO: change T to T* to enforce above comment
template <typename T>
class ObjectRing
{
private:

    // data

    DynRingBuf<T> d;
    BitVector bv;
    Lock lock = Lock("obj ring lock");

public:

    // functions

    T& operator[](const size_t index)
    {
        return this->d[index];
    }

    T get_back()
    {
        if (!this->d.empty()) {
            T thing = this->d.back();
#ifndef HSTA_NDEBUG
            if (dragon_hsta_debug) {
                hsta_utils.inc_access_count(thing);
            }
#endif // !HSTA_NDEBUG
            return thing;
        } else {
            return nullptr;
        }
    }

    T peek_front()
    {
        if (!this->d.empty())
        {
            T thing = this->d.front();
#ifndef HSTA_NDEBUG
            if (dragon_hsta_debug)
            {
                hsta_utils.inc_access_count(thing);
            }
#endif // !HSTA_NDEBUG
            return thing;
        }
        else
        {
            return nullptr;
        }
    }

    T peek_front_unique()
    {
        // we don't need to check the bit vector here, since
        // peeking doesn't affect uniqueness
        return this->peek_front();
    }

    T peek_front_unique_idx()
    {
        if (!this->d.empty())
        {
            return this->d.front();
        }
        else
        {
            return (T) HSTA_INVALID_INDEX;
        }
    }

    T peek_front_ts(bool need_thread_safe)
    {
        if (need_thread_safe) {
            hsta_lock_acquire(this->lock);
        }

        if (!this->d.empty()) {
            T thing = this->d.front();
#ifndef HSTA_NDEBUG
            if (dragon_hsta_debug) {
                hsta_utils.inc_access_count(thing);
            }
#endif // !HSTA_NDEBUG
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
            return thing;
        } else {
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
            return nullptr;
        }
    }

    void pop_front()
    {
        if (!this->d.empty())
        {
            this->d.pop_front();
        }
#ifndef HSTA_NDEBUG
        else if (dragon_hsta_debug) {
            hsta_utils.log("POP_FRONT FAILED\n\n");
            fflush(hsta_dbg_file);
            hsta_utils.graceful_exit(SIGINT);
        }
#endif // !HSTA_NDEBUG
    }

    void pop_front_unique()
    {
        if (!this->d.empty())
        {
            T thing = this->d.front();
            this->d.pop_front();
            bv.set(thing->uid, 0ul);
        }
#ifndef HSTA_NDEBUG
        else if (dragon_hsta_debug) {
            hsta_utils.log("POP_FRONT_UNIQUE FAILED\n\n");
            hsta_utils.graceful_exit(SIGINT);
        }
#endif // !HSTA_NDEBUG
    }

    void pop_front_unique_idx()
    {
        if (!this->d.empty())
        {
            T idx = this->d.front();
            this->d.pop_front();
            bv.set(idx, 0ul);
        }
#ifndef HSTA_NDEBUG
        else if (dragon_hsta_debug) {
            hsta_utils.log("POP_FRONT_UNIQUE_IDX FAILED\n\n");
            hsta_utils.graceful_exit(SIGINT);
        }
#endif // !HSTA_NDEBUG
    }

    void pop_front_ts(bool need_thread_safe)
    {
        if (need_thread_safe) {
            hsta_lock_acquire(this->lock);
        }

        if (!this->d.empty()) {
            this->d.pop_front();
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
        }
#ifndef HSTA_NDEBUG
        else if (dragon_hsta_debug) {
            hsta_utils.log("POP_FRONT FAILED\n\n");
            fflush(hsta_dbg_file);
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
            hsta_utils.graceful_exit(SIGINT);
        }
#endif // !HSTA_NDEBUG
        else {
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
        }
    }

    T pull_front()
    {
        if (!this->d.empty())
        {
            T thing = this->d.front();
#ifndef HSTA_NDEBUG
            if (dragon_hsta_debug)
            {
                hsta_utils.inc_access_count(thing);
            }
#endif // !HSTA_NDEBUG
            this->d.pop_front();
            return thing;
        }
        else
        {
            return nullptr;
        }
    }

    T pull_front_unique()
    {
        if (!this->d.empty())
        {
            T thing = this->d.front();
#ifndef HSTA_NDEBUG
            if (dragon_hsta_debug)
            {
                hsta_utils.inc_access_count(thing);
            }
#endif // !HSTA_NDEBUG
            this->d.pop_front();
            bv.set(thing->uid, 0ul);
            return thing;
        }
        else
        {
            return nullptr;
        }
    }

    T pull_front_unique_idx()
    {
        if (!this->d.empty())
        {
            T idx = this->d.front();
            this->d.pop_front();
            bv.set(idx, 0ul);
            return idx;
        }
        else
        {
            return (T) HSTA_INVALID_INDEX;
        }
    }

    T pull_front_ts(bool need_thread_safe)
    {
        if (need_thread_safe) {
            hsta_lock_acquire(this->lock);
        }

        if (!this->d.empty()) {
            T thing = this->d.front();
#ifndef HSTA_NDEBUG
            if (dragon_hsta_debug) {
                hsta_utils.inc_access_count(thing);
            }
#endif // !HSTA_NDEBUG
            this->d.pop_front();
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
            return thing;
        } else {
            if (need_thread_safe) {
                hsta_lock_release(this->lock);
            }
            return nullptr;
        }
    }

    void push_back(T thing)
    {
        this->d.push_back(thing);
    }

    void push_back_unique(T thing)
    {
        if (bv.size() <= thing->uid)
        {
            bv.resize(2ul * (1ul + thing->uid));
        }

        if (bv.get(thing->uid) == 0ul)
        {
            this->d.push_back(thing);
            bv.set(thing->uid, 1ul);
        }
    }

    void push_back_unique_idx(T idx)
    {
        if (bv.size() <= (size_t) idx)
        {
            bv.resize(2ul * (1ul + (size_t) idx));
        }

        if (bv.get(idx) == 0ul)
        {
            this->d.push_back(idx);
            bv.set(idx, 1ul);
        }
    }

    void push_back_ts(T thing, bool need_thread_safe)
    {
        if (need_thread_safe) {
            hsta_lock_acquire(this->lock);
        }
        this->d.push_back(thing);
        if (need_thread_safe) {
            hsta_lock_release(this->lock);
        }
    }

    bool empty()
    {
        return this->d.empty();
    }

    bool empty_ts(bool need_thread_safe)
    {
        if (need_thread_safe) {
            hsta_lock_acquire(this->lock);
        }
        auto is_empty = this->d.empty();
        if (need_thread_safe) {
            hsta_lock_release(this->lock);
        }
        return is_empty;
    }

    size_t size()
    {
        return this->d.size();
    }

    size_t size_ts(bool need_thread_safe)
    {
        if (need_thread_safe) {
            hsta_lock_acquire(this->lock);
        }
        auto size = this->d.size();
        if (need_thread_safe) {
            hsta_lock_release(this->lock);
        }
        return size;
    }
};

#endif // OBJ_RING_HPP
