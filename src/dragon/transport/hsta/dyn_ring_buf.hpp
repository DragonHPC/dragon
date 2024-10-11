#ifndef DYN_RING_BUF_HPP
#define DYN_RING_BUF_HPP

#include "globals.hpp"
#include "magic_numbers.hpp"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <unistd.h>
#include <vector>

#define DYN_RING_BUF_INITIAL_ELEMENTS 1024

template <typename T>
class DynRingBuf
{
private:

    std::vector<T> ring_buf;
    uint64_t head;
    uint64_t tail;
    uint64_t mask;

    size_t get_head()
    {
        return (this->head & this->mask);
    }

    size_t inc_head()
    {
        return (this->head++ & this->mask);
    }

    size_t get_tail()
    {
        return (this->tail & this->mask);
    }

    size_t inc_tail()
    {
        return (this->tail++ & this->mask);
    }

    size_t get_size()
    {
        return this->tail - this->head;
    }

    size_t get_last()
    {
        if (this->get_size() == 0ul) {
            // the last valid index is undefined in this case
            return HSTA_INVALID_UINT64;
        } else {
            return ((this->tail - 1ul) & this->mask);
        }
    }

public:

    DynRingBuf()
    {
        this->ring_buf.resize(DYN_RING_BUF_INITIAL_ELEMENTS);

        this->head = 0ul;
        this->tail = 0ul;
        this->mask = this->ring_buf.size() - 1;
    }

    T& operator[](const size_t offset_from_head)
    {
#ifndef HSTA_NDEBUG
        assert(this->head + offset_from_head < this->tail);
#endif // !HSTA_NDEBUG

        auto index = (this->head + offset_from_head) & this->mask;
        return this->ring_buf[index];
    }

    size_t size()
    {
        return this->get_size();
    }

    bool empty()
    {
        return (this->size() == 0ul);
    }

    T front()
    {
        return this->ring_buf[this->get_head()];
    }

    T back()
    {
        return this->ring_buf[this->get_last()];
    }

    // TODO: implement this and peek_front like we do in ObjectRing,
    // then we can replace ObjectRing with DynRingBuf
    T pull_front()
    {
        return this->ring_buf[this->inc_head()];
    }

    void pop_front()
    {
        ++this->head;
    }

    void push_back(T item)
    {
        this->ring_buf[this->inc_tail()] = item;

        auto nitems            = this->size();
        auto cur_ring_buf_size = this->ring_buf.size();

        if (nitems == cur_ring_buf_size)
        {
            // resize, update mask, move elements

            auto cur_head          = this->get_head();
            auto cur_mask          = this->mask;
            auto new_ring_buf_size = 2ul * cur_ring_buf_size;

            this->ring_buf.resize(new_ring_buf_size);
            this->mask = new_ring_buf_size - 1;

            if (cur_head != 0ul)
            {
                std::vector<T> tmp(cur_ring_buf_size);

                for (auto i = 0ul; i < nitems; ++i)
                {
                    tmp[i] = this->ring_buf[(cur_head + i) & cur_mask];
                }

                for (auto i = 0ul; i < nitems; ++i)
                {
                    this->ring_buf[i] = tmp[i];
                }
            }

            this->head = 0ul;
            this->tail = nitems;
        }
    }
};

#endif // DYN_RING_BUF_HPP
