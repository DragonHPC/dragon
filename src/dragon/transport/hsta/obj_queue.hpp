#ifndef OBJ_QUEUE_HPP
#define OBJ_QUEUE_HPP

#include <deque>
#include <functional>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "dyn_ring_buf.hpp"
#include "utils.hpp"

#define OBJQ_DEFAULT_BLOCK_SIZE 32

template <class T, typename... Args>
class ObjQ
{
private:

    int block_size;
    std::vector<T *> mem_blocks;
    std::function<void(Args... args)> init;
    Lock lock = Lock("objq lock");
    const char *name;
    std::unordered_set<T *> free_objs;
    std::unordered_set<T *> alloc_objs;
    std::unordered_map<T *, std::string> free_backtraces;
    std::unordered_map<T *, std::string> alloc_backtraces;

public:

    DynRingBuf<T *> q;

    ObjQ()
    {
        this->block_size = OBJQ_DEFAULT_BLOCK_SIZE;
        this->name = typeid(T).name();
    }

    ~ObjQ()
    {
        for (auto *block : this->mem_blocks)
        {
            delete[] block;
        }
    }

    bool is_free(T *obj)
    {
        return  (this->free_objs.find(obj) != this->free_objs.end());
    }

    bool is_alloc(T *obj)
    {
        return  (this->alloc_objs.find(obj) != this->alloc_objs.end());
    }

    std::string get_free_backtrace(T *obj)
    {
        return this->free_backtraces[obj];
    }

    std::string get_alloc_backtrace(T *obj)
    {
        return this->alloc_backtraces[obj];
    }

    void push_back(T *obj)
    {
        if (dragon_hsta_debug) {
            if (this->is_free(obj)) {
                fprintf(
                    hsta_dbg_file,
                    "error: double free of object with typename=%s, allocated status=%s\n"
                    "%s" // current backtrace
                    "cached backtrace for duplicate free object:\n%s",
                    this->name, this->is_alloc(obj) ? "allocated" : "not allocated",
                    hsta_utils.get_backtrace().c_str(),
                    this->free_backtraces[obj].c_str()
                );
                hsta_utils.graceful_exit(SIGINT);
            }
            this->free_objs.insert(obj);
            this->free_backtraces[obj] = hsta_utils.get_backtrace();
            if (this->is_alloc(obj)) {
                this->alloc_objs.erase(obj);
            }
        }
        obj->fini();
        this->q.push_back(obj);
    }

    void push_back_ts(T *obj)
    {
        hsta_lock_acquire(this->lock);
        this->push_back(obj);
        hsta_lock_release(this->lock);
    }

    T *pull_front(Args... args)
    {
        if (this->q.empty())
        {
            auto *block = new T[this->block_size];
            // TODO: handle out-of-memory
            this->mem_blocks.push_back(block);

            for (auto i = 0; i < this->block_size; ++i)
            {
                if (dragon_hsta_debug) {
                    this->free_objs.insert(&block[i]);
                    this->free_backtraces[&block[i]] = hsta_utils.get_backtrace();
                }
                this->q.push_back(&block[i]);
            }
        }

        T *front_obj = this->q.front();
        this->q.pop_front();

        front_obj->init(args...);

        if (dragon_hsta_debug) {
            if (this->is_alloc(front_obj)) {
                fprintf(
                    hsta_dbg_file,
                    "error: double allocation of object with typename=%s, free status=%s\n"
                    "%s" // current backtrace
                    "cached backtrace for duplicate allocated object:\n%s",
                    this->name, this->is_free(front_obj) ? "free" : "not free",
                    hsta_utils.get_backtrace().c_str(),
                    this->alloc_backtraces[front_obj].c_str()
                );
                hsta_utils.graceful_exit(SIGINT);
            }
            this->alloc_objs.insert(front_obj);
            this->alloc_backtraces[front_obj] = hsta_utils.get_backtrace();
            if (this->is_free(front_obj)) {
                this->free_objs.erase(front_obj);
            }
        }

        return front_obj;
    }

    T *pull_front_ts(Args... args)
    {
        hsta_lock_acquire(this->lock);
        auto obj = this->pull_front(args...);
        hsta_lock_release(this->lock);
        return obj;
    }
};

#endif // OBJ_QUEUE_HPP
