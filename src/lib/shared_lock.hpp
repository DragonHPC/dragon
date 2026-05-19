#ifndef HAVE_DRAGON_LOCK_HPP
#define HAVE_DRAGON_LOCK_HPP

#include "stdlib.h"
#include "shared_lock.h"

#include <stdexcept>

class dragonLock
{
private:

    dragonLock_t dlock;
    void *mem;

public:

    dragonLock()
    {
        auto lock_size = dragon_lock_size(DRAGON_LOCK_FIFO_LITE);
        this->mem = calloc(lock_size, 1ul);
        if (this->mem == nullptr) {
            throw std::runtime_error("failed to allocate memory for lock");
        }

        auto dragon_rc = dragon_lock_init(&this->dlock, mem, DRAGON_LOCK_FIFO_LITE);
        if (dragon_rc != DRAGON_SUCCESS) {
            throw std::runtime_error("failed to initialize lock");
        }
    }

    ~dragonLock()
    {
        dragon_lock_destroy(&this->dlock);
        free(this->mem);
    }

    dragonError_t
    acquire()
    {
        return dragon_lock(&this->dlock);
    }
    
    dragonError_t
    release()
    {
        return dragon_unlock(&this->dlock);
    }
};

#endif
