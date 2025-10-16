#include <iostream>
#include <assert.h>
#include <unordered_set>
#include <string>
#include <dragon/semaphore.hpp>
#include <dragon/return_codes.h>
#include <dragon/exceptions.hpp>
#include <dragon/utils.h>

using namespace dragon;

dragonError_t test_attach_detach(const char * sem_descr) {
    Semaphore sem(sem_descr);
    return DRAGON_SUCCESS;
}

dragonError_t test_attach_by_ch_descr(const char * sem_descr) {
    dragonChannelSerial_t ser_chan;
    dragonChannelDescr_t sem_ch_descr;
    dragonError_t err;

    ser_chan.data = dragon_base64_decode(sem_descr, &ser_chan.len);
    err = dragon_channel_attach(&ser_chan, &sem_ch_descr);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not attach to channel.");

    Semaphore sem(&sem_ch_descr);

    return DRAGON_SUCCESS;
}

dragonError_t test_serialize(const char * sem_descr) {
    Semaphore sem(sem_descr);
    std::string sem_descr_str = sem_descr;
    const std::string ser = sem.serialize();
    assert (sem_descr_str.compare(sem_descr) == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_acquire(const char * sem_descr) {
    Semaphore sem(sem_descr);
    bool ret = sem.acquire();
    assert(ret);
    return DRAGON_SUCCESS;
}

dragonError_t test_release(const char * sem_descr) {
    Semaphore sem(sem_descr);
    sem.release(1, nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_get_value(const char * sem_descr, uint64_t init_val) {
    Semaphore sem(sem_descr);
    dragonULInt val = sem.get_value();
    assert(val == init_val);

    for (size_t i=init_val ; i>0 ; i--) {
        bool ret = sem.acquire();
        assert(ret);
        val = sem.get_value();
        assert(val == i-1);
    }

    for (size_t i=0 ; i<init_val ; i++) {
        sem.release(1, nullptr);
        val = sem.get_value();
        assert(val == i+1);
    }

    return DRAGON_SUCCESS;
}

dragonError_t test_release_group(const char * sem_descr) {
    Semaphore sem(sem_descr);
    dragonULInt val = sem.get_value();
    assert(val == 0);
    sem.release(3, nullptr);
    val = sem.get_value();
    assert(val == 3);
    sem.release(4, nullptr);
    val = sem.get_value();
    assert(val == 7);

    return DRAGON_SUCCESS;
}

dragonError_t test_bounded(const char * sem_descr) {
    Semaphore sem(sem_descr);
    try{
        sem.release();
        return DRAGON_FAILURE;
    } catch(DragonError ex) {
        std::string err_str = dragon_get_rc_string(ex.rc());
        if (err_str.compare("DRAGON_INVALID_VALUE") == 0)
            return DRAGON_SUCCESS;
        else
            throw DragonError(DRAGON_FAILURE, "Failed to caught expected DragonError.");
    }
}

dragonError_t test_invalid_acquire(const char * sem_descr) {
    Semaphore sem(sem_descr);
    bool ret = sem.acquire(false, nullptr);
    assert(!ret);
    return DRAGON_SUCCESS;
}

int main(int argc, char* argv[]) {
    char* sem_descr = argv[1];
    std::string test = argv[2];
    dragonError_t err;

    try {
        if (test.compare("test_attach_detach") == 0) {
            err = test_attach_detach(sem_descr);
        } else if (test.compare("test_attach_by_ch_descr") == 0) {
            err = test_attach_by_ch_descr(sem_descr);
        } else if (test.compare("test_serialize") == 0) {
            err = test_serialize(sem_descr);
        } else if (test.compare("test_acquire") == 0) {
            err = test_acquire(sem_descr);
        } else if (test.compare("test_release") == 0) {
            err = test_release(sem_descr);
        } else if (test.compare("test_get_value") == 0) {
            char *tmpptr;
            uint64_t init_val = strtoul(argv[3], &tmpptr, 10);
            err = test_get_value(sem_descr, init_val);
        } else if (test.compare("test_release_group") == 0) {
            err = test_release_group(sem_descr);
        } else if (test.compare("test_bounded") == 0) {
            err = test_bounded(sem_descr);
        } else if (test.compare("test_invalid_acquire") == 0) {
            err = test_invalid_acquire(sem_descr);
        } else {
            return DRAGON_NOT_IMPLEMENTED;
        }

    } catch(DragonError ex) {
        cout << "Caught Exception " << ex << endl;
        return -1;
    } catch(...) {
        cout << "Caught Unknown Exception" << endl;
        return -2;
    }

    return err;
}