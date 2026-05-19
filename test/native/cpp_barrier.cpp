#include <iostream>
#include <assert.h>
#include <unordered_set>
#include <string>
#include <fstream>
#include <dragon/barrier.hpp>
#include <dragon/return_codes.h>
#include <dragon/exceptions.hpp>
#include <dragon/utils.h>

using namespace dragon;

static timespec_t TIMEOUT = {0,5000000000}; // Timeouts will be 5 second by default

dragonError_t test_attach_detach(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_attach_by_ch_descr(const char * barrier_descr) {
    dragonChannelSerial_t barrier_chan;
    dragonChannelDescr_t barrier_ch_descr;
    dragonError_t err;

    barrier_chan.data = dragon_base64_decode(barrier_descr, &barrier_chan.len);
    err = dragon_channel_attach(&barrier_chan, &barrier_ch_descr);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not attach to channel.");

    Barrier barrier(&barrier_ch_descr, nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_serialize(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    std::string barrier_descr_str = barrier_descr;
    const std::string barrier_ser = barrier.serialize();
    assert (barrier_descr_str.compare(barrier_ser) == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_get_parties(const char * barrier_descr, size_t num_parties) {
    Barrier barrier(barrier_descr, nullptr);
    size_t parties = barrier.parties();
    assert(parties == num_parties);
    return DRAGON_SUCCESS;
}

dragonError_t test_single_wait(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    barrier.wait(nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_wait_timeout(const char * barrier_descr) {
    try{
        Barrier barrier(barrier_descr, nullptr);
        barrier.wait(&TIMEOUT);
        return DRAGON_FAILURE;
    } catch (DragonError ex) {
        std::string rc_str = dragon_get_rc_string(ex.rc());
        if (rc_str.compare("DRAGON_TIMEOUT") == 0)
            return DRAGON_SUCCESS;
        else {
            cout<<"Failed to get expected timeout exception."<<endl;
            throw;
        }
    }
}

dragonError_t test_n_waiting(const char * barrier_descr, uint64_t nwaiting) {
    Barrier barrier(barrier_descr, nullptr);
    bool pass = false;
    for (size_t i=0 ; i<1000000 ; i++) {
        uint64_t n = barrier.n_waiting();
        if (n == nwaiting) {
            pass = true;
            break;
        }
    }
    assert(pass);
    barrier.wait(nullptr);
    return DRAGON_SUCCESS;
}

void write() {
    std::ofstream outputFile("cpp_barrier_test_action.txt", std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "ActionInvoked!";
        outputFile.close();
    } else {
        throw DragonError(DRAGON_FAILURE, "can't open the file.");
    }
}

dragonError_t test_wait_with_action(const char * barrier_descr) {
    Barrier barrier(barrier_descr, write);
    barrier.wait(nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_abort_reset_wait_proc(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    try {
        barrier.wait(nullptr);
        throw DragonError(DRAGON_FAILURE, "Failed to catch excepted broken barrier exception.");
    } catch (BrokenBarrierError ex) {
        return DRAGON_SUCCESS;
    }
}

dragonError_t test_abort(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    barrier.abort();
    return DRAGON_SUCCESS;
}

dragonError_t test_reset(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    barrier.reset();
    assert(barrier.n_waiting() == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_broken(const char * barrier_descr) {
    Barrier barrier(barrier_descr, nullptr);
    assert(!barrier.broken());
    barrier.abort();
    assert(barrier.broken());
    return DRAGON_SUCCESS;
}

dragonError_t test_abort_reset(const char * barrier_descr, uint64_t n) {
    Barrier barrier(barrier_descr, nullptr);
    assert(barrier.n_waiting() == n);
    assert(!barrier.broken());

    barrier.abort();
    assert(barrier.broken());
    uint64_t nw = barrier.n_waiting();
    assert(nw == 0);

    barrier.reset();
    assert(!barrier.broken());
    assert(barrier.n_waiting() == 0);
    return DRAGON_SUCCESS;
}

int main(int argc, char* argv[]) {
    char* barrier_descr = argv[1];
    std::string test = argv[2];
    dragonError_t err;

    try {
        if (test.compare("test_attach_detach") == 0) {
            err = test_attach_detach(barrier_descr);
        } else if (test.compare("test_attach_by_ch_descr") == 0) {
            err = test_attach_by_ch_descr(barrier_descr);
        } else if (test.compare("test_serialize") == 0) {
            err = test_serialize(barrier_descr);
        } else if (test.compare("test_get_parties") == 0) {
            char *tmpptr;
            size_t num_parties = strtoul(argv[3], &tmpptr, 10);
            err = test_get_parties(barrier_descr, num_parties);
        } else if (test.compare("test_single_wait") == 0) {
            err = test_single_wait(barrier_descr);
        } else if (test.compare("test_wait_timeout") == 0) {
            err = test_wait_timeout(barrier_descr);
        } else if (test.compare("test_n_waiting") == 0) {
            char *tmpptr;
            uint64_t nwaiting = strtoul(argv[3], &tmpptr, 10);
            err = test_n_waiting(barrier_descr, nwaiting);
        } else if (test.compare("test_wait_with_action") == 0) {
            err = test_wait_with_action(barrier_descr);
        } else if (test.compare("test_abort") == 0) {
            err = test_abort(barrier_descr);
        } else if (test.compare("test_abort_reset_wait_proc") == 0) {
            err = test_abort_reset_wait_proc(barrier_descr);
        } else if (test.compare("test_reset") == 0) {
            err = test_reset(barrier_descr);
        } else if (test.compare("test_broken") == 0) {
            err = test_broken(barrier_descr);
        } else if (test.compare("test_abort_reset") == 0) {
            char *tmpptr;
            uint64_t nwaiting = strtoul(argv[3], &tmpptr, 10);
            err = test_abort_reset(barrier_descr, nwaiting);
        } else {
            return DRAGON_NOT_IMPLEMENTED;
        }

    } catch(DragonError ex) {
        cout << "Caught Exception " << ex << endl;
        cout << ex.err_str() << endl;
        cout << dragon_get_rc_string(ex.rc()) << endl;
        return -1;
    } catch(...) {
        cout << "Caught Unknown Exception" << endl;
        return -2;
    }

    return err;
}