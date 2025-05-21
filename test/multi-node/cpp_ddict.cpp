#include <iostream>
#include <assert.h>
#include <set>
#include <string>
#include <dragon/dictionary.hpp>
#include <dragon/return_codes.h>
#include "../_ctest_utils.h"

static timespec_t TIMEOUT = {0,500000000}; // Timeouts will be 0.5 second by default

class SerializableInt : public DDictSerializable {
    public:
    SerializableInt();
    SerializableInt(int x);
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    static SerializableInt* create(size_t num_bytes, uint8_t* data);
    int getVal() const;
    private:
    int val=0;
};

SerializableInt::SerializableInt(): val(0) {}
SerializableInt::SerializableInt(int x): val(x) {}

SerializableInt* SerializableInt::create(size_t num_bytes, uint8_t* data) {
    auto val = new SerializableInt((int)*data);
    free(data);
    return val;
}

void SerializableInt::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err;
    err = dragon_ddict_write_bytes(req, sizeof(int), (uint8_t*)&val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());
}

void SerializableInt::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    uint8_t * received_val = nullptr;
    size_t num_val_expected = 1;


    err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, dragon_getlasterrstr());

    if (actual_size != sizeof(int))
        throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of the integer was not correct.");

    val = (int)*received_val;

    free(received_val);

    err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);

    if (err != DRAGON_EOT) {
        fprintf(stderr, "Did not received expected EOT, ec: %s\ntraceback: %s\n", dragon_get_rc_string(err), dragon_getlasterrstr());
        fflush(stderr);
    }
}

int SerializableInt::getVal() const {return val;}

dragonError_t test_local_keys(const char * ddict_ser, const size_t num_managers) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    auto local_managers = dd.local_managers();
    assert (local_managers.size() == 2); // 2 managers per node
    std::vector<uint64_t> non_local_managers;
    std::set<uint64_t> local_managers_set(local_managers.begin(), local_managers.end());
    for (uint64_t i=0 ; i<(uint64_t)num_managers ; i++) {
        if (local_managers_set.find(i) == local_managers_set.end())
            non_local_managers.push_back(i);
    }
    assert (non_local_managers.size() >= 2); // 2 managers per node

    SerializableInt y(100); // value
    // write local key
    DDict<SerializableInt, SerializableInt> local_d0 = dd.manager(local_managers[0]);
    DDict<SerializableInt, SerializableInt> local_d1 = dd.manager(local_managers[1]);

    SerializableInt x0(0); // local key
    SerializableInt x1(1); // local key

    local_d0[x0] = y;
    local_d1[x1] = y;

    // write non local key
    DDict<SerializableInt, SerializableInt> non_local_d0 = dd.manager(non_local_managers[0]);
    DDict<SerializableInt, SerializableInt> non_local_d1 = dd.manager(non_local_managers[1]);

    SerializableInt x_non_local_0(-1); // non local key
    SerializableInt x_non_local_1(-2); // non local key

    non_local_d0[x_non_local_0] = y;
    non_local_d1[x_non_local_1] = y;

    // get local keys and verify them
    auto local_keys = dd.local_keys();
    bool found_key1 = false;
    bool found_key0 = false;
    for (auto key: local_keys) {
        if (key->getVal() != x0.getVal() && key->getVal() != x1.getVal()) {
            err_fail(DRAGON_FAILURE, "Received unexpected local key.");
        }
        found_key1 |= x1.getVal() == key->getVal();
        found_key0 |= x0.getVal() == key->getVal();
    }
    assert(found_key1 && found_key0);
    return DRAGON_SUCCESS;
}

int main(int argc, char* argv[]) {
    char* ddict_descr = argv[1];
    std::string test = argv[2];
    dragonError_t err;

    if (test.compare("test_local_keys") == 0) {
        char *tmpptr;
        size_t num_managers = strtoul(argv[3], &tmpptr, 10);
        err = test_local_keys(ddict_descr, num_managers);
    } else {
        return DRAGON_NOT_IMPLEMENTED;
    }

    return err;
}