#include <iostream>
#include <assert.h>
#include <unordered_set>
#include <string>
#include <dragon/dictionary.hpp>
#include <dragon/return_codes.h>

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
        throw DragonError(err, "Could not write bytes to ddict.");
}

void SerializableInt::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    uint8_t * received_val = nullptr;
    size_t num_val_expected = 1;

    err = dragon_ddict_read_bytes(req, sizeof(int), &actual_size, &received_val);
    if (err != DRAGON_SUCCESS)
        throw DragonError(err, "Could not read bytes from ddict.");

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

// serializable 2D double vector
class SerializableDoubleVector : public DDictSerializable {
    public:
    SerializableDoubleVector();
    SerializableDoubleVector(std::vector<std::vector<double>>& vec);
    virtual void serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    virtual void deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout);
    std::vector<std::vector<double>>& getVal() const;

    private:
    std::vector<std::vector<double>> v = {{}};
    std::vector<std::vector<double>>& val = v;
};

SerializableDoubleVector::SerializableDoubleVector(): v({{}}){}
SerializableDoubleVector::SerializableDoubleVector(std::vector<std::vector<double>>& x): val(x) {}

void SerializableDoubleVector::serialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err;
    for (auto& row: val) {
        for (auto& x: row) {
            uint8_t* ptr = reinterpret_cast<uint8_t*>(&x);
            err = dragon_ddict_write_bytes(req, sizeof(double), ptr);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not write bytes to ddict.");
        }
    }
}

void SerializableDoubleVector::deserialize(dragonDDictRequestDescr_t* req, const timespec_t* timeout) {
    dragonError_t err = DRAGON_SUCCESS;
    size_t actual_size;
    uint8_t * received_val = nullptr;
    int row = 2;
    int col = 3;
    std::vector<std::vector<double>> val_deserialize;

    for (int i=0 ; i<row ; i++) {
        std::vector<double> tmp_vec;
        for(int j=0 ; j<col ; j++) {
            err = dragon_ddict_read_bytes(req, sizeof(double), &actual_size, &received_val);
            if (err != DRAGON_SUCCESS)
                throw DragonError(err, "Could not read bytes from ddict.");

            if (actual_size != sizeof(double))
                throw DragonError(DRAGON_INVALID_ARGUMENT, "The size of double was not correct.");

            double element = *reinterpret_cast<double*>(received_val);
            tmp_vec.push_back(element);
            free(received_val);
        }
        val_deserialize.push_back(tmp_vec);
    }
    val = val_deserialize;

    err = dragon_ddict_read_bytes(req, sizeof(double), &actual_size, &received_val);

    if (err != DRAGON_EOT) {
        fprintf(stderr, "Did not received expected EOT, ec: %s\ntraceback: %s\n", dragon_get_rc_string(err), dragon_getlasterrstr());
        fflush(stderr);
    }
}

std::vector<std::vector<double>>& SerializableDoubleVector::getVal() const {return val;}

dragonError_t test_serialize(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    const std::string ser_str = dd.serialize();
    const std::string ddict_ser_str = ddict_ser;
    assert (ser_str.compare(ddict_ser_str) == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_attach_detach(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    // dd.detach()?
    return DRAGON_SUCCESS;
}

dragonError_t test_length(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    assert (dd.size() == 3);
    return DRAGON_SUCCESS;
}

dragonError_t test_clear(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.clear();
    assert (dd.size() == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_put_and_get(const char * ddict_ser) {
    SerializableInt x(6); // key
    SerializableInt y(42); // value
    // <type of key, type of value>
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd[x] = y;
    SerializableInt z = dd[x];
    assert (z.getVal() == 42);

    return DRAGON_SUCCESS;
}

dragonError_t test_pput(const char * ddict_ser) {
    SerializableInt x(6); // key
    SerializableInt y(42); // value
    // <type of key, type of value>
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.pput(x, y);
    SerializableInt z = dd[x];
    assert (z.getVal() == 42);

    return DRAGON_SUCCESS;
}

dragonError_t test_contains_existing_key(const char * ddict_ser) {
    SerializableInt x(6); // key
    SerializableInt y(42); // value
    // <type of key, type of value>
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd[x] = y;
    assert (dd.contains(x));

    return DRAGON_SUCCESS;
}

dragonError_t test_contains_non_existing_key(const char * ddict_ser) {
    SerializableInt x(6); // key
    // <type of key, type of value>
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    assert (!dd.contains(x));

    return DRAGON_SUCCESS;
}

dragonError_t test_erase_existing_key(const char * ddict_ser) {
    SerializableInt x(6); // key
    SerializableInt y(42); // value

    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd[x] = y;

    SerializableInt delted_val = dd.erase(x);
    assert (delted_val.getVal() == y.getVal());
    assert (!dd.contains(x));

    return DRAGON_SUCCESS;
}

dragonError_t test_erase_non_existing_key(const char * ddict_ser) {
    SerializableInt x(8); // key

    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);

    try {
        dd.erase(x); // delete a non-existing key, expect an exception here!
        return DRAGON_FAILURE;
    } catch (const DragonError& e) {
        std::string ec_str = dragon_get_rc_string(e.get_rc());
        assert(ec_str.compare("DRAGON_KEY_NOT_FOUND") == 0);
    }

    return DRAGON_SUCCESS;
}

dragonError_t test_keys(const char * ddict_ser) {
    SerializableInt x(6); // key
    SerializableInt y(42); // value

    SerializableInt x1(7); // key
    SerializableInt y1(43); // value

    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd[x] = y;
    dd[x1] = y1;

    auto dd_keys = dd.keys();

    assert(dd.size() == 2);

    bool got6 = false;
    bool got7 = false;

    for (int i=0; i<dd_keys.size() ; i++) {
        int val = dd_keys[i]->getVal();

        got6 = got6 || (val == 6);
        got7 = got7 || (val == 7);
    }

    assert(got6);
    assert(got7);

    return DRAGON_SUCCESS;
}

dragonError_t test_checkpoint(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.checkpoint();
    assert (dd.current_checkpoint_id() == 1);
    return DRAGON_SUCCESS;
}

dragonError_t test_rollback(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.rollback(); // should return chkpt 0 when rolling back with chkpt 0
    assert (dd.current_checkpoint_id() == 0);
    dd.checkpoint(); // chkpt 1
    assert (dd.current_checkpoint_id() == 1);
    dd.checkpoint(); // chkpt 2
    assert (dd.current_checkpoint_id() == 2);
    dd.rollback(); // chkpt 1
    assert (dd.current_checkpoint_id() == 1);
    return DRAGON_SUCCESS;
}

dragonError_t test_sync_to_newest_checkpoint(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.sync_to_newest_checkpoint();
    assert (dd.current_checkpoint_id() == 2);
    return DRAGON_SUCCESS;
}

dragonError_t test_current_checkpoint_id(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    assert (dd.current_checkpoint_id() == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_local_manager(const char * ddict_ser, const std::string local_manager) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    uint64_t id = dd.local_manager();
    std::string id_str = to_string(id);
    assert(id_str.compare(local_manager) == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_main_manager(const char * ddict_ser, const std::string main_manager) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    uint64_t id = dd.main_manager();
    // convert to string for string comparison
    std::string id_str = to_string(id);
    assert(id_str.compare(main_manager) == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_custom_manager(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    DDict<SerializableInt, SerializableInt> dd_m0 = dd.manager(0);
    DDict<SerializableInt, SerializableInt> dd_m1 = dd.manager(1);
    std::string dd_ser = dd.serialize();
    std::string dd_m0_ser = dd_m0.serialize();
    std::string dd_m1_ser = dd_m1.serialize();
    assert(dd_ser.compare(dd_m0_ser) == 0);
    assert(dd_ser.compare(dd_m1_ser) == 0);

    SerializableInt x(6); // key
    SerializableInt y(42); // value
    SerializableInt x1(7); // key
    SerializableInt y1(43); // value

    dd_m0[x] = y;
    dd_m1[x1] = y1;
    assert(dd.size() == 2);
    assert(dd_m0.size() == 1);
    assert(dd_m1.size() == 1);
    return DRAGON_SUCCESS;
}

dragonError_t test_empty_managers(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    std::vector<uint64_t> empty_managers = dd.empty_managers();
    for (size_t i=0 ; i<empty_managers.size() ; i++)
        cout<<"empty_managers["<<i<<"]: "<<empty_managers[i]<<endl;
    return DRAGON_SUCCESS;
}

dragonError_t test_local_managers(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    std::vector<uint64_t> local_managers = dd.local_managers();
    for (size_t i=0 ; i<local_managers.size() ; i++)
        cout<<"local_managers["<<i<<"]: "<<local_managers[i]<<endl;
    return DRAGON_SUCCESS;
}

dragonError_t test_local_keys(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    auto local_keys = dd.local_keys();
    SerializableInt key1(1);
    SerializableInt key0(0);
    bool found_key1 = false;
    bool found_key0 = false;
    for (auto key: local_keys) {
        found_key1 |= key1.getVal() == key->getVal();
        found_key0 |= key0.getVal() == key->getVal();
    }
    assert(found_key1 && found_key0);
    return DRAGON_SUCCESS;
}

dragonError_t test_synchronize(std::vector<std::string>& ser_ddicts) {
    DDict<SerializableInt, SerializableInt>::synchronize(ser_ddicts);
    return DRAGON_SUCCESS;
}

dragonError_t test_clone(const char * ddict_ser, std::vector<std::string>& ser_ddicts) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.clone(ser_ddicts);
    return DRAGON_SUCCESS;
}

dragonError_t test_write_np_arr(const char * ddict_ser) {
    DDict<SerializableInt, SerializableDoubleVector> dd(ddict_ser, &TIMEOUT);
    SerializableInt key(32);
    std::vector<std::vector<double>> vec = {{1.5, 2.5, 3.5}, {4.5, 5.5, 6.5}};
    SerializableDoubleVector ser_vec(vec);
    dd[key] = ser_vec;
    return DRAGON_SUCCESS;
}

dragonError_t test_read_np_arr(const char * ddict_ser) {
    DDict<SerializableInt, SerializableDoubleVector> dd(ddict_ser, &TIMEOUT);
    SerializableInt key_from_py(2048);

    // The dimension of the array is baked into the deserialize function of the class SerializableDoubleVector in this example.
    // While deserializing the data, user is expected to understand the dimension to reform the array.
    SerializableDoubleVector ser_vals_from_py = dd[key_from_py];
    auto vals_from_py = ser_vals_from_py.getVal();

    std::vector<std::vector<double>> expected_vals_from_py = {{0.12, 0.31, 3.4}, {4.579, 5.98, 6.54}};

    cout<<"Array written from python client: ";
    for (int i=0 ; i<2 ; i++) {
        cout<<"[";
        for (int j=0 ; j<3 ; j++) {
            cout<<vals_from_py[i][j]<<", ";
            assert (vals_from_py[i][j] == expected_vals_from_py[i][j]);
        }
        cout<<"], ";
    }
    std::cout << std::endl;

    return DRAGON_SUCCESS;
}

dragonError_t test_keys_read_from_py(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);

    // write multiple integer keys from C++ client, will read from python client later
    SerializableInt key1(1024);
    SerializableInt key2(9876);
    SerializableInt key3(2048);
    SerializableInt val(0);
    dd[key1] = val;
    dd[key2] = val;
    dd[key3] = val;
    return DRAGON_SUCCESS;
}

int main(int argc, char* argv[]) {
    char* ddict_descr = argv[1];
    std::string test = argv[2];
    dragonError_t err;

    try {
        if (test.compare("test_serialize") == 0) {
            err = test_serialize(ddict_descr);
        } else if (test.compare("test_attach_detach") == 0) {
            err = test_attach_detach(ddict_descr);
        } else if (test.compare("test_length") == 0) {
            err = test_length(ddict_descr);
        } else if (test.compare("test_clear") == 0) {
            err = test_clear(ddict_descr);
        } else if (test.compare("test_put_and_get") == 0) {
            err = test_put_and_get(ddict_descr);
        } else if (test.compare("test_pput") == 0) {
            err = test_pput(ddict_descr);
        } else if (test.compare("test_contains_existing_key") == 0) {
            err = test_contains_existing_key(ddict_descr);
        } else if (test.compare("test_contains_non_existing_key") == 0) {
            err = test_contains_non_existing_key(ddict_descr);
        } else if (test.compare("test_erase_existing_key") == 0) {
            err = test_erase_existing_key(ddict_descr);
        } else if (test.compare("test_erase_non_existing_key") == 0) {
            err = test_erase_non_existing_key(ddict_descr);
        } else if (test.compare("test_keys") == 0) {
            err = test_keys(ddict_descr);
        } else if (test.compare("test_checkpoint") == 0) {
            err = test_checkpoint(ddict_descr);
        } else if (test.compare("test_rollback") == 0) {
            err = test_rollback(ddict_descr);
        } else if (test.compare("test_sync_to_newest_checkpoint") == 0) {
            err = test_sync_to_newest_checkpoint(ddict_descr);
        } else if (test.compare("test_current_checkpoint_id") == 0) {
            err = test_current_checkpoint_id(ddict_descr);
        } else if (test.compare("test_local_manager") == 0) {
            std::string local_manager = argv[3];
            err = test_local_manager(ddict_descr, local_manager);
        } else if (test.compare("test_main_manager") == 0) {
            std::string main_manager = argv[3];
            err = test_main_manager(ddict_descr, main_manager);
        } else if (test.compare("test_custom_manager") == 0) {
            err = test_custom_manager(ddict_descr);
        } else if (test.compare("test_empty_managers") == 0) {
            err = test_empty_managers(ddict_descr);
        } else if (test.compare("test_local_managers") == 0) {
            err = test_local_managers(ddict_descr);
        } else if (test.compare("test_local_keys") == 0) {
            err = test_local_keys(ddict_descr);
        } else if (test.compare("test_synchronize") == 0) {
            char *tmpptr;
            size_t num_serialized_ddicts = strtoul(argv[3], &tmpptr, 10);
            std::vector<std::string> ser_ddicts;
            for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
                std::string ser_ddict = argv[4+i];
                ser_ddicts.push_back(ser_ddict);
            }
            err = test_synchronize(ser_ddicts);
        } else if (test.compare("test_clone") == 0) {
            char *tmpptr;
            size_t num_serialized_ddicts = strtoul(argv[3], &tmpptr, 10);
            std::vector<std::string> ser_ddicts;
            for (size_t i=0 ; i<num_serialized_ddicts ; i++) {
                std::string ser_ddict = argv[4+i];
                ser_ddicts.push_back(ser_ddict);
            }
            err = test_clone(ddict_descr, ser_ddicts);
        } else if (test.compare("test_write_np_arr") == 0){
            err = test_write_np_arr(ddict_descr);
        } else if (test.compare("test_read_np_arr") == 0){
            err = test_read_np_arr(ddict_descr);
        } else if (test.compare("test_keys_read_from_py") == 0){
            err = test_keys_read_from_py(ddict_descr);
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