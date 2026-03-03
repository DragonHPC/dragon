#include <iostream>
#include <assert.h>
#include <unordered_set>
#include <string>
#include <dragon/dictionary.hpp>
#include <dragon/return_codes.h>

using namespace dragon;

static timespec_t TIMEOUT = {0,5000000000}; // Timeouts will be 5 second by default

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
    uint64_t manager_id;
    SerializableInt x(6); // key
    SerializableInt y(42); // value
    // <type of key, type of value>
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    manager_id = dd.which_manager(x); // call this to test it.
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
        std::string ec_str = dragon_get_rc_string(e.rc());
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
        int val = dd_keys[i].getVal();

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
    assert (dd.checkpoint_id() == 1);
    return DRAGON_SUCCESS;
}

dragonError_t test_rollback(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.rollback(); // should return chkpt 0 when rolling back with chkpt 0
    assert (dd.checkpoint_id() == 0);
    dd.checkpoint(); // chkpt 1
    assert (dd.checkpoint_id() == 1);
    dd.checkpoint(); // chkpt 2
    assert (dd.checkpoint_id() == 2);
    dd.rollback(); // chkpt 1
    assert (dd.checkpoint_id() == 1);
    return DRAGON_SUCCESS;
}

dragonError_t test_sync_to_newest_checkpoint(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    dd.sync_to_newest_checkpoint();
    assert (dd.checkpoint_id() == 2);
    return DRAGON_SUCCESS;
}

dragonError_t test_checkpoint_id(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    assert (dd.checkpoint_id() == 0);
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
        found_key1 |= key1.getVal() == key.getVal();
        found_key0 |= key0.getVal() == key.getVal();
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
    DDict<SerializableInt, SerializableDouble2DVector> dd(ddict_ser, &TIMEOUT);
    SerializableInt key(32);
    std::vector<std::vector<double>> vec = {{1.5, 2.5, 3.5}, {4.5, 5.5, 6.5}};
    SerializableDouble2DVector ser_vec(vec);
    dd[key] = ser_vec;
    return DRAGON_SUCCESS;
}

dragonError_t test_read_np_arr(const char * ddict_ser) {
    DDict<SerializableInt, SerializableDouble2DVector> dd(ddict_ser, &TIMEOUT);
    SerializableInt key_from_py(2048);

    // The dimension of the array is baked into the deserialize function of the class SerializableDouble2DVector in this example.
    // While deserializing the data, user is expected to understand the dimension to reform the array.
    SerializableDouble2DVector ser_vals_from_py = dd[key_from_py];
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

dragonError_t test_freeze(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);
    assert(!dd.is_frozen());
    dd.freeze();
    assert(dd.is_frozen());
    dd.unfreeze();
    assert(!dd.is_frozen());
    return DRAGON_SUCCESS;
}

dragonError_t test_batch_put(const char * ddict_ser) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);

    SerializableInt key1(1024);
    SerializableInt key2(9876);
    SerializableInt key3(2048);
    SerializableInt val(0);

    dd.start_batch_put(false);
    dd[key1] = val;
    dd[key2] = val;
    dd[key3] = val;
    dd.end_batch_put();

    SerializableInt received_val = dd[key1];
    assert(received_val.getVal() == val.getVal());
    received_val = dd[key2];
    assert(received_val.getVal() == val.getVal());
    received_val = dd[key3];
    assert(received_val.getVal() == val.getVal());

    return DRAGON_SUCCESS;
}

dragonError_t test_bput_bget(const char * ddict_ser, uint64_t num_managers) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);

    SerializableInt key1(1024);
    SerializableInt key2(9876);
    SerializableInt key3(2048);
    SerializableInt val(1);

    dd.bput(key1, val);
    dd.bput(key2, val);
    dd.bput(key3, val);

    for (uint64_t i=0 ; i<num_managers ; i++) {
        DDict<SerializableInt, SerializableInt> dselect = dd.manager(i);
        SerializableInt received_val = dselect.bget(key1);
        assert(received_val.getVal() == val.getVal());
        received_val = dselect.bget(key2);
        assert(received_val.getVal() == val.getVal());
        received_val = dselect.bget(key3);
        assert(received_val.getVal() == val.getVal());
    }

    return DRAGON_SUCCESS;
}

dragonError_t test_bput_batch(const char * ddict_ser, uint64_t num_managers) {
    DDict<SerializableInt, SerializableInt> dd(ddict_ser, &TIMEOUT);

    SerializableInt key1(1024);
    SerializableInt key2(9876);
    SerializableInt key3(2048);
    SerializableInt val(0);

    dd.start_batch_put(false);
    dd.bput(key1, val);
    dd.bput(key2, val);
    dd.bput(key3, val);
    dd.end_batch_put();

    for (uint64_t i=0 ; i<num_managers ; i++) {
        DDict<SerializableInt, SerializableInt> dselect = dd.manager(i);
        SerializableInt received_val = dselect[key1];
        assert(received_val.getVal() == val.getVal());
        received_val = dselect[key2];
        assert(received_val.getVal() == val.getVal());
        received_val = dselect[key3];
        assert(received_val.getVal() == val.getVal());
    }
    return DRAGON_SUCCESS;
}

dragonError_t test_bput_multiple_batch(const char * ddict_ser, uint64_t num_managers) {
    uint64_t num_batches = 10;
    uint64_t num_keys = 5;
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);


    for (uint64_t i_batch=0 ; i_batch<num_batches ; i_batch++) {
        dd.start_batch_put(false);
        for (size_t i=0 ; i<num_keys ; i++) {
            std::string s = "dragon_" + std::to_string(i_batch) + "_" + std::to_string(i);
            SerializableString key(s);
            SerializableInt val(i_batch*10+i);
            dd.bput(key, val);
        }
        dd.end_batch_put();
    }

    for (uint64_t i=0 ; i<num_managers ; i++) {
        DDict<SerializableString, SerializableInt> dselect = dd.manager(i);
        for(uint64_t j_batch=0 ; j_batch<num_batches ; j_batch++) {
            for (uint64_t j=0 ; j<num_keys ; j++) {
                std::string s = "dragon_" + std::to_string(j_batch) + "_" + std::to_string(j);
                SerializableString key(s);
                SerializableInt val(j_batch*10+j);
                SerializableInt received_val = dselect.bget(key);
                assert(received_val.getVal() == val.getVal());
            }
        }
    }

    return DRAGON_SUCCESS;
}

dragonError_t test_write_chkpts_to_disk(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    SerializableString key("dragon");
    for (int i=0 ; i<5 ; i++) {
        SerializableInt val(i);
        dd[key] = val;
        dd.checkpoint();
    }
    return DRAGON_SUCCESS;
}

dragonError_t test_advance(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    SerializableString key("dragon");
    for (uint64_t i=0 ; i<3 ; i+=2) {
        uint64_t chkptID = dd.checkpoint_id();
        assert(chkptID == i);
        SerializableInt val = dd[key];
        assert(val.getVal() == i);
        if (i != 2)
            dd.advance();
    }
    return DRAGON_SUCCESS;
}

dragonError_t test_persist(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    SerializableString key("dragon");
    for (uint64_t i=0 ; i<5 ; i++) {
        SerializableInt val(i);
        dd[key] = val;
        if (i % 2 ==0) { // persist chkpt 0, 2, 4
            dd.persist();
        }
        dd.checkpoint();
    }
    return DRAGON_SUCCESS;
}

dragonError_t test_restore(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    SerializableString key("dragon");
    for (uint64_t i=4 ; i>0 ; i-=2) {
        dd.restore(i);
        assert(dd.checkpoint_id() == i);
        SerializableInt val = dd[key];
        assert(val.getVal() == i);
    }
    return DRAGON_SUCCESS;
}

dragonError_t test_persisted_ids_0_2_4(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    std::vector<uint64_t> ids = dd.persisted_ids();
    assert(ids.size() == 3);
    assert(ids[0] == 0);
    assert(ids[1] == 2);
    assert(ids[2] == 4);
    return DRAGON_SUCCESS;
}

dragonError_t test_no_persisted_ids(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    std::vector<uint64_t> ids = dd.persisted_ids();
    assert(ids.size() == 0);
    return DRAGON_SUCCESS;
}

// // add this to the multi-node c, cpp test as well
dragonError_t test_local_size(const char * ddict_ser) {
    DDict<SerializableString, SerializableInt> dd(ddict_ser, &TIMEOUT);
    assert(dd.local_size() == 2);
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
        } else if (test.compare("test_checkpoint_id") == 0) {
            err = test_checkpoint_id(ddict_descr);
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
        } else if (test.compare("test_freeze") == 0){
            err = test_freeze(ddict_descr);
        } else if (test.compare("test_batch_put") == 0){
            err = test_batch_put(ddict_descr);
        } else if (test.compare("test_bput_bget") == 0){
            char *tmpptr;
            uint64_t num_managers = strtoul(argv[3], &tmpptr, 10);
            err = test_bput_bget(ddict_descr, num_managers);
        } else if (test.compare("test_bput_batch") == 0){
            char *tmpptr;
            uint64_t num_managers = strtoul(argv[3], &tmpptr, 10);
            err = test_bput_batch(ddict_descr, num_managers);
        } else if (test.compare("test_bput_multiple_batch") == 0){
            char *tmpptr;
            uint64_t num_managers = strtoul(argv[3], &tmpptr, 10);
            err = test_bput_multiple_batch(ddict_descr, num_managers);
        } else if (test.compare("test_write_chkpts_to_disk") == 0){
            err = test_write_chkpts_to_disk(ddict_descr);
        } else if (test.compare("test_advance") == 0){
            err = test_advance(ddict_descr);
        } else if (test.compare("test_persist") == 0){
            err = test_persist(ddict_descr);
        } else if (test.compare("test_restore") == 0){
            err = test_restore(ddict_descr);
        } else if (test.compare("test_persisted_ids_0_2_4") == 0){
            err = test_persisted_ids_0_2_4(ddict_descr);
        } else if (test.compare("test_no_persisted_ids") == 0){
            err = test_no_persisted_ids(ddict_descr);
        } else if (test.compare("test_local_size") == 0){
            err = test_local_size(ddict_descr);
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