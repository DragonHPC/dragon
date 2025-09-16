#include <iostream>
#include <assert.h>
#include <string>

#include <dragon/queue.hpp>
#include <dragon/return_codes.h>
#include "../_ctest_utils.h"

using namespace dragon;

static timespec_t TIMEOUT = {0,500000000}; // Timeouts will be 0.5 second by default

dragonError_t test_attach_detach(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr); // use default pool
    return DRAGON_SUCCESS;
}

dragonError_t test_attach_with_pool(const char * queue_ser) {
    dragonMemoryPoolDescr_t pool;
    dragonError_t err;

    // create a pool
    size_t mem_size = 1UL<<30;
    char fname_char[] = "test_queue_with_pool";
    char * fname = util_salt_filename(fname_char);
    err = dragon_memory_pool_create(&pool, mem_size, fname, 0, nullptr);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to create memory pool");

    // attach queue and pool
    Queue<SerializableInt> q(queue_ser, &pool); // use the specified pool
    SerializableInt x(25);
    q.put(x);
    q.put(x);
    SerializableInt y = q.get();
    assert(y.getVal() == x.getVal());
    y = q.get();
    assert(y.getVal() == x.getVal());

    // destroy the pool
    err = dragon_memory_pool_destroy(&pool);
    if (err != DRAGON_SUCCESS)
        err_fail(err, "Failed to destroy memory pool.");

    return DRAGON_SUCCESS;
}

dragonError_t test_serialize(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    std::string ser = q.serialize();
    std::string queue_ser_str = queue_ser;
    assert(ser.compare(queue_ser_str) == 0);
    return DRAGON_SUCCESS;
}

dragonError_t test_single_put(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    q.put(x);
    SerializableInt y = q.get();
    assert(y.getVal() == x.getVal());
    return DRAGON_SUCCESS;
}

dragonError_t test_multiple_puts(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    size_t num_val = 10;
    SerializableInt** x = new SerializableInt*[num_val];
    for(size_t i=0 ; i<num_val ; i++) {
        x[i] = new SerializableInt((int)i);
        q.put(*x[i]);
    }
    for (size_t i=0 ; i<num_val ; i++) {
        SerializableInt y = q.get();
        assert(y.getVal() == x[i]->getVal());
        delete x[i];
    }
    delete[] x;
    return DRAGON_SUCCESS;
}

dragonError_t test_put_in_full_queue(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    try{
        timespec_t timeout = {0, 0};
        q.put_nowait(x); // put in unblock mode
        throw DragonError(DRAGON_FAILURE, "Failed to caught expected FullError.");
    } catch(FullError ex) {
        return DRAGON_SUCCESS;
    }
}

dragonError_t test_get_nowait(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    try {
        SerializableInt z = q.get_nowait();
        throw DragonError(DRAGON_FAILURE, "Failed to caught expected DragonError.");
    } catch (DragonError ex) {
        std::string err_str = dragon_get_rc_string(ex.rc());
        if (err_str.compare("DRAGON_CHANNEL_EMPTY") == 0)
            return DRAGON_SUCCESS;
        else
            throw DragonError(DRAGON_FAILURE, "Failed to caught expected DragonError.");
    }
}

dragonError_t test_get_from_empty_queue(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    try {
        SerializableInt z = q.get(&TIMEOUT);
        throw DragonError(DRAGON_FAILURE, "Failed to caught expected EmptyError.");
    } catch (EmptyError ex) {
        return DRAGON_SUCCESS;
    }
}

dragonError_t test_get_from_emptied_queue(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    q.put(x);
    SerializableInt y = q.get();
    try {
        SerializableInt z = q.get(&TIMEOUT);
        throw DragonError(DRAGON_FAILURE, "Failed to caught expected EmptyError.");
    } catch (EmptyError ex) {
        return DRAGON_SUCCESS;
    }
}

dragonError_t test_poll_empty_queue(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    bool res = q.poll(&TIMEOUT);
    assert(!res);
    return DRAGON_SUCCESS;
}

dragonError_t test_poll(const char * queue_ser){
    Queue<SerializableInt> q(queue_ser, nullptr);
    // put a value and poll
    SerializableInt x(25);
    q.put(x);
    bool res = q.poll(&TIMEOUT);
    assert(res);
    // remove the value and poll
    SerializableInt y = q.get();
    assert(y.getVal() == x.getVal());
    res = q.poll(&TIMEOUT);
    assert(!res);
    return DRAGON_SUCCESS;
}

dragonError_t test_full(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    bool res = q.full();
    assert(!res);
    return DRAGON_SUCCESS;
}

dragonError_t test_full_from_filled_queue(const char * queue_ser, int maxsize) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    for (int i=0 ; i<maxsize ; i++)
        q.put(x);

    bool res = q.full();
    assert(res);
    return DRAGON_SUCCESS;
}

dragonError_t test_put_with_arg(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    uint64_t arg = 46;
    q.put(x, STREAM_CHANNEL_IS_MAIN_FOR_BUFFERED_SEND, nullptr, arg, false, &TIMEOUT);
    uint64_t recv_arg = 0;
    SerializableInt y = q.get(nullptr, nullptr, &recv_arg, &TIMEOUT);
    assert(arg == recv_arg);
    assert(x.getVal() == y.getVal());
    return DRAGON_SUCCESS;
}

dragonError_t test_empty(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    assert(q.empty(&TIMEOUT));

    SerializableInt x(25);
    for (int i=0 ; i<10 ; i++)
        q.put(x);

    assert(!q.empty(&TIMEOUT));
    for(int i=10 ; i>0 ; i--)
        q.get();

    assert(q.empty(&TIMEOUT));

    return DRAGON_SUCCESS;
}

dragonError_t test_size(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    assert(q.size(&TIMEOUT) == 0);
    SerializableInt x(25);
    for (int i=0 ; i<10 ; i++) {
        q.put(x);
        assert(q.size(&TIMEOUT) == i+1);
    }
    assert(q.size(&TIMEOUT) == 10);
    for(int i=10 ; i>0 ; i--) {
        q.get();
        assert(q.size(&TIMEOUT) == i-1);
    }
    return DRAGON_SUCCESS;
}

dragonError_t test_task_done(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    q.task_done(nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_multiple_task_done(const char * queue_ser, int num_puts) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    for (int i=0 ; i<num_puts ; i++)
        q.task_done(nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_join(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    q.join(&TIMEOUT);
    return DRAGON_SUCCESS;
}

dragonError_t test_new_task(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    q.put(x);
    return DRAGON_SUCCESS;
}

dragonError_t test_multiple_new_tasks(const char * queue_ser, int num_puts) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    for (int i=0 ; i<num_puts ; i++)
        q.put(x);
    q.join(nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_new_tasks_and_task_done_and_join(const char * queue_ser) {
    Queue<SerializableInt> q(queue_ser, nullptr);
    SerializableInt x(25);
    int num_puts = 10;
    for (int i=0 ; i<num_puts ; i++)
        q.put(x);

    for (int i=0 ; i<num_puts ; i++)
        q.task_done(nullptr);

    q.join(nullptr);
    return DRAGON_SUCCESS;
}

dragonError_t test_custom_pickler_dump(const char * queue_ser) {
    Queue<SerializableDouble2DVector> q(queue_ser, nullptr);
    std::vector<std::vector<double>> expected_vals_from_py = {{0.12, 0.31, 3.4}, {4.579, 5.98, 6.54}};
    SerializableDouble2DVector ser_vals_from_py = q.get();
    auto vals_from_py = ser_vals_from_py.getVal();
    for (int i=0 ; i<2 ; i++) {
        for (int j=0 ; j<3 ; j++)
            assert (vals_from_py[i][j] == expected_vals_from_py[i][j]);
    }
    std::cout << std::endl;
    return DRAGON_SUCCESS;
}

dragonError_t test_custom_pickler_load(const char * queue_ser) {
    Queue<SerializableDouble2DVector> q(queue_ser, nullptr);
    std::vector<std::vector<double>> vec = {{0.12, 0.31, 3.4}, {4.579, 5.98, 6.54}};
    SerializableDouble2DVector ser_vec(vec);
    q.put(ser_vec);
    return DRAGON_SUCCESS;
}

dragonError_t test_2d_vector_put(const char * queue_ser) {
    Queue<SerializableDouble2DVector> q(queue_ser, nullptr);
    std::vector<std::vector<double>> vec = {{0.12, 0.31, 3.4}, {4.579, 5.98, 6.54}};
    SerializableDouble2DVector ser_vec(vec);
    q.put(ser_vec);
    SerializableDouble2DVector recv_ser_vals = q.get();
    auto received_val = recv_ser_vals.getVal();
    for (int i=0 ; i<2 ; i++) {
        for (int j=0 ; j<3 ; j++)
            assert (received_val[i][j] == vec[i][j]);
    }
    std::cout << std::endl;
    return DRAGON_SUCCESS;
}

int main(int argc, char* argv[]) {
    char* queue_descr = argv[1];
    std::string test = argv[2];
    dragonError_t err;

    try {
        if (test.compare("test_attach_detach") == 0) {
            err = test_attach_detach(queue_descr);
        } else if (test.compare("test_attach_with_pool") == 0) {
            err = test_attach_with_pool(queue_descr);
        } else if (test.compare("test_serialize") == 0) {
            err = test_serialize(queue_descr);
        } else if (test.compare("test_single_put") == 0) {
            err = test_single_put(queue_descr);
        } else if (test.compare("test_multiple_puts") == 0) {
            err = test_multiple_puts(queue_descr);
        } else if (test.compare("test_put_in_full_queue") == 0) {
            err = test_put_in_full_queue(queue_descr);
        } else if (test.compare("test_get_nowait") == 0) {
            err = test_get_nowait(queue_descr);
        } else if (test.compare("test_get_from_empty_queue") == 0) {
            err = test_get_from_empty_queue(queue_descr);
        } else if (test.compare("test_get_from_emptied_queue") == 0) {
            err = test_get_from_emptied_queue(queue_descr);
        } else if (test.compare("test_poll_empty_queue") == 0) {
            err = test_poll_empty_queue(queue_descr);
        } else if (test.compare("test_poll") == 0) {
            err = test_poll(queue_descr);
        } else if (test.compare("test_full") == 0) {
            err = test_full(queue_descr);
        } else if (test.compare("test_full_from_filled_queue") == 0) {
            int maxsize = std::stoi(argv[3]);
            err = test_full_from_filled_queue(queue_descr, maxsize);
        } else if (test.compare("test_put_with_arg") == 0) {
            err = test_put_with_arg(queue_descr);
        } else if (test.compare("test_empty") == 0) {
            err = test_empty(queue_descr);
        } else if (test.compare("test_size") == 0) {
            err = test_size(queue_descr);
        } else if (test.compare("test_task_done") == 0) {
            err = test_task_done(queue_descr);
        } else if (test.compare("test_multiple_task_done") == 0) {
            int num_puts = std::stoi(argv[3]);
            err = test_multiple_task_done(queue_descr, num_puts);
        } else if (test.compare("test_join") == 0) {
            err = test_join(queue_descr);
        } else if (test.compare("test_new_task") == 0) {
            err = test_new_task(queue_descr);
        } else if (test.compare("test_multiple_new_tasks") == 0) {
            int num_puts = std::stoi(argv[3]);
            err = test_multiple_new_tasks(queue_descr, num_puts);
        } else if (test.compare("test_new_tasks_and_task_done_and_join") == 0) {
            err = test_new_tasks_and_task_done_and_join(queue_descr);
        } else if (test.compare("test_custom_pickler_dump") == 0) {
            err = test_custom_pickler_dump(queue_descr);
        } else if (test.compare("test_custom_pickler_load") == 0) {
            err = test_custom_pickler_load(queue_descr);
        } else if (test.compare("test_2d_vector_put") == 0) {
            err = test_2d_vector_put(queue_descr);
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