/**
 * C++ test implementation for SerializableNDArray and related Serializable classes.
 * This works in concert with test_serializables_driver.py to provide integrated tests.
 */

#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <unistd.h>
#include <dragon/queue.hpp>
#include <dragon/serializable.hpp>
#include <dragon/dictionary.hpp>
#include <dragon/barrier.hpp>
#include <dragon/semaphore.hpp>

using namespace dragon;

/**
 * Test receiving an XNDArray through a Queue and validating its data
 */
int test_xndarray_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {

    try {
        // Attach to the queue using the C++ template API
        Queue<Serializable> in_queue(ser_in_queue, nullptr);
        Queue<Serializable> out_queue(ser_out_queue, nullptr);

        // Receive the Serializable object from the queue with a 5 second timeout
        timespec_t timeout;
        timeout.tv_sec = 10;
        timeout.tv_nsec = 0;

        SerializableDoubleNDArray received = in_queue.get(&timeout);

        // Expected data: [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
        std::vector<std::vector<double>> expected = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};

        for (int i=0; i<received.size();i++) {
            SerializableDoubleNDArray row = received[i];
            for (int j=0;j<row.size();j++) {
                double element = received[i][j];
                if (element != expected[i][j]) {
                    std::cerr << "The expected value at row " << i << " column " << j << " was " << expected[i][j] << " and it was " << element << " instead." << std::endl;
                    return 1;
                }
            }
        }

        received[0][2] = 8.0;
        received.sync();

        int count = 0;
        /* While writing code like this loop would be a terrible idea in general, it tests
           refresh in C++ and sync in Python. */
        while (received[0][0] != 2.0 && count < 1000) {
            sleep(1);
            received.refresh();
        }
        SerializableDoubleNDArray copy = received[1].regionAsNDArray();
        SerializableDoubleNDArray copy2 = received[0].regionAsNDArray();

        copy2.destroy();
        copy2.ddict_detach();

        out_queue.put(copy);

        return 0;

    } catch (const TimeoutError& e) {
        std::cerr << "Timeout while receiving from queue: " << e.err_str() << std::endl;
        return 1;
    } catch (const EmptyError& e) {
        std::cerr << "Queue is empty: " << e.err_str() << std::endl;
        return 1;
    } catch (const DragonError& e) {
        std::cerr << "Dragon error in test_xndarray_through_queue: " << e.err_str() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Exception in test_xndarray_through_queue: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown exception in test_xndarray_through_queue" << std::endl;
        return 1;
    }
}

/**
 * Helpers shared by the tests below.
 */
static timespec_t test_timeout() {
    timespec_t timeout;
    timeout.tv_sec = 10;
    timeout.tv_nsec = 0;
    return timeout;
}

template<class T>
static int check_vector(const std::vector<T>& got, const std::vector<T>& expected, const char* what) {
    if (got.size() != expected.size()) {
        std::cerr << what << ": expected " << expected.size() << " elements and got " << got.size() << " instead." << std::endl;
        return 1;
    }

    for (size_t i=0; i<got.size(); i++) {
        if (got[i] != expected[i]) {
            std::cerr << what << ": expected " << expected[i] << " at index " << i << " and got " << got[i] << " instead." << std::endl;
            return 1;
        }
    }

    return 0;
}

template<class T>
static int check_matrix(const std::vector<std::vector<T>>& got, const std::vector<std::vector<T>>& expected, const char* what) {
    if (got.size() != expected.size()) {
        std::cerr << what << ": expected " << expected.size() << " rows and got " << got.size() << " instead." << std::endl;
        return 1;
    }

    for (size_t i=0; i<got.size(); i++)
        if (check_vector(got[i], expected[i], what) != 0)
            return 1;

    return 0;
}

/**
 * Test a SerializableString round-trip with Python.
 */
int test_string_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    SerializableString received = in_queue.get(&timeout);

    if (received.val() != "Hello from Python") {
        std::cerr << R"(Expected "Hello from Python" and got ")" << received.val() << "\" instead." << std::endl;
        return 1;
    }

    out_queue.put("Hello from C++");

    return 0;
}

/**
 * Test a SerializableInt round-trip with Python.
 */
int test_int_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    int received = in_queue.get(&timeout);

    if (received != 42) {
        std::cerr << "Expected 42 and got " << received << " instead." << std::endl;
        return 1;
    }

    out_queue.put(received * 2);

    return 0;
}

/**
 * Test a SerializableDouble round-trip with Python.
 */
int test_double_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    double received = in_queue.get(&timeout);

    if (received != 3.5) {
        std::cerr << "Expected 3.5 and got " << received << " instead." << std::endl;
        return 1;
    }

    out_queue.put(received * 2.0);

    return 0;
}

/**
 * Test a SerializableByteBuffer round-trip with Python.
 */
int test_bytes_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    SerializableByteBuffer received = in_queue.get(&timeout);
    std::string expected = "bytes from Python";

    if (received.getSize() != expected.size() ||
        std::memcmp(received.getPtr(), expected.data(), expected.size()) != 0) {
        std::cerr << "Expected \"" << expected << "\" and got " << received.getSize() << " bytes of other data instead." << std::endl;
        return 1;
    }

    std::string reply = "bytes from C++";
    out_queue.put(Serializable(reply.size(), (uint8_t*)reply.data()));

    return 0;
}

/**
 * Test a SerializableIntVector round-trip with Python.
 */
int test_int_vector_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    std::vector<int> received = in_queue.get(&timeout);
    std::vector<int> expected = {1, 2, 3, 4};

    if (check_vector(received, expected, "int vector") != 0)
        return 1;

    for (auto& element : received)
        element *= 2;

    out_queue.put(received);

    return 0;
}

/**
 * Test a SerializableDoubleVector round-trip with Python.
 */
int test_double_vector_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    std::vector<double> received = in_queue.get(&timeout);
    std::vector<double> expected = {1.5, 2.5, 3.5, 4.5};

    if (check_vector(received, expected, "double vector") != 0)
        return 1;

    for (auto& element : received)
        element *= 2.0;

    out_queue.put(received);

    return 0;
}

/**
 * Test a Serializable2DIntMatrix round-trip with Python.
 */
int test_int_matrix_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    std::vector<std::vector<int>> received = in_queue.get(&timeout);
    std::vector<std::vector<int>> expected = {{1, 2, 3}, {4, 5, 6}};

    if (check_matrix(received, expected, "int matrix") != 0)
        return 1;

    for (auto& row : received)
        for (auto& element : row)
            element *= 2;

    out_queue.put(received);

    return 0;
}

/**
 * Test a Serializable2DDoubleMatrix round-trip with Python.
 */
int test_double_matrix_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    std::vector<std::vector<double>> received = in_queue.get(&timeout);
    std::vector<std::vector<double>> expected = {{1.5, 2.5, 3.5}, {4.5, 5.5, 6.5}};

    if (check_matrix(received, expected, "double matrix") != 0)
        return 1;

    for (auto& row : received)
        for (auto& element : row)
            element *= 2.0;

    out_queue.put(received);

    return 0;
}

/**
 * Receive a 2D SerializableNDArray of the given type, validate it, double every element
 * and write it back to the DDict, then return the last row to Python as its own NDArray.
 */
template<class NDArrayType, class T>
static int ndarray_through_queue(const char* ser_in_queue, const char* ser_out_queue, const std::vector<std::vector<T>>& expected, const char* what) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    NDArrayType received = in_queue.get(&timeout);

    if ((size_t)received.size() != expected.size()) {
        std::cerr << what << ": expected " << expected.size() << " rows and got " << received.size() << " instead." << std::endl;
        return 1;
    }

    for (int i=0; i<received.size(); i++) {
        NDArrayType row = received[i];

        if ((size_t)row.size() != expected[i].size()) {
            std::cerr << what << ": expected " << expected[i].size() << " columns in row " << i << " and got " << row.size() << " instead." << std::endl;
            return 1;
        }

        for (int j=0; j<row.size(); j++) {
            T element = received[i][j];

            if (element != expected[i][j]) {
                std::cerr << what << ": expected " << expected[i][j] << " at row " << i << " column " << j << " and got " << element << " instead." << std::endl;
                return 1;
            }

            received[i][j] = (T)(element * 2);
        }
    }

    received.sync();

    out_queue.put(received[received.size()-1].regionAsNDArray());

    return 0;
}

/**
 * Test a SerializableFloatNDArray round-trip with Python.
 */
int test_float_ndarray_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    std::vector<std::vector<float>> expected = {{1.0f, 2.0f, 3.0f}, {4.0f, 5.0f, 6.0f}};
    return ndarray_through_queue<SerializableFloatNDArray, float>(ser_in_queue, ser_out_queue, expected, "float ndarray");
}

/**
 * Test a SerializableIntNDArray round-trip with Python.
 */
int test_int_ndarray_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    std::vector<std::vector<int>> expected = {{1, 2, 3}, {4, 5, 6}};
    return ndarray_through_queue<SerializableIntNDArray, int>(ser_in_queue, ser_out_queue, expected, "int ndarray");
}

/**
 * Test a SerializableLongNDArray round-trip with Python.
 */
int test_long_ndarray_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    std::vector<std::vector<long>> expected = {{1, 2, 3}, {4, 5, 6}};
    return ndarray_through_queue<SerializableLongNDArray, long>(ser_in_queue, ser_out_queue, expected, "long ndarray");
}

/**
 * Test a SerializableQueue round-trip with Python.
 *
 * The Queue that arrives on the queue is attached to, a value is placed on it, and a
 * descriptor for it is sent back to Python.
 */
int test_queue_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    Queue<Serializable> inner = in_queue.get(&timeout);

    inner.put(SerializableString("Hello from the inner queue"));

    out_queue.put(inner);

    return 0;
}

/**
 * Test a SerializableDDict round-trip with Python.
 *
 * The DDict that arrives on the queue is attached to, a key/value pair is stored in it,
 * and a descriptor for it is sent back to Python.
 */
int test_ddict_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    DDict<Serializable, Serializable> inner = in_queue.get(&timeout);

    inner[SerializableString("hello")] = SerializableInt(42);

    out_queue.put(inner);

    return 0;
}

/**
 * Test a SerializableBarrier round-trip with Python.
 *
 * The Barrier that arrives on the queue is attached to and waited on with Python, and a
 * descriptor for it is sent back to Python.
 */
int test_barrier_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    Barrier inner = in_queue.get(&timeout);

    out_queue.put(inner);

    inner.wait(&timeout);

    return 0;
}

/**
 * Test a SerializableSemaphore round-trip with Python.
 *
 * The Semaphore that arrives on the queue is attached to and released, and a descriptor
 * for it is sent back to Python.
 */
int test_semaphore_through_queue(const char* ser_in_queue, const char* ser_out_queue, const char* /*ser_ddict*/) {
    Queue<Serializable> in_queue(ser_in_queue, nullptr);
    Queue<Serializable> out_queue(ser_out_queue, nullptr);
    timespec_t timeout = test_timeout();

    Semaphore inner = in_queue.get(&timeout);

    inner.release(1, &timeout);

    out_queue.put(inner);

    return 0;
}

struct TestCase {
    const char* name;
    int (*fn)(const char*, const char*, const char*);
};

static const TestCase TESTS[] = {
    {"test_xndarray_through_queue", test_xndarray_through_queue},
    {"test_string_through_queue", test_string_through_queue},
    {"test_int_through_queue", test_int_through_queue},
    {"test_double_through_queue", test_double_through_queue},
    {"test_bytes_through_queue", test_bytes_through_queue},
    {"test_int_vector_through_queue", test_int_vector_through_queue},
    {"test_double_vector_through_queue", test_double_vector_through_queue},
    {"test_int_matrix_through_queue", test_int_matrix_through_queue},
    {"test_double_matrix_through_queue", test_double_matrix_through_queue},
    {"test_float_ndarray_through_queue", test_float_ndarray_through_queue},
    {"test_int_ndarray_through_queue", test_int_ndarray_through_queue},
    {"test_long_ndarray_through_queue", test_long_ndarray_through_queue},
    {"test_queue_through_queue", test_queue_through_queue},
    {"test_ddict_through_queue", test_ddict_through_queue},
    {"test_barrier_through_queue", test_barrier_through_queue},
    {"test_semaphore_through_queue", test_semaphore_through_queue}
};

/**
 * Main entry point - routes to the appropriate test based on command line argument
 */
int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0] << " <serialized_in_queue> <serialized_out_queue> <serialized_ddict> <test_name>" << std::endl;
        return 1;
    }

    const char* ser_in_queue = argv[1];
    const char* ser_out_queue = argv[2];
    const char* ser_ddict = argv[3];
    const char* test_name = argv[4];

    for (const auto& test : TESTS) {
        if (std::strcmp(test_name, test.name) == 0) {
            try {
                return test.fn(ser_in_queue, ser_out_queue, ser_ddict);
            } catch (const TimeoutError& e) {
                std::cerr << test_name << ": timeout while using the queue: " << e.err_str() << std::endl;
            } catch (const EmptyError& e) {
                std::cerr << test_name << ": queue is empty: " << e.err_str() << std::endl;
            } catch (const DragonError& e) {
                std::cerr << test_name << ": dragon error: " << e.err_str() << std::endl;
            } catch (const std::exception& e) {
                std::cerr << test_name << ": exception: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << test_name << ": unknown exception" << std::endl;
            }

            return 1;
        }
    }

    std::cerr << "Unknown test: " << test_name << std::endl;

    return 1;
}
