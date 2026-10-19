/**
 * A C++ worker for the heat diffusion example. See heat_simulation.py for the
 * Python side of this program and README.md for a description of the example.
 *
 * The worker is given nothing but a serialized Distributed Dictionary and its
 * worker id. Everything else it needs, including the shared grid, the Barrier
 * it synchronizes on and the Queue it reports results to, is stored in that
 * dictionary as a Serializable. This is the point of the example: any Dragon
 * object that can be serialized can be handed from Python to C++ this way.
 */

#include <iostream>
#include <string>
#include <cmath>

#include <dragon/serializable.hpp>
#include <dragon/dictionary.hpp>
#include <dragon/queue.hpp>
#include <dragon/barrier.hpp>
#include <dragon/semaphore.hpp>

using namespace dragon;

static timespec_t TIMEOUT = {30, 0};

static SerializableString key(const std::string& name) {
    return SerializableString(name);
}

static SerializableString key(const std::string& name, int worker_id) {
    return SerializableString(name + std::to_string(worker_id));
}

int simulate(const char* ser_config, int worker_id) {
    //! [start-attaching-cpp]
    DDict<Serializable, Serializable> config(ser_config, &TIMEOUT);

    // Every one of these values was written by Python.
    int cols = config[key("cols")];
    int steps = config[key("steps")];
    int start_row = config[key("start_", worker_id)];
    int end_row = config[key("end_", worker_id)];

    // The whole grid. Its data lives in the DDict, so only the meta data was
    // passed to us. Nothing is read until refresh is called.
    SerializableDoubleNDArray grid = config[key("grid")];

    // Just the rows this worker owns. This is where our results are published.
    SerializableDoubleNDArray band = config[key("band_", worker_id)];

    Barrier barrier = config[key("barrier")];
    Semaphore ready = config[key("ready")];
    Queue<Serializable> results = config[key("results")];

    //! [end-attaching-cpp]
    // Let Python know this worker has attached to everything.
    ready.release();

    double max_delta = 0.0;

    for (int step = 0; step < steps; step++) {
        // Pull the grid Python published at the end of the previous step.
        grid.refresh();

        max_delta = 0.0;

        for (int i = start_row; i < end_row; i++) {
            // Indexing a row once per row rather than once per column keeps the
            // temporaries out of the inner loop.
            //! [slice-start]
            SerializableDoubleNDArray above = grid[i-1];
            SerializableDoubleNDArray row = grid[i];
            SerializableDoubleNDArray below = grid[i+1];
            SerializableDoubleNDArray out = band[i - start_row];
            //! [slice-end]

            for (int j = 0; j < cols; j++) {
                double current = row[j];
                double updated;

                if (j == 0 || j == cols - 1) {
                    // The left and right edges are held at a fixed temperature.
                    updated = current;
                } else {
                    updated = 0.25 * ((double)above[j] + (double)below[j] +
                                      (double)row[j-1] + (double)row[j+1]);
                }

                double delta = std::fabs(updated - current);
                if (delta > max_delta)
                    max_delta = delta;

                out[j] = updated;
            }
        }

        // Publish this worker's rows, then wait for every other worker to publish
        // theirs before Python stitches them back into the grid.
        band.sync();
        barrier.wait(&TIMEOUT);

        // Wait again so nobody starts the next step until the new grid is published.
        barrier.wait(&TIMEOUT);
    }

    results.put(max_delta);

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <serialized_ddict> <worker_id>" << std::endl;
        return 1;
    }

    const char* ser_config = argv[1];
    int worker_id = std::stoi(argv[2]);

    try {
        return simulate(ser_config, worker_id);
    } catch (const TimeoutError& e) {
        std::cerr << "worker " << worker_id << ": timed out: " << e.err_str() << std::endl;
    } catch (const DragonError& e) {
        std::cerr << "worker " << worker_id << ": dragon error: " << e.err_str() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "worker " << worker_id << ": exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "worker " << worker_id << ": unknown exception" << std::endl;
    }

    return 1;
}
