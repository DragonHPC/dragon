#include <iostream>
#include <cmath>
#include <string>
#include<unistd.h>

#include <serializable.hpp>
#include <dragon/dictionary.hpp>
#include <dragon/return_codes.h>


static timespec_t TIMEOUT = {0,5000000000}; // Timeouts will be 5 second by default
static int NUM_POINTS = 100000;

// Begin user program

bool in_circle(double x, double y) {
    return sqrt(pow(x, 2) + pow(y, 2)) <= 1;
}

void training(char* ddict_descr, int client_id, int num_procs, uint64_t digits, double places, bool trace) {
    pid_t pid = getpid();
    // reseed
    srand(time(NULL) + pid);
    SerializableInt key_client_id(client_id);
    DDict<SerializableInt, SerializableDouble> child_dd(ddict_descr, &TIMEOUT);

    double precision = pow(10, digits);

    double avg = 0.0;
    bool done = false;

    double NUM_POINTS_DOUBLE = (double)NUM_POINTS;
    double step_double = 0.0;
    double num_procs_double = 0.0;

    try {
        while (!done) {

            // generate a set of random points between (-1, 1) check if they are in the circle
            double num_points_in_circle = 0;
            for (int i=0 ; i<NUM_POINTS ; i++) {
                double x = (rand()%(int)(2*precision) - precision)/precision;
                double y = (rand()%(int)(2*precision) - precision)/precision;
                if (in_circle(x, y))
                    num_points_in_circle += 1;
            }
            // calculate the ratio of points fall within the circle to total number of points generated
            double new_sim = num_points_in_circle / NUM_POINTS_DOUBLE;

            // calculate the weighted average of the ratio
            double local_avg = ((num_procs_double * step_double * NUM_POINTS_DOUBLE) * avg + new_sim * (double)NUM_POINTS_DOUBLE) / ((num_procs_double * step_double * NUM_POINTS_DOUBLE) + NUM_POINTS_DOUBLE);

            // write new simulation result to dictionary as a non-persistent key that will be updated in every future checkpoint
            SerializableDouble val_local_average(local_avg);
            child_dd[key_client_id] = val_local_average;
            if (trace)
                cout<<"Checkpoint "<<step_double<<", client "<<client_id<<", local average is "<<local_avg*4<<endl;

            // retrieve result from aggregate dictionary
            double prev_avg = avg;
            SerializableInt key(-1);
            SerializableDouble result = child_dd[key];
            avg = result.getVal();

            // stop the loop if the it has converged to a value that changes less than places
            // on each iteration.
            if (abs(avg - prev_avg) < places)
                done = true;

            // moving to the next checkpoint/iteration
            child_dd.checkpoint();
            step_double += 1;
        }
        cout<<"PI simulation result = "<<4*avg<< " from client "<<client_id<<endl;
    } catch (const DragonError& ex) {
        fprintf(stderr, "Client %d, ec: %s, err_str: %s\n", client_id, dragon_get_rc_string(ex.get_rc()), ex.get_err_str());
        fflush(stderr);
    } catch (...) {
        fprintf(stderr, "Client %d, caught unexpected err\n", client_id);
        fflush(stderr);
    }
}

int main(int argc, char* argv[]) {
    char* ddict_descr = argv[1];
    char* client_id_str = argv[2];
    char* num_procs_str = argv[3];
    std::string num_digits_str = argv[4];
    std::string trace_str = argv[5];

    uint64_t digits = std::stoull(num_digits_str);
    int client_id = std::atoi(client_id_str);
    int num_procs = std::atoi(num_procs_str);

    double exponent = -1-(double)digits;
    double places = pow(5, exponent);

    bool trace;
    if (trace_str.compare("true") == 0) {
        trace = true;
    } else if (trace_str.compare("false") == 0) {
        trace = false;
    } else {
        cout<<"Invalid trace value: "<<trace_str<<", trace should be either true or false."<<endl;
        return DRAGON_INVALID_ARGUMENT;
    }

    srand(time(NULL));

    try {
        training(ddict_descr, client_id, num_procs, digits, places, trace);
    } catch (const DragonError& ex) {
        fprintf(stderr, "ec: %s, err_str: %s\n",dragon_get_rc_string(ex.get_rc()), ex.get_err_str());
        fflush(stderr);
        return DRAGON_FAILURE;
    } catch (...) {
        fprintf(stderr, "caught unexpected err\n");
        fflush(stderr);
        return DRAGON_FAILURE;
    }
    return DRAGON_SUCCESS;
}