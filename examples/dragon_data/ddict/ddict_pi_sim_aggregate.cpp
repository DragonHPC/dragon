#include <iostream>
#include <assert.h>
#include <cmath>
#include <string>
#include<unistd.h>

#include <serializable.hpp>
#include <dragon/dictionary.hpp>
#include <dragon/return_codes.h>


static timespec_t TIMEOUT = {0,5000000000}; // Timeouts will be 5 second by default

/*
 * This function demonstrate several CPP client APIs of the distributed dictionary.
 */
void read_write_erase_metadata(char * ddict_descr, int num_procs) {

    DDict<SerializableString, SerializableInt> dd(ddict_descr, &TIMEOUT);

    // This dictionary will perform raed/write/erase/get keys operations only on  manager 0.
    DDict<SerializableString, SerializableInt> dd_manager0 = dd.manager(0);

    SerializableString key("num_clients");
    SerializableInt num_clients(num_procs);
    SerializableString key1("num_points");
    int NUM_POINTS = 100000;
    SerializableInt num_points(NUM_POINTS);

    dd_manager0.pput(key, num_clients);
    assert (dd_manager0.contains(key));
    SerializableInt val = dd_manager0[key];
    assert (val.getVal() == num_clients.getVal());

    dd_manager0.pput(key1, num_points);
    assert (dd_manager0.contains(key1));
    SerializableInt val1 = dd_manager0[key1];
    assert (val1.getVal() == num_points.getVal());

    assert (dd_manager0.size() == 2);

    // get all keys from manager 0
    auto keys = dd_manager0.keys();
    for(int i=0 ; i<keys.size() ; i++) {
        std::string val = keys[i]->getVal();
        cout<<val<<endl;
    }
    dd_manager0.erase(key);
    assert (!(dd_manager0.contains(key)));
    assert (dd_manager0.size() == 1);
    // clear the dictionary
    dd.clear();
}

int main(int argc, char* argv[]) {
    char* ddict_descr = argv[1];
    char* num_procs_str = argv[2];
    std::string num_digits_str = argv[3];
    std::string trace_str = argv[4];

    uint64_t digits = std::stoull(num_digits_str);
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

    // Not for the simulation purpose, just demonstrate CPP client APIs.
    // read_write_erase_metadata(ddict_descr, num_procs);

    try{

        double avg = 0.0;
        bool done = false;
        DDict<SerializableInt, SerializableDouble> aggregate_dd(ddict_descr, &TIMEOUT);
        while (!done) {
            double sum_of_avgs = 0.0;
            for (int i=0 ; i<num_procs ; i++) {
                SerializableInt client_id(i);
                SerializableDouble result = aggregate_dd[client_id];
                sum_of_avgs += result.getVal();
            }
            double prev_avg = avg;
            avg = sum_of_avgs / (double)num_procs;
            SerializableDouble val(avg);
            SerializableInt key(-1);
            aggregate_dd[key] = val;

            if (trace) {
                int chkpt = aggregate_dd.checkpoint_id();
                cout<<"Step "<<chkpt<<" result: "<<4*avg<<endl;
            }

            if (abs(avg - prev_avg) < places)
                done = true;

            aggregate_dd.checkpoint();
        }
        cout<<"result: "<<4*avg<<endl;
    } catch (const DragonError& ex) {
        fprintf(stderr, "in parent proc, ec: %s, err_str: %s\n",dragon_get_rc_string(ex.get_rc()), ex.get_err_str());
        fflush(stderr);
        return DRAGON_FAILURE;
    } catch (...) {
        fprintf(stderr, "caught unexpected err\n");
        fflush(stderr);
        return DRAGON_FAILURE;
    }
    return DRAGON_SUCCESS;
}