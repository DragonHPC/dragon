#ifndef PARSE_ARGS_HPP
#define PARSE_ARGS_HPP

// NOTE: we have to define globals in this file to use argparse,
// so this header can only be include one place (hsta.cpp)

#include <argp.h>
#include <stdlib.h>

const char *argp_program_version = "HSTA version 0.0.0";

static char doc[] = "\nHight Speed Transport Agent. See dragonhpc.org for more details.";

static char args_doc[] = "";

static struct argp_option options[] = {
    { "dump-network-config", 'd', 0, 0, "Indicates that HSTA should dump it's network config to a JSON file and exit."},
    { "num-threads", 't', "NUMTHREADS", 0, "Specifies number of threads to use for HSTA."},
    { 0 } 
};

struct arguments {
    bool dump_net_config;
    int num_threads;
};

static error_t
parse_args(int key, char *arg, struct argp_state *state)
{
    struct arguments *arguments = (struct arguments *) state->input;

    switch (key)
    {
        case 'd': arguments->dump_net_config = true; break;
        case 't': arguments->num_threads     = arg ? atoi(arg) : 0; break;
        case ARGP_KEY_ARG:
        case ARGP_KEY_END: break;
        default: return ARGP_ERR_UNKNOWN;
    }

    return 0;
}

class ParseArgs {
private:

    struct arguments arguments;

public:

    ParseArgs(int argc, char **argv)
    {
        arguments = { .dump_net_config = false, .num_threads = -1 };
        struct argp argp = { options, parse_args, args_doc, doc, 0, 0, 0 };
        argp_parse(&argp, argc, argv, 0, 0, &this->arguments);
    }

    bool get_dump_net_config()
    {
        return arguments.dump_net_config;
    }

    int get_num_threads()
    {
        return arguments.num_threads;
    }
};

#endif // PARSE_ARGS_HPP

