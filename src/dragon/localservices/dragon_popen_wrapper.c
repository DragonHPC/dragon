#include <dragon/return_codes.h>
#include <dragon/utils.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

int main(int argc, char* argv[]) {

    if (argc < 3) {
        fprintf(stderr, "Two few arguments provided to dragon-popen.\nUsage: dragon-popen <affinity_string> <args>\n");
        return -1;
    }

    /* argv[1] will be a string of integers which are the
       cores to be set as core affinity for this process. */
    dragon_set_my_core_affinity(argv[1]);

    int rc = execvp(argv[2], &argv[2]);
    fprintf(stderr, "execvp return code=%d for %s\n", rc, argv[2]);
    fprintf(stderr, "Error: Could not exec: %s (errno: %d)\n", strerror(errno), errno);
    return -1;
}