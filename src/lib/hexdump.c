#include "_hexdump.h"

#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define TRUE 1
#define FALSE 0

/** @brief Dump a blob of memory to a file descriptor for debug purposes.
 *
 *  This prints the contents of a blob of memory as a hex dump to a given file descriptor.
 *  Multiple lines of zeroes in the dump will be supressed so only the first and last line
 *  of zeroes is printed.
 *
 *  @param fd An open, writable file descriptor.
 *  @param title A null-terminated string to identify the dump in the output.
 *  @param addr The pointer to the memory to be dumped.
 *  @param len The number of bytes to dump.
 *  @param indent A null-terminated string of characters to prepend to each line of output.
 *  @return void
 */

void hex_dump_to_fd(FILE* fd, const char *title, const void *addr, size_t len, const char* indent)
{
    size_t i,j;
    unsigned char ascii[17];
    unsigned char* pc = (unsigned char*)addr;
    size_t skip = 0;
    char skipping = FALSE;

    if (title != NULL)
        fprintf(fd, "%s%s:\n", indent, title);

    // Process every byte in the data.
    for (i = 0; i < len; i++) {
        // Multiple of 16 means new line (with line offset).
        if (i%16 == 0 && skip <= 1) {
            // Just don't print ASCII for the zeroth line.
            if (i!=0)
                fprintf(fd, "  %s\n", ascii);
        }

        if ((i % 16) == 0 && (i != 0)) {
            skipping = TRUE;
            for (j=i; j<MIN(i+16,len); j++)
                if (pc[j] != 0) {
                    skipping = FALSE;
                }

            // If we are on the last line of the dump, print it.
            if ((len-i) <= 16)
                skipping = FALSE;

            if (skipping)
                skip++;
            else
                skip=0;
        }

        if (i % 16 == 0) {
            if (skip < 2) {
                fprintf(fd, "%s  %016llX", indent, (unsigned long long int)addr+i);
            } else if (skip == 2) {
                fprintf(fd, "%s  ...\n", indent);
            } else if (skip > 2) {
            // do nothing.
            }
        }

        if (skip <= 1) {
            // Now the hex code for the specific character.
            fprintf(fd, " %02x", pc[i]);
        }

        // And store a printable ASCII character for later.
        if ((pc[i] < 0x20) || (pc[i] > 0x7e)) {
            ascii[i % 16] = '.';
        } else {
            ascii[i % 16] = pc[i];
        }

        ascii[(i % 16) + 1] = '\0';
    }


    // Pad out last line if not exactly 16 characters.
    while ((i % 16) != 0) {
        fprintf(fd, "   ");
        i++;
    }

    // And print the final ASCII bit.
    fprintf(fd, "  %s\n", ascii);
}

/** @brief Dump a blob of memory to standard output for debug purposes.
 *
 *  This prints the contents of a blob of memory as a hex dump to standard output.
 *  Multiple lines of zeroes in the dump will be supressed so only the first and last line
 *  of zeroes is printed.
 *
 *  @param title A null-terminated string to identify the dump in the output.
 *  @param addr The pointer to the memory to be dumped.
 *  @param len The number of bytes to dump.
 *  @param indent A null-terminated string of characters to prepend to each line of output.
 *  @return void
 */

void hex_dump(const char *title, const void *addr, const size_t len, const char* indent) {
    hex_dump_to_fd(stdout, title, addr, len, indent);
}
