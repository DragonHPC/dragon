
#ifndef HAVE_DRAGON_HEXDUMP_H
#define HAVE_DRAGON_HEXDUMP_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

/* -----------------------------------------------------------------------------
 * The hex_dump function dumps memory to stdout for debug purposes. It prints
 * addresses on each line. In a sequence of 0's the first 16 and last 16 byte
 * lines are printed and one ... is printed to represent all suppressed lines.
 * ASCII versions of printable characters appear to the right side of the dump.
 *
 * NOTE: On x86 systems muli-byte values (i.e. pointers, integers) are stored in
 * little endian format. For instance, when dumped the pointer 00007F733441E010
 * prints as 10 E0 41 34 73 7F 00 00.
 *
 * The title is printed before the dump is printed. The indent is printed at the
 * start of each line of the dump. Addr and len indicate the starting address
 * and the number of bytes dumped.
 *
 * The hex_dump_to_fd provides the additional flexibility to dump to any writable
 * file descriptor.
 * ----------------------------------------------------------------------------- */
void
hex_dump(const char *title, const void *addr, const size_t len, const char* indent);

void
hex_dump_to_fd(FILE* fd, const char *title, const void *addr, const size_t len, const char* indent);

#endif
