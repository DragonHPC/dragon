"""Example: ProcessGroup with per-process file output redirection.

Each of four processes writes a unique haiku to its own output file
using Dragon's file-path stdout redirection feature, managed as a
ProcessGroup rather than individual Process objects.

Usage:
    dragon file_redirect_haiku.py
"""

import os

import dragon
import multiprocessing as mp

from dragon.native.process import ProcessTemplate
from dragon.native.process_group import ProcessGroup

HAIKUS = [
    "An old silent pond\nA frog jumps into the pond\nSplash! Silence again",
    "Autumn moonlight\nA worm digs silently\nInto the chestnut",
    "In the twilight rain\nThese brilliant-hued hibiscus\nA lovely sunset",
    "Over the wintry\nForest winds howl in rage\nWith no leaves to blow",
]


def write_haiku(rank):
    """Print a haiku to stdout (which is redirected to a file)."""
    print(HAIKUS[rank])


def main():
    mp.set_start_method("dragon")

    output_files = [f"haiku_{i}.txt" for i in range(4)]
    #output_file="haikus.txt"
    # Create a ProcessGroup and add one process per haiku,
    # each with its own stdout file redirection
    pg = ProcessGroup()
    for i in range(4):
        template = ProcessTemplate(
            target=write_haiku,
            args=(i,),
            stdout=output_files[i],
            #stdout=output_file,
        )
        pg.add_process(nproc=1, template=template)

    pg.init()
    pg.start()
    pg.join()
    pg.close()

    # Display results
    print("All processes complete. Output files:")
    print("-" * 40)
    for path in output_files:
        print(f"\n[{path}]")
        with open(path, "rb") as f:
            print(f.read().decode("utf-8", errors="replace").strip())



if __name__ == "__main__":
    main()
