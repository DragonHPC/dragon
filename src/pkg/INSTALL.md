# Installation Notes
The Dragon runtime environment includes Python-specific and non-Python-specific object files.  The non-Python
shared objects are separated out from the Dragon Python wheel file.  This is to support use cases of the
Dragon runtime environment from other languages, such as Fortran/C/C++.

Before you can run programs using Dragon, you must set up the run-time for your
environment. You must have Python 3.9 installed and it must be in your path
somewhere. A common choice is to use a Python virtual environment, which can be initialized
from a base Python 3.9+ with:

        python3 -m venv --clear _env
        . _env/bin/activate


The untarred distribution file contains several subdirectories. All provided commands
are relative to the directory that contains the README.md.

The `dragon-*.whl` file must be pip3 installed once for your environment.

        pip3 install --force-reinstall dragon-0.61-cp39-cp39-linux_x86_64.whl


Check and possibly update that `$PATH` is has the location of pip installed
console scripts, such as ~/.local/bin if you're not using a virtual environment.

        export PATH=~/.local/bin:${PATH}


Set up the path to the Dragon module

        module use [/absolute path to directory with this INSTALL.md file]/modulefiles


Load the Dragon module

        module load dragon


Test your configuration using the point-to-point latency test.  You should see output similar to below after the
`dragon` command.
```
cd examples/multiprocessing

dragon p2p_lat.py --dragon
using Dragon
Msglen [B]   Lat [usec]
2  28.75431440770626
4  39.88605458289385
8  37.25141752511263
16  43.31085830926895
+++ head proc exited, code 0
```

Test the same across two nodes within an allocation.
```
salloc --nodes=2 --exclusive
dragon p2p_lat.py --dragon
using Dragon
Msglen [B]   Lat [usec]
2  1244.2267920050654
4  1369.1070925051463
8  1383.1850550050146
16  1378.226093002013
```
