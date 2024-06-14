# Installation Notes
The Dragon runtime environment includes Python-specific and non-Python-specific object files.  The non-Python
shared objects are separated out from the Dragon Python wheel file.  This is to support use cases of the
Dragon runtime environment from other languages, such as Fortran/C/C++.

Before you can run programs using Dragon, you must set up the run-time for your
environment. You must have Python 3.9, 3.10, or 3.11 installed and it must correspond to the version of the Dragon package that was downloaded. A common choice for running Python programs is to use a Python virtual
environment. An install script is supplied in the distribution that performs the
install step(s) for you and creates and activates a virtual environment. You will
find this install script in the untarred distribution file at the root level.
    
    ./dragon-install 

You have completed the prerequisites for running Dragon with multiprocessing programs. If you are not in the virtual environment, you may need to navigate to the untarred distribution file at the root level and follow the commands below for activating the virtual environment. 

If you have already installed and want to come back and use your install at a later
time you may have to reactivate your environment. Execute this from the same directory as the install was run from above.

    . _env/bin/activate

Along with reactivating your environment you will also need to load the dragon
module.

    module use $PWD/modulefiles
    module load dragon

If you are NOT using a virtual environment then check and possibly update the
`$PATH` so it has the location of pip installed console scripts, such as
~/.local/bin. If using a virtual environment, this step is not necessary.

    export PATH=~/.local/bin:${PATH}

You can test your configuration using the point-to-point latency test.  You should see output similar to below after the `dragon` command for a single node allocation.
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
