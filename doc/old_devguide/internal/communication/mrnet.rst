.. _MRNet:

MRNet
+++++

The MRNetServerFE and MRNetServerBE classes of the :ref:`Launcher` use MRNet to communicate during
:ref:`MultiNodeBringup` and :ref:`MultiNodeTeardown` of Dragon. The MRNetServerFE and MRNetServerBE classes
were written to encapsulate and simplify the use of MRNet within the Dragon project, but the API may prove
useful to other projects as well.

The Multicast Reduction Network  (MRNet) is a publicly available library from the University of Wisconsin and
a company called Paradyn. As of this writing it had not changed since 2015. It is largely stable and is used
by the Debugger team on the valgrind4hpc tool. The MRNet Server component in the Dragon project was adapted
from some of this work. Documentation of MRNet, while useful, is not complete. 

MRNet was added to the dragon repository as a submodule. The :ref:`BuildingMRNet` subsection of this part of
the document goes into some detail on how to configure and make MRNet for use in this project.

.. _BuildingMRNet:

Building MRNet
==============

.. Pino slack channel for pino has the ip address.
.. Appollo running HPCM might be one to try: cfa6k and pea2k

The MRNet library source code is included as a git submodule of the dragon repository. The submodule comes
from the Debugger team and their mrnet repository. Initially setting up the submodule required the following
command be executed.

.. code-block:: bash

    cd external
    git clone --recurse-submodules ssh://git@stash.us.cray.com:7999/pe-cdst/mrnet.git

This command cloned the repository as a submodule and placed it in the external/mrnet subdirectory of the
dragon repository. Building MRNet requires a system that has the correct modules installed on it to allow
compilation. Typically these are development systems with multiple nodes and slurm installed, like jupiter or
horizon. The following script is located in the external/ directory as
*mrnet_build.sh* and can be run to build all of MRNet. Note the comments below. MRNet makefiles are broken and
do not force recompile of code correctly so it is necessary to delete the install directory to get things rebuilt should
you modify any of the MRNet code (by putting in debug statements for instance) to get things rebuilt
correctly.

The MRNet build also neglects to copy an important include file into the install directory. This is done at
the end of this build script so the include can be found later when compiling the MRNetServer code.

In addition, the MRNet build files are affected by certain environment variables being set that may be set by
the bigger Dragon build process. For that reason, many of the environment variables that are set by Dragon are
unset before running this script. This script, which is run from the bigger build of Dragon, is run as a shell
under the Dragon build so that environment variables of the Dragon build process are not affected by unsetting
them here in this build script.

.. code-block:: bash

    #!/bin/bash
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    cd $DIR
    # The mrnet configure and install is very sensitive to env vars and
    # we call this from a make file in the dragon source mrnetserver dir.
    unset INSTALL
    unset CC
    unset CFLAGS
    unset ROOT_DIR
    unset BASE_INSTALL
    unset BASE_DEV_INSTALL
    unset INCLUDE
    unset LIBRARIES
    unset INSTALL_LIB
    unset INSTALL_INCLUDE
    unset INSTALL_PYLIB
    unset H_SOURCES
    unset C_SOURCES
    unset LIBPRODUCTS
    # This sets up the environment and builds the mrnet submodule
    # of the dragon project. You can run this script on a system
    # like jupiter which has the correct packages and modules
    # installed to support the compilation of the code.
    git clone --recurse-submodules ssh://git@stash.us.cray.com:7999/pe-cdst/mrnet.git
    cd mrnet
    # The makefiles are broken and we must blow
    # away the install directory to force the
    # installed files to be rebuilt.
    rm -Rf install
    git fetch origin
    git checkout gdb4hpc-external
    # On Shasta the following command does the two next commands
    # for you.
    # module restore PrgEnv-gnu
    module unload PrgEnv-cray
    module load PrgEnv-gnu
    module unload atp
    module load cray-cti
    ./configure --prefix=$DIR/mrnet/install --with-startup=cray-cti --enable-shared --with-craycti-inc=$CTI_INSTALL_DIR/include/ --with-craycti-lib=$CTI_INSTALL_DIR/lib --with-boost=/opt/cray/pe/cdst-support/default/
    make
    make install
    # The following two lines were necessary (2/8/2021) to get the xplat_config.h into a directory
    # that could be found on an include path.
    cd install/include/xplat/
    ln -s ../../lib/xplat-5.0.1/include/xplat_config.h

Please note that building MRNet, and the rest of this code, can be done on a separate system from where it is
run.  In some cases it may be necessary to rebuild on the target system if other system libraries are not
compatible, but in many cases it will work to build on one system and run on another.

The commands in this build script work with the module system on XC systems. Shasta systems may need the
unload and load module commands replaced by a single

.. code-block:: bash

    module restore PrgEnv-gnu

This may or may not be true of other future systems.

After executing this build script, the library binaries are installed in the *install* directory. The
install/lib directory needs to be in the *LD_LIBRARY_PATH* and the *install/include* directory needs to be one
of the include directories for any code that will use this library.

Updating the *LD_LIBRARY_PATH* is the responsibility of the dragon module found in the modulefiles
subdirectory of the *src* directory. The external/mrnet/install/lib directory is added to the *LD_LIBRARY_PATH* in this module
file as shown in this module file.

.. code-block:: bash

    #%Module
    #
    #
    # Module Dragon
    # Copyright 2004-2019 Cray Inc. All Rights Reserved.
    #
    setenv DRAGON_BASE_DIR [file dirname [ file dirname $ModulesCurrentModulefile ] ]

    prepend-path PATH $env(DRAGON_BASE_DIR)/bin
    setenv PYTHONUSERBASE $env(DRAGON_BASE_DIR)/lib
    prepend-path LD_LIBRARY_PATH $env(DRAGON_BASE_DIR)/../external/mrnet/install/lib:$env(DRAGON_BASE_DIR)/lib
    prepend-path PYTHONPATH $env(DRAGON_BASE_DIR)/pylib

    setenv DRAGON_INCLUDE_DIR $env(DRAGON_BASE_DIR)/include

In the root directory of the dragon repository is a *setup.sh* script that does the module load for dragon to
set up these directories. It is given here for reference.

.. code-block:: bash

    #!/bin/bash
    # This sets up the environment for compiling code.
    # It also sets up the environment for finding
    # include files in the dragon subdirectories.
    # You should source ./setup.sh in this directory.
    module unload PrgEnv-cray
    module load PrgEnv-gnu
    module unload atp
    module load cray-cti
    cd src
    module use $PWD/modulefiles
    module load dragon
    cd ..
    export PATH=.:$PWD/external/mrnet/install/bin:$PATH

The typical steps in building MRNet would be:

    #. Clone the dragon repository
    #. Execute the setup.sh in the root directory of the dragon repository.
    #. Run 'make all-multinode' to make all of Dragon, MRNet, and the MRNetServer code.

This will result in all the correct files being installed in the external/mrnet/install directory for MRNet,
and all the libraries being compiled for Dragon  as well. All libraries and inlcudes are then referenced
correctly by the LD_LIBRARY_PATH and the include directories used in building the MRNetServerFE and
MRNetServerBE classes described in the next section.

The MRNetServerFE and MRNetServerBE Classes
===========================================

There are two executables that cooperate in setting up an MRNet network between the login node and the compute
nodes. The MRNetServerFE encapsulates the functionality needed for the login node.  The MRNetServerBE class
encapsulates the functionality of MRNet needed on the compute nodes.

The MRNetServerFE is given the name of the MRNetServerBE executable. The two classes then set up the
communication between the two sides and establish that communication pathway.

From the login node, a user program can broadcast a message to all compute nodes or it can send a message to a
single node which is designated the *primary* node.

On the back end, a user program can send data up the MRNet network tree to the login node by using a send
operation.

Between the MRNetServerFE and MRNetServerBE, all messages are strings. These strings may have further
structure (like JSON) imposed on them, but that is left to the user program using this framework.

Included here are the two class definitions along with their methods and sample code.

.. cpp:class:: MRNetServerFE

    .. cpp:function:: MRNetServerFE(std::vector<const char*>& ctiExecutableArgs, \
        std::vector<const char*>& backendExecutableArgs, \
        MRNetServerFEDataCallBack callback)

        Constructor for the MRNetServer Front End.  Objects of type MRNetServerFE are meant to be instantiated
        once on the login node of a multinode allocation. The actual allocation of the nodes is outside the
        scope of this framework. MRNet is started up to include all the compute nodes in the allocation. The
        CTI executable in the arguments is the "program" that will be run on each of the compute nodes. The
        CTI executable is watched by a CTI daemon. If the process running the CTI executable terminates, CTI
        will clean up all running processes on the compute nodes and end the allocation.
        
        The backend executable is the program that makes up the backend of the MRNetServer framework. The
        backend executable is run on each compute node and it must create an instance of the MRNetServerBE
        class which will than attach to the MRNet network.
        
        Finally, the user of this framework provides a callback function to process data that arrives from the
        backend over the MRNet network. All data is sent in simple string format. No formatting is done by
        this framework. So, for instance, a user program may choose to encode messages in JSON format.

        **ARGUMENTS**

        * ctiExecutableArgs

            A vector of null-terminated C strings with the executable at position 0 and any arguments
            following. The CTI executable is the watched process by CTI. If this process terminates, the
            allocation is cleaned up and terminated.

        * backendExecutableArgs

            A vector of null-terminated C strings with the executable at position 0 and any arguments
            following. The backend executable must create an instance of MRNetServerBE to attach to the
            network. The arguments are provided on the command-line to the executable. There are 5 additional
            arguments that are added at the end of the command-line arguments to the backend executable by the
            MRNet library and those arguments are passed on to MRNet by this framework.
            
            The user supplied backendExecutableArgs are examined to see if they are files. If any of them are
            existing files, those files are added as binary manifest files when the MRNet is created. In this
            way, the files get transmitted to a location where the MRNet backend compute nodes can find them.

        * callback

            A pointer to a function that is the function to call when data arrives off the network from a
            compute node. The callback must have the same type signature of the MRNetServerFEDataCallBack
            type. The callback must take as arguments a pointer to an MRNetServerFE object (this type) and a
            string reference to the message coming from a compute node. The return type of the callback is
            void.

        **RETURNS**

            none

    .. cpp:function:: virtual ~MRNetServerFE()

        The destructor for the MRNetServerFE object.  If an instance of this object was created with "new"
        then "delete" should be called on the instance pointer to free the object. Otherwise, the destructor
        is called automatically.

        **ARGUMENTS**

        none

        **RETURNS**

        none

    .. cpp:function:: void send_all(const std::string& msg)

        Broadcast a message to all compute nodes.  The message is sent to each compute node in the allocation.
        This method cannot be called from a callback. It must be called from the main thread.

        **ARGUMENTS**

        * msg

            The string contained in msg is sent to all compute nodes.

        **RETURNS**

        none

    .. cpp:function:: void send_primary(const std::string& msg)

        Send a message to the primary node of the allocation only.
        The message is sent to the primary compute node only in the allocation. This method cannot be called
        from a callback. It must be called from the main thread.

        **ARGUMENTS**

        * msg

            The string contained in msg is sent to the primary compute node.

        **RETURNS**

        none

    .. cpp:function:: int get_num_nodes() const

        This returns the count of nodes in the allocation.

        **ARGUMENTS**

        none

        **RETURNS**

        none

    .. cpp:function:: std::string get_primary_hostname()

        Returns the primary node's hostname for the allocation.

        **ARGUMENTS**

        none

        **RETURNS**

        none

    .. cpp:function:: void shutdown()

        Calling this initiates a shutdown of the network. Normal shutdown is initiated through this call.

        **ARGUMENTS**

        none

        **RETURNS**

        none

.. cpp:class:: MRNetServerBE

    .. cpp:function:: MRNetServerBE(int argc, char* argv[], MRNetServerBEDataCallBack callback)

        Constructor for the MRNetServer Backend.
        Object's of this type are meant to be instantiated once on each compute node via the backend
        executable argument provided to the MRNetServerFE front end class.

        The backend executable is the program that makes up the backend of the MRNetServer framework. The
        backend executable is run on each compute node and it must create an instance of the MRNetServerBE
        class which will than attach to the MRNet network.

        The user of this framework provides a callback function to process data that arrives from at the
        backend over the MRNet network. All data is sent in simple string format. No formatting is done by
        this framework. So, for instance, a user program may choose to encode messages in JSON format.

        **ARGUMENTS**

        *  @ argc

            The user should pass the argc given to the main program as its command-line argument count.

        * argv

            An array of null-terminated C strings. This should be the argv passed on the command-line to the
            backendExecutable. This is necessary because MRNet provides configuration parameters on the
            command line to the backend executable. The last 5 arguments of the command-line belong to MRNet.
            This allows other command-line arguments to be supplied to the backendExecutable. The will be
            before the MRNet arguments, but all arguments may be given to this constructor. MRNet will only
            use the last 5.

        * callback

            A pointer to a function that is the function to call when data arrives off the network from the
            login node. The callback must have the same type signature of the MRNetServerBEDataCallBack type.
            The callback must take as arguments a pointer to an MRNetServerBE object (this type) and a string
            reference to the message coming from the login node. The return type of the callback is void.

        **RETURNS**

        none

    .. cpp:function:: virtual ~MRNetServerBE()

        Destructor for the MRNetServer Backend.
        Deallocate the network by deleting its pointer. The destructor of MRN::Network takes care of
        bringing the network down.

        **ARGUMENTS**

        none

        **RETURNS**

        none

    .. cpp:function:: void send(const std::string& msg)

        Send a message. All messages are strings that may be encoded in some way (like json). From the
        backend, either the main thread or the callback handler may send messages. Sending directly from the
        callback handler is not supported by MRNet, but with this framework, messages are queued to be sent
        later by the main thread. Serve must be called from the main thread to insure messages from the
        callback handler get sent. Alternatively, the main thread may call flush to cause messages sent by the
        callback handler to be sent. Calling send from the main thread will cause all waiting messages (queued
        by sending from the callback handler) to immediately be sent.

        **ARGUMENTS**

        * msg

            The data to be sent to the front end MRNetServerFE object.

        **RETURNS**

        none

    .. cpp:function:: void flush()

        Flush messages sent by the callback handler to the front end.  If the main thread calls serve, then
        flush does not need to be called. If the main thread does not call serve, then messages sent from the
        callback handler are queued until either flush is called by the main thread or until the main thread
        calls send. In either case, all queued messages are sent in order including the last message when sent
        by the main thread.

        **ARGUMENTS**

        none

        **RETURNS**

        none

    .. cpp:function:: void serve()

        Block the main thread until shutdown is called.
        If the main thread does not have work to do (i.e. all work is done by the callback handler) then serve
        should be called to block the main thread until shutdown of the MRNetServerFE/BE is initiated by the
        front end. If the main thread calls serve, then flush does not need to be called. If the main thread
        does not call serve, then messages sent from the callback handler are queued until either flush is
        called by the main thread or until the main thread calls send.

        **ARGUMENTS**

        none

        **RETURNS**

        none

    .. cpp:function:: void shutdown()

        **ARGUMENTS**

        none

        **RETURNS**

        none



Building and Running the MRNet Server Test
==========================================

To build the MRNetServer test program you must compile two programs in the mrnet test directory and the
correct library files must be in the LD_LIBRARY_PATH to run the test program. The correct directories are
added to the library path by sourcing the setup.sh file in the dragon root directory which also loads the
dragon module. The loading of the dragon module correctly sets the LD_LIBRARY_PATH so the test can be
executed.

The *runit* script in the mrnet test directory is responsible for setting up and running the test. The scripts
contents are given here.

.. code-block:: bash

    #!/bin/bash
    # with these two environment variables
    # CTI will log errors on the backend
    # to the log directory. The nodes
    # in the allocation will print to the
    # log directory with their node number
    # in the name of the log file.
    # CTI_DEBUG=1 will turn on CTI debugging.
    export CTI_DEBUG=1
    export CTI_LOG_DIR=$PWD/log
    # MRNET_DEBUG_LEVEL can be set to 3 for
    # much more debugging statements in log files.
    #export MRNET_DEBUG_LEVEL=3
    export MRNET_DEBUG_LEVEL=0
    mkdir log
    rm log/*
    salloc --ntasks=2 --ntasks-per-node=1 --time=20 << EOF
    ./mrnet_test
    EOF

If debugging needs to be done, the CTI_DEBUG should be set to 1 and the MRNET_DEBUG_LEVEL should be set to 3.
In this way, the debug statements in the MRNet library will be printed to standard error.  Any printing to
standard error in the MRNetServerFE/BE code will also be logged. The log directory contains the log files.
There is a log file created for each compute node's output. There is a log file created for the login nodes
log file. And finally there is a CTI log file that is created as well.

To determine if the LD_LIBRARY_PATH is set correctly you can use the *ldd* command to determine where all the
libraries will be pulled from.

The salloc command command is a SLURM command and allocates two compute nodes for 20 seconds. The salloc
command creates a shell and the mrnet_test program is run within that shell. The test prints that it completed
successfully if all goes well.



.. _MRNetAPI:

MRNet API
===============

