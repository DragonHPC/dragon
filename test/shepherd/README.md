Running Shepherd Tests
========================
To run the tests you must go up one directory to the dragon/test 
directory. Then 

	source ./setup.sh

This will setup the PYTHONPATH correctly to run 
test cases directly from the source directory. Otherwise,
you can build the test whole project and run from the build 
directory with the proper setup. 

If the Cray Python is not loaded then find the latest Python

	module avail

and load it

	module load cray-python/3.8.5

To run a particular test you type something like this from the test directory:

./shepherd/single_internal.py SingleInternal.test_process_create

and to run all the tests you type this from the test directory.

./shepherd/single_internal.py SingleInternal


