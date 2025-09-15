#!/bin/bash

cd ../../../ && . hack/multinode_config && . _env/bin/activate && cd -

# Tell CTI where to log to
echo $PWD
rm -f *.log

# clean up stuff on the login
rm -rf  /dev/shm/_dragon*

# dragon_multi is a sym link to src/dragon/dragon_multi_fe.py
echo "running test"
python3 ./test_launcher_fe.py
