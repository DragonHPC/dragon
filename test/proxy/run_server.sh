#!/bin/bash

cd ../multi-node/
make clean
make c_ddict
make cpp_ddict
cd ../proxy/
export DRAGON_DEFAULT_SEG_SZ=21474836480
dragon-cleanup
rm ./*.log core > /dev/null 2>&1
rm ~/.dragon/my-runtime > /dev/null 2>&1
dragon server.py --exit-path='/path/to/check/for/exit'
