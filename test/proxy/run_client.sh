#!/bin/bash

cd ../multi-node/
make clean
make c_ddict
make cpp_ddict
cd ../proxy/
dragon-cleanup -s
rm *.log
dragon -s test_proxy_multinode.py --exit-path='/path/to/check/for/exit' |& tee output.log