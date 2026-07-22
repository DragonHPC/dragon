#!/bin/bash

cd ../../ && . devtools/setup && . _env/bin/activate && cd -

# Remove any cores lying
rm core

echo $PWD
rm -f *.log

echo "running test"
dragon -l dragon-file=INFO -l actor-file=INFO ./helloworld.py