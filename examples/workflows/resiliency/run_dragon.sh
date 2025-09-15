#!/bin/bash

cd ../../../ && . hack/setup && cd -

rm *log
rm .dragon-net-conf

dragon --resilient --nodes=2 -l dragon_file=DEBUG -l actor_file=DEBUG dragon_resiliency.py --trigger-restart