#!/bin/bash

cd $4 && . _env/bin/activate
ray start --address=$1:6379 --node-ip-address=$2 --num-cpus=$3
