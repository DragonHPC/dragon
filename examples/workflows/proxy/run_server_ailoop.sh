#!/bin/bash

#dragon-cleanup > /dev/null 2>&1
dragon-cleanup > /dev/null 2>&1
rm ./*.log core > /dev/null 2>&1
rm ~/.dragon/my-runtime > /dev/null 2>&1
rm my-runtime > /dev/null 2>&1
rm exit_client > /dev/null 2>&1
make
dragon server.py
make clean
