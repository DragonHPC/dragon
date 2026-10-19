#!/bin/bash

dragon-cleanup > /dev/null 2>&1
rm ./*.log core > /dev/null 2>&1
rm ~/.dragon/my-runtime > /dev/null 2>&1
rm my-runtime > /dev/null 2>&1
dragon remote-server.py
