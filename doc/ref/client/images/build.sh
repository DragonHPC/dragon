#!/bin/bash
export PATH=$PATH:/home/users/klee/home/SRMStoFigs
ls -1 *.srms | xargs -n 1 srms2png
