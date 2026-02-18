#!/bin/bash
# hn="$(hostname)"
mp_benchdir=$PWD
export PYTHONHASHSEED=0

export PATH=$PATH:${mp_benchdir}/tests

npp=${mp_benchdir}/util

pp=$PYTHONPATH
export PYTHONPATH=${pp:+$pp:}$npp # appends to path, sets if empty
