#!/bin/bash

cc -o mpi_hello mpi_hello.c
dragon lazy_attach.py --system="pippin" --exit-path="/lus/scratch/wahlc/dragon/dev/AICI-1940-proxy-docs-build/examples/workflows/proxy/client_exit" --remote-working-dir="/lus/scratch/wahlc/dragon/dev/AICI-1940-proxy-docs-build/examples/workflows/proxy"
