#!/bin/bash
. _env/bin/activate
ray stop
echo quit | nvidia-cuda-mps-control