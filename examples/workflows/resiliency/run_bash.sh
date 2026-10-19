#!/bin/bash

module swap PrgEnv-cray PrgEnv-gnu
module load gcc
module load cray-python


srun --ntasks=2 --nodes=2 --cpu_bind=none python3 bash_resiliency.py --trigger-restart

# Enter a restart loop
while [ $? != 0 ]; do
    echo "Entering restart and executing on same nodes"
    export BASH_RESILIENT_RESTART=1
    srun --ntasks=2 --nodes=2 --cpu_bind=none python3 bash_resiliency.py --trigger-restart
done

echo "exited loop"