#!/bin/bash

rm slurm.json || true
rm ray_output.txt || true

echo "things are being cleaned up"

NUM_GPUS_PER_NODE="$1"
ROOT_DIR="$2" # root dir
NUM_PROMPTS="$3" # Set num gpus per node
EXCEL_OUT_DIR="$4"
MODEL_SIZE="$5"

echo "NUM_GPUS_PER_NODE: $NUM_GPUS_PER_NODE"
echo "ROOT_DIR: $ROOT_DIR"
echo "NUM_PROMPTS: $NUM_PROMPTS"
echo "EXCEL_OUT_DIR: $EXCEL_OUT_DIR"
echo "MODEL_SIZE: $MODEL_SIZE"


# generate the slurm.json file with the network configuration
dragon-network-config --wlm slurm --output-to-json

# get a string of ip addresses from the json file
ip_list=$(python3 $ROOT_DIR/tests/performance_tests/ray/get_ips_from_json.py)

# Convert the space-separated string into an array
ip_array=($ip_list)
echo "All ip-addresses: $ip_list"

# We use the first ip address as the head node of the Ray cluster
# and the rest as the worker nodes.
# Store the first element of the array and remove it from the array
head_ip="${ip_array[0]}"
echo "Head IP Address: $head_ip"

echo "Root dir: $ROOT_DIR"
# Run python single node (MP Inference) workflow. Single GPU variation
ssh $head_ip "cd ${ROOT_DIR} && . _env/bin/activate && \
python3 tests/performance_tests/python_mp/mp_performance.py \
--model_size $MODEL_SIZE \
--num_gpus_per_node $NUM_GPUS_PER_NODE \
--run_type a \
--num_prompts $NUM_PROMPTS \
--output_dir $EXCEL_OUT_DIR"


# Run python single node (MP Inference) workflow. Multiple GPU variation

ssh $head_ip "cd ${ROOT_DIR} && . _env/bin/activate && \
python3 tests/performance_tests/python_mp/mp_performance.py \
--model_size $MODEL_SIZE \
--num_gpus_per_node $NUM_GPUS_PER_NODE \
--run_type b \
--num_prompts $NUM_PROMPTS \
--output_dir $EXCEL_OUT_DIR"