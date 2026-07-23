#!/bin/bash

rm slurm.json || true
rm ray_output.txt || true

ROOT_DIR="$1" # root dir
NUM_GPUS="$2" # Set num gpus per node

echo "ROOT_DIR: $ROOT_DIR"
echo "NUM_GPUS: $NUM_GPUS"

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
echo "$head_ip" > ray_output.txt

# Set up the head node of the Ray cluster
ssh $head_ip "cd ${ROOT_DIR} && . _env/bin/activate && ray start --head --port=6379 --num-gpus=$NUM_GPUS --node-ip-address=${head_ip}"
echo "Head node is up"

# Set up the worker nodes of the Ray cluster
for ((i = 1; i < ${#ip_array[@]}; i++)); do
  	ip="${ip_array[$i]}"
	echo $ip
	ssh $ip "cd ${ROOT_DIR} && bash tests/performance_tests/ray/exec_per_node.sh ${head_ip} ${ip} $NUM_GPUS ${ROOT_DIR}"
done
